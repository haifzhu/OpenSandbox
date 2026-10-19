// Copyright 2026 The OpenSandbox Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// workspace-sync is the s3-workspace pause sync Job entrypoint. It receives a
// base64 snapshotcontract.S3Request, POSTs the sync call to execd inside the
// sandbox pod, and writes the S3Result JSON to /dev/termination-log so the
// SandboxSnapshot controller can read it from the Job's termination message.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/alibaba/opensandbox/execd/pkg/objsync"
)

// syncRequest mirrors the controller-side snapshotcontract.S3Request wire
// format (version "v1"). It is defined locally because execd and the
// Kubernetes operator are separate Go modules; the Version field guards the
// contract across evolutions.
type syncRequest struct {
	Version         string   `json:"version"`
	TargetURL       string   `json:"targetUrl"`
	SourcePodIP     string   `json:"sourcePodIp,omitempty"`
	ExecDPort       int      `json:"execdPort"`
	Roots           []string `json:"roots"`
	ExcludePatterns []string `json:"excludePatterns,omitempty"`
	Endpoint        string   `json:"endpoint,omitempty"`
	Region          string   `json:"region,omitempty"`
	ForcePathStyle  bool     `json:"forcePathStyle,omitempty"`
	CredsSecretName string   `json:"credsSecretName,omitempty"`
}

// syncResult mirrors snapshotcontract.Result.S3Workspace for the termination
// message.
type syncResult struct {
	S3Workspace *s3Result `json:"s3Workspace,omitempty"`
}

type s3Result struct {
	Generation      uint64   `json:"generation"`
	Target          string   `json:"target"`
	Roots           []string `json:"roots"`
	Endpoint        string   `json:"endpoint,omitempty"`
	Region          string   `json:"region,omitempty"`
	ForcePathStyle  bool     `json:"forcePathStyle,omitempty"`
	CredsSecretName string   `json:"credsSecretName,omitempty"`
	ManifestDigest  string   `json:"manifestDigest,omitempty"`
	FilesTotal      int      `json:"filesTotal"`
	FilesChanged    int      `json:"filesChanged"`
	FilesDeleted    int      `json:"filesDeleted"`
	BytesUploaded   int64    `json:"bytesUploaded"`
	SyncSeconds     int64    `json:"syncSeconds"`
}

func main() {
	requestBase64 := flag.String("request-base64", "", "base64 JSON snapshotcontract.S3Request")
	execdTimeout := flag.Duration("execd-timeout", 9*time.Minute, "overall deadline for the execd sync call")
	purgeTarget := flag.String("purge-target", "", "purge mode: delete every object under this s3://bucket/prefix and exit")
	purgeEndpoint := flag.String("endpoint", "", "S3-compatible endpoint (purge mode)")
	purgeRegion := flag.String("region", "us-east-1", "S3 region (purge mode)")
	purgePathStyle := flag.Bool("path-style", false, "force path-style addressing (purge mode)")
	flag.Parse()

	if *purgeTarget != "" {
		if err := purge(*purgeTarget, *purgeEndpoint, *purgeRegion, *purgePathStyle); err != nil {
			fatal("purge failed", err)
		}
		return
	}

	if *requestBase64 == "" {
		fatal("--request-base64 is required", nil)
	}
	raw, err := base64.StdEncoding.DecodeString(*requestBase64)
	if err != nil {
		fatal("decode request", err)
	}
	var req syncRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		fatal("parse request", err)
	}
	if req.SourcePodIP == "" || req.ExecDPort <= 0 || req.TargetURL == "" || len(req.Roots) == 0 {
		fatal("request missing sourcePodIp/execdPort/targetUrl/roots", nil)
	}

	if err := run(req, *execdTimeout); err != nil {
		fatal("sync failed", err)
	}
}

func run(req syncRequest, timeout time.Duration) error {
	payload := map[string]any{
		"target":         req.TargetURL,
		"roots":          req.Roots,
		"exclude":        req.ExcludePatterns,
		"endpoint":       req.Endpoint,
		"region":         req.Region,
		"forcePathStyle": req.ForcePathStyle,
	}
	// execd runs as the sandbox workload's uid and cannot read the root-only
	// credential projection; this Job is an infra pod that never runs user
	// code, so hand the credentials over with the call.
	if ak := os.Getenv("OBJSYNC_ACCESS_KEY_ID"); ak != "" {
		payload["accessKeyId"] = ak
		payload["secretAccessKey"] = os.Getenv("OBJSYNC_SECRET_ACCESS_KEY")
		if tok := os.Getenv("OBJSYNC_SESSION_TOKEN"); tok != "" {
			payload["sessionToken"] = tok
		}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s:%d/internal/snapshot/sync", req.SourcePodIP, req.ExecDPort)

	client := &http.Client{Timeout: timeout}

	// Kick the sync off in the background and narrate its progress: a first
	// sync of a multi-GB workspace takes minutes, and a silent wait in the
	// Job log is indistinguishable from a hang.
	type postResult struct {
		body []byte
		code int
		err  error
	}
	done := make(chan postResult, 1)
	go func() {
		resp, err := client.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			done <- postResult{err: err}
			return
		}
		defer resp.Body.Close()
		rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		done <- postResult{body: rb, code: resp.StatusCode}
	}()

	statusURL := fmt.Sprintf("http://%s:%d/internal/snapshot/status", req.SourcePodIP, req.ExecDPort)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var post postResult
	waiting := true
	for waiting {
		select {
		case post = <-done:
			waiting = false
		case <-ticker.C:
			if line := progressLine(statusURL); line != "" {
				fmt.Println(line)
			}
		}
	}
	if post.err != nil {
		return fmt.Errorf("call execd %s: %w", url, post.err)
	}
	if post.code != http.StatusOK {
		return fmt.Errorf("execd answered HTTP %d: %s", post.code, string(post.body))
	}
	respBody := post.body

	var syncResp struct {
		Generation uint64 `json:"generation"`
		Stats      struct {
			FilesTotal   int   `json:"filesTotal"`
			FilesChanged int   `json:"filesChanged"`
			FilesDeleted int   `json:"filesDeleted"`
			DurationMS   int64 `json:"durationMs"`
		} `json:"stats"`
		UploadedBytes int64 `json:"uploadedBytes"`
		Latest        struct {
			Generation     uint64 `json:"generation"`
			ManifestDigest string `json:"manifestDigest"`
		} `json:"latest"`
	}
	if err := json.Unmarshal(respBody, &syncResp); err != nil {
		return fmt.Errorf("decode execd response: %w", err)
	}

	// Independent commit-point verification: when credentials are projected
	// into this Job, read LATEST straight from the object store and confirm
	// it matches what execd reported. Never trust only the in-pod HTTP
	// answer before declaring the snapshot durable.
	if err := verifyLatest(req, syncResp.Latest.Generation, syncResp.Latest.ManifestDigest); err != nil {
		return err
	}

	result := syncResult{
		S3Workspace: &s3Result{
			Generation:      syncResp.Generation,
			Target:          req.TargetURL,
			Roots:           req.Roots,
			Endpoint:        req.Endpoint,
			Region:          req.Region,
			ForcePathStyle:  req.ForcePathStyle,
			CredsSecretName: req.CredsSecretName,
			ManifestDigest:  syncResp.Latest.ManifestDigest,
			FilesTotal:      syncResp.Stats.FilesTotal,
			FilesChanged:    syncResp.Stats.FilesChanged,
			FilesDeleted:    syncResp.Stats.FilesDeleted,
			BytesUploaded:   syncResp.UploadedBytes,
			SyncSeconds:     syncResp.Stats.DurationMS / 1000,
		},
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	// The controller reads the Job termination message from this file.
	if err := os.WriteFile("/dev/termination-log", encoded, 0o644); err != nil {
		return fmt.Errorf("write termination message: %w", err)
	}
	fmt.Printf("workspace-sync: generation %d synced (%d changed, %d deleted, %d bytes)\n",
		syncResp.Generation, syncResp.Stats.FilesChanged, syncResp.Stats.FilesDeleted, syncResp.UploadedBytes)
	return nil
}

// purge deletes every object under the target prefix (best-effort sandbox
// cleanup on deletion; bucket lifecycle rules are the backstop).
func purge(targetURL, endpoint, region string, pathStyle bool) error {
	target, err := objsync.ParseTarget(targetURL)
	if err != nil {
		return err
	}
	store := objsync.NewS3Store(objsync.S3Config{
		Endpoint:       endpoint,
		Region:         region,
		AccessKey:      os.Getenv("OBJSYNC_ACCESS_KEY_ID"),
		SecretKey:      os.Getenv("OBJSYNC_SECRET_ACCESS_KEY"),
		SessionToken:   os.Getenv("OBJSYNC_SESSION_TOKEN"),
		ForcePathStyle: pathStyle,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	prefix := ""
	if target.Base != "" {
		prefix = target.Base + "/"
	}
	deleted := 0
	marker := ""
	for {
		keys, next, err := store.List(ctx, target.Bucket, prefix, marker, 1000)
		if err != nil {
			return fmt.Errorf("list %s: %w", prefix, err)
		}
		for _, key := range keys {
			if err := store.Delete(ctx, target.Bucket, key); err != nil {
				return fmt.Errorf("delete %s: %w", key, err)
			}
			deleted++
		}
		if next == "" {
			break
		}
		marker = next
	}
	fmt.Printf("workspace-sync: purged %d objects under %s\n", deleted, targetURL)
	return nil
}

func verifyLatest(req syncRequest, wantGeneration uint64, wantDigest string) error {
	accessKey := os.Getenv("OBJSYNC_ACCESS_KEY_ID")
	secretKey := os.Getenv("OBJSYNC_SECRET_ACCESS_KEY")
	if accessKey == "" && secretKey == "" {
		// No credentials projected: skip the independent check (the controller
		// only omits them for pre-migration deployments).
		fmt.Println("workspace-sync: no credentials projected, skipping LATEST verification")
		return nil
	}
	target, err := objsync.ParseTarget(req.TargetURL)
	if err != nil {
		return fmt.Errorf("parse target for verification: %w", err)
	}
	store := objsync.NewS3Store(objsync.S3Config{
		Endpoint:       req.Endpoint,
		Region:         req.Region,
		AccessKey:      accessKey,
		SecretKey:      secretKey,
		SessionToken:   os.Getenv("OBJSYNC_SESSION_TOKEN"),
		ForcePathStyle: req.ForcePathStyle,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	latest, err := objsync.ReadLatest(ctx, store, target)
	if err != nil {
		return fmt.Errorf("verify LATEST: %w", err)
	}
	if latest.Generation != wantGeneration || latest.ManifestDigest != wantDigest {
		return fmt.Errorf(
			"LATEST mismatch after sync: store has gen %d digest %s, execd reported gen %d digest %s",
			latest.Generation, latest.ManifestDigest, wantGeneration, wantDigest)
	}
	return nil
}

func fatal(msg string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "workspace-sync: %s: %v\n", msg, err)
	} else {
		fmt.Fprintf(os.Stderr, "workspace-sync: %s\n", msg)
	}
	os.Exit(1)
}

// progressLine renders one progress line from execd's status endpoint, or ""
// when nothing is running (or execd does not answer).
func progressLine(statusURL string) string {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(statusURL)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	var p struct {
		Operation   string `json:"operation"`
		Phase       string `json:"phase"`
		FilesTotal  int    `json:"filesTotal"`
		FilesDone   int    `json:"filesDone"`
		BytesTotal  int64  `json:"bytesTotal"`
		BytesDone   int64  `json:"bytesDone"`
		CurrentFile string `json:"currentFile"`
		ElapsedMs   int64  `json:"elapsedMs"`
	}
	if err := json.Unmarshal(raw, &p); err != nil || p.Operation == "" {
		return ""
	}
	pct := 0.0
	if p.BytesTotal > 0 {
		pct = float64(p.BytesDone) / float64(p.BytesTotal) * 100
	} else if p.FilesTotal > 0 {
		pct = float64(p.FilesDone) / float64(p.FilesTotal) * 100
	}
	return fmt.Sprintf("workspace-sync: %s %s %.1f%% — %d/%d files, %d/%d bytes (%.1fs)",
		p.Operation, p.Phase, pct, p.FilesDone, p.FilesTotal, p.BytesDone, p.BytesTotal, float64(p.ElapsedMs)/1000)
}
