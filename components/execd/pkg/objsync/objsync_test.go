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

package objsync

import (
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func mustTarget(t *testing.T) Target {
	t.Helper()
	target, err := ParseTarget("s3://test-bucket/myprefix/sandboxes/ns/sbx-1")
	if err != nil {
		t.Fatal(err)
	}
	return target
}

func writeFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

// snapshotTree captures the comparable state of a tree: path → content (file
// bytes, symlink target) plus modes.
func snapshotTree(t *testing.T, root string) map[string]fileState {
	t.Helper()
	out := map[string]fileState{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || path == root {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		info, _ := d.Info()
		state := fileState{mode: info.Mode()}
		switch {
		case info.Mode().IsRegular():
			b, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			state.content = string(b)
		case info.Mode()&os.ModeSymlink != 0:
			target, readErr := os.Readlink(path)
			if readErr != nil {
				return readErr
			}
			state.content = target
		}
		out[rel] = state
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

type fileState struct {
	content string
	mode    os.FileMode
}

// wipeTree empties a root so Restore can rebuild it in place, mirroring a
// resumed sandbox container (restore happens at the same absolute paths).
func wipeTree(t *testing.T, root string) {
	t.Helper()
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
}

func compareTrees(t *testing.T, want, got map[string]fileState) {
	t.Helper()
	for rel, w := range want {
		g, ok := got[rel]
		if !ok {
			t.Errorf("restored tree missing %s", rel)
			continue
		}
		if g.content != w.content {
			t.Errorf("%s content mismatch: got %q want %q", rel, g.content, w.content)
		}
		if g.mode.Perm() != w.mode.Perm() {
			t.Errorf("%s mode mismatch: got %v want %v", rel, g.mode.Perm(), w.mode.Perm())
		}
	}
	for rel := range got {
		if _, ok := want[rel]; !ok {
			t.Errorf("restored tree has unexpected entry %s", rel)
		}
	}
}

func TestParseTarget(t *testing.T) {
	cases := []struct {
		raw, bucket, base string
		wantErr           bool
	}{
		{raw: "s3://b/p/sandboxes/ns/id", bucket: "b", base: "p/sandboxes/ns/id"},
		{raw: "s3://b", bucket: "b", base: ""},
		{raw: "s3://b/", bucket: "b", base: ""},
		{raw: "s3://b//x", wantErr: true},
		{raw: "s3://b/p//q", wantErr: true},
		{raw: "http://b/x", wantErr: true},
		{raw: "s3://", wantErr: true},
	}
	for _, c := range cases {
		target, err := ParseTarget(c.raw)
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseTarget(%q) expected error", c.raw)
			}
			continue
		}
		if err != nil || target.Bucket != c.bucket || target.Base != c.base {
			t.Errorf("ParseTarget(%q) = %+v, %v; want bucket=%s base=%s", c.raw, target, err, c.bucket, c.base)
		}
	}
}

func TestSyncFirstGenerationAndRestoreRoundtrip(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()

	writeFile(t, filepath.Join(root, "hello.txt"), "hello world", 0o644)
	writeFile(t, filepath.Join(root, "bin", "run.sh"), "#!/bin/sh\n", 0o755)
	if err := os.Symlink("hello.txt", filepath.Join(root, "link.txt")); err != nil {
		t.Fatal(err)
	}

	syncer := &Syncer{Store: store}
	result, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target})
	if err != nil {
		t.Fatal(err)
	}
	if result.Generation != 1 {
		t.Fatalf("first generation = %d, want 1", result.Generation)
	}
	// 2 regular files + 1 symlink carry content; directories do not upload.
	if result.Stats.FilesChanged != 3 {
		t.Fatalf("filesChanged = %d, want 3", result.Stats.FilesChanged)
	}

	// All keys must live under the target prefix — structural prefix rule.
	const prefix = "myprefix/sandboxes/ns/sbx-1/"
	for _, key := range store.Keys(target.Bucket) {
		if len(key) < len(prefix) || key[:len(prefix)] != prefix {
			t.Fatalf("key escaped prefix: %s", key)
		}
	}

	want := snapshotTree(t, root)
	wipeTree(t, root)
	if _, err := Restore(ctx, store, RestoreOptions{Target: target, Roots: []string{root}}); err != nil {
		t.Fatal(err)
	}
	compareTrees(t, want, snapshotTree(t, root))
}

func TestSyncIncrementalDeltaOnly(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()

	writeFile(t, filepath.Join(root, "a.txt"), "aaa", 0o644)
	writeFile(t, filepath.Join(root, "b.txt"), "bbb", 0o644)
	syncer := &Syncer{Store: store}
	if _, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target}); err != nil {
		t.Fatal(err)
	}

	// Mutate: change a.txt, delete b.txt, add c.txt.
	time.Sleep(2 * time.Millisecond) // ensure mtime advances a measurable tick
	writeFile(t, filepath.Join(root, "a.txt"), "aaa2", 0o644)
	if err := os.Remove(filepath.Join(root, "b.txt")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(root, "c.txt"), "ccc", 0o644)

	result, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target})
	if err != nil {
		t.Fatal(err)
	}
	if result.Generation != 2 {
		t.Fatalf("generation = %d, want 2", result.Generation)
	}
	if result.Stats.FilesChanged != 2 { // a.txt changed + c.txt new
		t.Fatalf("filesChanged = %d, want 2", result.Stats.FilesChanged)
	}
	if result.Stats.FilesDeleted != 1 {
		t.Fatalf("filesDeleted = %d, want 1", result.Stats.FilesDeleted)
	}

	want := snapshotTree(t, root)
	wipeTree(t, root)
	if _, err := Restore(ctx, store, RestoreOptions{Target: target, Roots: []string{root}}); err != nil {
		t.Fatal(err)
	}
	compareTrees(t, want, snapshotTree(t, root))
}

func TestSyncRenameDedupesRemoteUpload(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()

	writeFile(t, filepath.Join(root, "big.txt"), "same content", 0o644)
	syncer := &Syncer{Store: store}
	if _, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target}); err != nil {
		t.Fatal(err)
	}

	// Rename: new path, identical content — remote blob already exists.
	if err := os.Rename(filepath.Join(root, "big.txt"), filepath.Join(root, "moved.txt")); err != nil {
		t.Fatal(err)
	}
	result, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target})
	if err != nil {
		t.Fatal(err)
	}
	if result.UploadedBytes != 0 {
		t.Fatalf("rename uploaded %d bytes, want 0 (content-addressed dedupe)", result.UploadedBytes)
	}
	if result.SkippedBlobs != 1 {
		t.Fatalf("skippedBlobs = %d, want 1", result.SkippedBlobs)
	}
}

func TestSyncPartialFailureKeepsLatest(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()

	writeFile(t, filepath.Join(root, "keep.txt"), "v1", 0o644)
	syncer := &Syncer{Store: store}
	if _, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target}); err != nil {
		t.Fatal(err)
	}

	// Build a store that already spent generation-1's write budget, so the
	// very first generation-2 blob upload fails.
	seed := store.Keys(target.Bucket)
	failing := NewMemStore(len(seed))
	for _, key := range seed {
		var buf bytes.Buffer
		if _, err := store.Get(ctx, target.Bucket, key, &buf); err != nil {
			t.Fatal(err)
		}
		failing.objects[key] = buf.Bytes()
	}
	failing.writes = len(seed)

	time.Sleep(2 * time.Millisecond)
	writeFile(t, filepath.Join(root, "keep.txt"), "v2", 0o644)
	brokenSyncer := &Syncer{Store: failing}
	if _, err := brokenSyncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target}); err == nil {
		t.Fatal("expected sync to fail with injected store failure")
	}

	// The original store's LATEST must still point at generation 1 and a
	// restore from it yields v1 (partial generation 2 invisible).
	want := snapshotTree(t, root)
	wipeTree(t, root)
	if _, err := Restore(ctx, store, RestoreOptions{Target: target, Roots: []string{root}}); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(filepath.Join(root, "keep.txt"))
	if string(got) != "v1" {
		t.Fatalf("restore after failed sync = %q, want %q", got, "v1")
	}
	_ = want
}

func TestRestoreFailsClosedOutsideRoots(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "f.txt"), "x", 0o644)
	syncer := &Syncer{Store: store}
	if _, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target}); err != nil {
		t.Fatal(err)
	}

	// A manifest whose entry lives outside the restore root must abort.
	var manifestBuf bytes.Buffer
	if _, err := store.Get(ctx, target.Bucket, target.manifestKey(1), &manifestBuf); err != nil {
		t.Fatal(err)
	}
	var manifest Manifest
	if err := json.Unmarshal(manifestBuf.Bytes(), &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Files = append(manifest.Files, FileEntry{Path: "/etc/passwd", Type: TypeRegular, Size: 1, Mode: 0o644, SHA256: sha256HexStr("x")})
	body, _ := json.Marshal(&manifest)
	if err := store.Put(ctx, target.Bucket, target.manifestKey(2), bytes.NewReader(body), int64(len(body)), ""); err != nil {
		t.Fatal(err)
	}
	latestBody, _ := json.Marshal(Latest{Generation: 2, ManifestDigest: sha256Hex(body)})
	if err := store.Put(ctx, target.Bucket, target.latestKey(), bytes.NewReader(latestBody), int64(len(latestBody)), ""); err != nil {
		t.Fatal(err)
	}

	other := t.TempDir()
	if _, err := Restore(ctx, store, RestoreOptions{Target: target, Roots: []string{other}}); err == nil {
		t.Fatal("restore must fail closed when a manifest entry escapes the roots")
	}
}

func TestSyncExcludes(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "keep.txt"), "x", 0o644)
	writeFile(t, filepath.Join(root, "node_modules", "pkg", "index.js"), "y", 0o644)
	writeFile(t, filepath.Join(root, "log", "a.log"), "z", 0o644)

	syncer := &Syncer{Store: store}
	result, err := syncer.Sync(ctx, SyncOptions{
		Roots:   []string{root},
		Target:  target,
		Exclude: []string{"node_modules", "**/*.log"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range result.Manifest.Files {
		if f.Path == filepath.Join(root, "node_modules", "pkg", "index.js") || f.Path == filepath.Join(root, "log", "a.log") {
			t.Fatalf("excluded file %s made it into the manifest", f.Path)
		}
	}
}

// TestSigV4AWSDocsExample pins the signer against the worked example in the
// AWS "Authenticating Requests (AWS Signature Version 4)" documentation.
func TestSigV4AWSDocsExample(t *testing.T) {
	store := NewS3Store(S3Config{
		Region:    "us-east-1",
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
	})
	req, err := http.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=0-9")
	store.sign(req, "us-east-1", time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC), emptyPayloadSHA256)

	const want = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, " +
		"SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, " +
		"Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
	if got := req.Header.Get("Authorization"); got != want {
		t.Fatalf("Authorization mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestManifestValidate(t *testing.T) {
	m := &Manifest{FormatVersion: FormatVersionV1, Generation: 1}
	m.Files = []FileEntry{{Path: "relative/path", Type: TypeRegular, SHA256: sha256HexStr("x")}}
	if err := m.Validate(); err == nil {
		t.Fatal("relative path must be rejected")
	}
	m.Files = []FileEntry{{Path: "/a", Type: TypeRegular, SHA256: "short"}}
	if err := m.Validate(); err == nil {
		t.Fatal("short digest must be rejected")
	}
	m.Files = []FileEntry{{Path: "/a", Type: "weird"}}
	if err := m.Validate(); err == nil {
		t.Fatal("unknown type must be rejected")
	}
	m.Files = []FileEntry{{Path: "/a", Type: TypeDir}, {Path: "/a", Type: TypeDir}}
	if err := m.Validate(); err == nil {
		t.Fatal("duplicate path must be rejected")
	}
	m.Files = []FileEntry{{Path: "/a", Type: TypeDir}}
	if err := m.Validate(); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}
}

func TestS3ResolveHost(t *testing.T) {
	cases := []struct {
		name     string
		cfg      S3Config
		bucket   string
		wantBase string
		wantPath string
	}{
		{
			name:     "path-style minio",
			cfg:      S3Config{Endpoint: "http://minio.svc:9000", ForcePathStyle: true},
			bucket:   "b",
			wantBase: "http://minio.svc:9000",
			wantPath: "/b/k",
		},
		{
			name:     "virtual-host custom endpoint",
			cfg:      S3Config{Endpoint: "https://oss.example.com"},
			bucket:   "b",
			wantBase: "https://b.oss.example.com",
			wantPath: "/k",
		},
		{
			name:     "ip endpoint falls back to path style",
			cfg:      S3Config{Endpoint: "http://10.0.0.1:9000"},
			bucket:   "b",
			wantBase: "http://10.0.0.1:9000",
			wantPath: "/b/k",
		},
	}
	for _, c := range cases {
		store := NewS3Store(c.cfg)
		base, path := store.resolveHost(c.bucket, "k")
		if base != c.wantBase || path != c.wantPath {
			t.Errorf("%s: resolveHost = %s %s, want %s %s", c.name, base, path, c.wantBase, c.wantPath)
		}
	}
}

func TestSyncLargeFileUsesMultipart(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()

	big := bytes.Repeat([]byte("0123456789"), 1<<20) // 10 MiB > 8 MiB part threshold
	if err := os.WriteFile(filepath.Join(root, "big.bin"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	syncer := &Syncer{Store: store}
	if _, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target}); err != nil {
		t.Fatal(err)
	}

	want := snapshotTree(t, root)
	wipeTree(t, root)
	if _, err := Restore(ctx, store, RestoreOptions{Target: target, Roots: []string{root}}); err != nil {
		t.Fatal(err)
	}
	compareTrees(t, want, snapshotTree(t, root))
}

func sha256HexStr(s string) string { return sha256Hex([]byte(s)) }

// TestUnchangedFilesSurviveGenerationRestore is the regression test for the
// shared-blob-pool fix: a file unchanged across generations is not re-uploaded
// (its hash is only referenced by the previous manifest), so restoring the
// newer generation must still find its blob.
func TestUnchangedFilesSurviveGenerationRestore(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()

	writeFile(t, filepath.Join(root, "keep.txt"), "unchanged", 0o644)
	writeFile(t, filepath.Join(root, "churn.txt"), "v1", 0o644)
	syncer := &Syncer{Store: store}
	if _, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Millisecond)
	writeFile(t, filepath.Join(root, "churn.txt"), "v2", 0o644)
	result, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target})
	if err != nil {
		t.Fatal(err)
	}
	if result.Generation != 2 || result.UploadedBytes == 0 {
		t.Fatalf("generation=%d uploaded=%d, want 2 and >0", result.Generation, result.UploadedBytes)
	}

	want := snapshotTree(t, root)
	wipeTree(t, root)
	if _, err := Restore(ctx, store, RestoreOptions{Target: target, Roots: []string{root}}); err != nil {
		t.Fatalf("restore of generation 2 failed (unchanged blob missing from pool?): %v", err)
	}
	compareTrees(t, want, snapshotTree(t, root))
}

// TestSyncNoChangeKeepsGeneration verifies the idempotency fence: syncing an
// unchanged tree reuses the standing generation instead of stacking identical
// ones (retry safety for the sync Job and zero-change pauses).
func TestSyncNoChangeKeepsGeneration(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.txt"), "aaa", 0o644)

	syncer := &Syncer{Store: store}
	first, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target})
	if err != nil {
		t.Fatal(err)
	}
	keysAfterFirst := len(store.Keys(target.Bucket))

	second, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target})
	if err != nil {
		t.Fatal(err)
	}
	if second.Generation != first.Generation {
		t.Fatalf("no-change sync bumped generation %d -> %d", first.Generation, second.Generation)
	}
	if got := len(store.Keys(target.Bucket)); got != keysAfterFirst {
		t.Fatalf("no-change sync wrote objects: %d -> %d", keysAfterFirst, got)
	}
}

// TestSymlinkMtimeSurvivesRestoreForFence pins the no-change idempotency
// fence when symlinks are present: restore must recreate them with the
// recorded lstat mtime, not creation time (test-env finding: gen bumped
// every pause with 0 bytes uploaded, diff was only the link's mtimeNano).
func TestSymlinkMtimeSurvivesRestoreForFence(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "target.txt"), "x", 0o644)
	if err := os.Symlink("target.txt", filepath.Join(root, "lnk")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond) // ensure mtime is distinct from "now"
	syncer := &Syncer{Store: store}
	if _, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target}); err != nil {
		t.Fatal(err)
	}
	_, lnkBefore, _ := symlinkMtime(t, filepath.Join(root, "lnk"))

	wipeTree(t, root)
	if _, err := Restore(ctx, store, RestoreOptions{Target: target, Roots: []string{root}}); err != nil {
		t.Fatal(err)
	}
	_, lnkAfter, _ := symlinkMtime(t, filepath.Join(root, "lnk"))
	// Filesystem rounding (APFS sub-μs) may shift the restored value by a
	// few hundred ns; the FENCE (checked below) tolerates that via
	// manifestsEquivalent's mtimeTolerance.
	if d := lnkAfter - lnkBefore; d > 2000 || d < -2000 {
		t.Fatalf("symlink mtime drifted beyond tolerance: before=%d after=%d", lnkBefore, lnkAfter)
	}

	// Second sync must be a no-op (fence holds).
	res, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target})
	if err != nil {
		t.Fatal(err)
	}
	if res.Generation != 1 {
		t.Fatalf("no-change sync with symlink bumped generation to %d, want 1", res.Generation)
	}
}

func symlinkMtime(t *testing.T, path string) (time.Time, int64, error) {
	t.Helper()
	fi, err := os.Lstat(path)
	if err != nil {
		return time.Time{}, 0, err
	}
	return fi.ModTime(), fi.ModTime().UnixNano(), nil
}
