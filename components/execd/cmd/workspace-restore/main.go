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

// workspace-restore is the init-container entrypoint that materializes the
// latest workspace generation from object storage before the sandbox's main
// container starts. Credentials come from the same OBJSYNC_* environment
// variables execd uses (projected from a Secret by the control plane).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/alibaba/opensandbox/execd/pkg/log"
	"github.com/alibaba/opensandbox/execd/pkg/objsync"
)

func main() {
	targetFlag := flag.String("target", "", "canonical s3://bucket/prefix namespace of this sandbox")
	rootsFlag := flag.String("roots", "", "comma-separated absolute container paths to restore")
	endpoint := flag.String("endpoint", "", "S3-compatible endpoint (empty = AWS S3)")
	region := flag.String("region", "us-east-1", "S3 region")
	pathStyle := flag.Bool("path-style", false, "force path-style addressing (MinIO/Ceph)")
	concurrency := flag.Int("concurrency", 4, "download concurrency")
	generation := flag.Uint64("generation", 0, "pin a generation; 0 = LATEST")
	flag.Parse()

	if *targetFlag == "" || *rootsFlag == "" {
		fmt.Fprintln(os.Stderr, "workspace-restore: --target and --roots are required")
		os.Exit(2)
	}
	target, err := objsync.ParseTarget(*targetFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "workspace-restore: %v\n", err)
		os.Exit(2)
	}
	var roots []string
	for _, root := range strings.Split(*rootsFlag, ",") {
		root = strings.TrimSpace(root)
		if root == "" {
			continue
		}
		if !strings.HasPrefix(root, "/") {
			fmt.Fprintf(os.Stderr, "workspace-restore: root %q must be absolute\n", root)
			os.Exit(2)
		}
		roots = append(roots, strings.TrimRight(root, "/"))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store := objsync.NewS3Store(objsync.S3Config{
		Endpoint:       *endpoint,
		Region:         *region,
		AccessKey:      os.Getenv("OBJSYNC_ACCESS_KEY_ID"),
		SecretKey:      os.Getenv("OBJSYNC_SECRET_ACCESS_KEY"),
		SessionToken:   os.Getenv("OBJSYNC_SESSION_TOKEN"),
		ForcePathStyle: *pathStyle,
	})

	manifest, err := objsync.Restore(ctx, store, objsync.RestoreOptions{
		Target:      target,
		Roots:       roots,
		Concurrency: *concurrency,
		Generation:  *generation,
	})
	if err != nil {
		log.Error("workspace-restore failed: %v", err)
		fmt.Fprintf(os.Stderr, "workspace-restore: %v\n", err)
		os.Exit(1)
	}
	log.Info("workspace-restore: generation %d, %d files", manifest.Generation, len(manifest.Files))
	fmt.Printf("workspace-restore: restored generation %d (%d entries)\n", manifest.Generation, len(manifest.Files))
}
