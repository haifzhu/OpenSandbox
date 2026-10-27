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
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestSyncHotFileRaceCommitsSelfConsistentGeneration pins the walk→upload race
// fix. The walk digests daemon.log at instant T; the daemon appends before the
// upload reads the file; the upload stores the newer bytes. Addressing the
// blob by the walk-time digest used to commit a manifest whose entry never
// matched the stored content — every restore of that generation fail-closed
// with "digest mismatch" and the sandbox could never resume again.
//
// The interleaving is reproduced deterministically by handing uploadBlobs an
// entry carrying the OLD digest while the file on disk already holds the NEW
// content — exactly the state the race leaves behind.
func TestSyncHotFileRaceCommitsSelfConsistentGeneration(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()
	path := filepath.Join(root, "daemon.log")

	oldContent := "heartbeats at walk time\n"
	newContent := oldContent + "heartbeat appended during the upload window\n"
	if err := os.WriteFile(path, []byte(newContent), 0o644); err != nil {
		t.Fatal(err)
	}

	// Walk-time snapshot of a file that has since been appended to.
	stale := FileEntry{
		Path:      path,
		Type:      TypeRegular,
		Size:      int64(len(oldContent)),
		Mode:      0o644,
		SHA256:    sha256Hex([]byte(oldContent)),
		MtimeNano: time.Now().UnixNano(),
	}

	var uploaded, bytesUp atomic.Int64
	var skipped atomic.Int64
	encodings := map[string]string{}
	staged := map[string]stagedUpload{}
	syncer := &Syncer{Store: store}
	if err := syncer.uploadBlobs(ctx, SyncOptions{Target: target, Roots: []string{root}},
		[]FileEntry{stale}, nil, 1, DefaultPartSize, 2, &uploaded, &bytesUp, &skipped, encodings, staged); err != nil {
		t.Fatalf("uploadBlobs: %v", err)
	}

	// The staged digest must describe the bytes actually stored, not the walk.
	st, ok := staged[path]
	if !ok {
		t.Fatal("entry not staged")
	}
	if want := sha256Hex([]byte(newContent)); st.digest != want {
		t.Fatalf("staged digest = %s (walk-time), want %s (actual content)", st.digest, want)
	}
	if st.size != int64(len(newContent)) {
		t.Fatalf("staged size = %d, want %d", st.size, len(newContent))
	}

	// Committing a manifest from the staged values (as Sync does) must
	// restore cleanly — the old code poisoned this generation.
	entry := stale
	entry.SHA256 = st.digest
	entry.Size = st.size
	entry.Encoding = encodings[path]
	manifest := &Manifest{
		FormatVersion: FormatVersionV1,
		Generation:    1,
		Roots:         []string{root},
		CreatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		Files:         []FileEntry{entry},
	}
	manifest.Sort()
	body, digest, err := encodeJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, target.Bucket, target.manifestKey(1), mustReader(body), int64(len(body)), ""); err != nil {
		t.Fatal(err)
	}
	latest := Latest{Generation: 1, ManifestDigest: digest}
	lb, _, err := encodeJSON(latest)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, target.Bucket, target.latestKey(), mustReader(lb), int64(len(lb)), ""); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(ctx, store, RestoreOptions{Target: target, Roots: []string{root}}); err != nil {
		t.Fatalf("restore of the raced generation failed: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != newContent {
		t.Fatalf("restored content = %q, want the appended content %q", got, newContent)
	}
}

// TestSyncHotFileRaceWouldFailBeforeFix documents the old behaviour with the
// old addressing: a manifest built from the walk-time digest (no staging)
// fail-closes at restore. It constructs the poisoned generation directly, the
// way the pre-fix upload path did, and proves restore rejects it.
func TestSyncHotFileRaceWouldFailBeforeFix(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()
	path := filepath.Join(root, "daemon.log")

	oldContent := "walk time\n"
	newContent := oldContent + "appended mid-window\n"
	if err := os.WriteFile(path, []byte(newContent), 0o644); err != nil {
		t.Fatal(err)
	}

	// Pre-fix shape: blob addressed by the walk digest, holding newer bytes.
	staleDigest := sha256Hex([]byte(oldContent))
	key := target.blobKeyFor(staleDigest, "")
	if err := store.Put(ctx, target.Bucket, key, mustReader([]byte(newContent)), int64(len(newContent)), ""); err != nil {
		t.Fatal(err)
	}
	entry := FileEntry{
		Path: path, Type: TypeRegular,
		Size: int64(len(oldContent)), Mode: 0o644, SHA256: staleDigest,
		MtimeNano: time.Now().UnixNano(),
	}
	manifest := &Manifest{
		FormatVersion: FormatVersionV1, Generation: 1, Roots: []string{root},
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Files:     []FileEntry{entry},
	}
	manifest.Sort()
	body, digest, err := encodeJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, target.Bucket, target.manifestKey(1), mustReader(body), int64(len(body)), ""); err != nil {
		t.Fatal(err)
	}
	lb, _, err := encodeJSON(Latest{Generation: 1, ManifestDigest: digest})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, target.Bucket, target.latestKey(), mustReader(lb), int64(len(lb)), ""); err != nil {
		t.Fatal(err)
	}

	if _, err := Restore(ctx, store, RestoreOptions{Target: target, Roots: []string{root}}); err == nil {
		t.Fatal("restore must fail closed on the poisoned generation")
	} else if want := "digest mismatch"; !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want it to mention %q", err, want)
	}
}

func mustReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
