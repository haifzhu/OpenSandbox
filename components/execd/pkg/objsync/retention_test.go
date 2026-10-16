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
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestGCTrimsOldGenerationsAndOrphanBlobs drives five generations, keeps the
// newest three, and checks that (a) trimmed manifests are gone, (b) blobs no
// survivor references are gone, and (c) every survivor still restores.
func TestGCTrimsOldGenerationsAndOrphanBlobs(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()
	syncer := &Syncer{Store: store}

	// Generation 1..5, each with a unique file so blobs differ per generation.
	for gen := 1; gen <= 5; gen++ {
		writeFile(t, filepath.Join(root, "keep.txt"), "stable-content", 0o644)
		writeFile(t, filepath.Join(root, "gen.txt"), strings.Repeat("x", gen*100), 0o644)
		time.Sleep(2 * time.Millisecond)
		if _, err := syncer.Sync(ctx, SyncOptions{
			Roots: []string{root}, Target: target, RetentionGenerations: 3,
		}); err != nil {
			t.Fatalf("sync gen %d: %v", gen, err)
		}
	}

	keys := store.Keys(target.Bucket)
	manifests := 0
	for _, k := range keys {
		if strings.HasSuffix(k, "manifest.json") {
			manifests++
		}
	}
	if manifests != 3 {
		t.Fatalf("retained %d manifests, want 3 (keys: %v)", manifests, keys)
	}
	for _, gone := range []string{"gen/1/manifest.json", "gen/2/manifest.json"} {
		for _, k := range keys {
			if strings.HasSuffix(k, gone) {
				t.Fatalf("trimmed manifest still present: %s", k)
			}
		}
	}

	// Survivors restore.
	for gen := 3; gen <= 5; gen++ {
		if _, err := Restore(ctx, store, RestoreOptions{Target: target, Roots: []string{root}, Generation: uint64(gen)}); err != nil {
			t.Fatalf("restore retained gen %d: %v", gen, err)
		}
	}

	// The orphan check: blobs present must all be referenced by survivors.
	latest, err := ReadLatest(ctx, store, target)
	if err != nil {
		t.Fatal(err)
	}
	referenced := map[string]struct{}{}
	for gen := uint64(3); gen <= latest.Generation; gen++ {
		m, err := ReadManifest(ctx, store, target, gen, "")
		if err != nil {
			t.Fatalf("read survivor gen %d: %v", gen, err)
		}
		for _, f := range m.Files {
			if f.SHA256 != "" {
				referenced[f.SHA256] = struct{}{}
			}
		}
	}
	for _, k := range store.Keys(target.Bucket) {
		if !strings.Contains(k, "/blobs/") {
			continue
		}
		hash := k[strings.LastIndex(k, "/")+1:]
		hash = strings.TrimSuffix(hash, "."+EncodingZstd)
		if _, ok := referenced[hash]; !ok {
			t.Fatalf("orphan blob survived GC: %s", k)
		}
	}
}

// TestGCKeepsEverythingWhenUnderRetention: with fewer generations than the
// retention window, GC must be a no-op.
func TestGCKeepsEverythingWhenUnderRetention(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.txt"), "a", 0o644)
	syncer := &Syncer{Store: store}
	if _, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target, RetentionGenerations: 5}); err != nil {
		t.Fatal(err)
	}
	before := len(store.Keys(target.Bucket))
	if _, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target, RetentionGenerations: 5}); err != nil {
		t.Fatal(err)
	}
	// No-change fence also holds, so nothing new was written anyway.
	if got := len(store.Keys(target.Bucket)); got != before {
		t.Fatalf("key count changed on no-op sync: %d -> %d", before, got)
	}
}

// TestCompressionRoundtrip: compressible content is stored compressed, is
// declared as such in the manifest, and restores byte-identically.
func TestCompressionRoundtrip(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()

	// Highly compressible payload (repetitive text).
	body := strings.Repeat("the quick brown fox jumps over the lazy dog\n", 4000) // ~176 KiB
	writeFile(t, filepath.Join(root, "big.txt"), body, 0o644)
	// Incompressible payload:真随机字节(zstd 无法压缩)。
	randBytes := make([]byte, 64<<10)
	if _, err := rand.Read(randBytes); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "blob.bin"), randBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	syncer := &Syncer{Store: store}
	if _, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target, Compress: true}); err != nil {
		t.Fatal(err)
	}

	manifest, err := ReadManifest(ctx, store, target, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	var txt, bin *FileEntry
	for i := range manifest.Files {
		switch filepath.Base(manifest.Files[i].Path) {
		case "big.txt":
			txt = &manifest.Files[i]
		case "blob.bin":
			bin = &manifest.Files[i]
		}
	}
	if txt == nil || bin == nil {
		t.Fatal("manifest missing expected entries")
	}
	if txt.Encoding != EncodingZstd {
		t.Fatalf("compressible file encoding = %q, want %q", txt.Encoding, EncodingZstd)
	}
	if bin.Encoding != "" {
		t.Fatalf("incompressible file encoding = %q, want raw", bin.Encoding)
	}
	// The compressed blob is stored under a suffixed key.
	found := false
	for _, k := range store.Keys(target.Bucket) {
		if strings.Contains(k, txt.SHA256+"."+EncodingZstd) {
			found = true
		}
	}
	if !found {
		t.Fatalf("compressed blob key missing suffix .%s", EncodingZstd)
	}

	want := snapshotTree(t, root)
	wipeTree(t, root)
	if _, err := Restore(ctx, store, RestoreOptions{Target: target, Roots: []string{root}}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	compareTrees(t, want, snapshotTree(t, root))
}

// TestRestoreRejectsUnknownEncoding fails closed rather than writing garbage.
func TestRestoreRejectsUnknownEncoding(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.txt"), "a", 0o644)
	syncer := &Syncer{Store: store}
	if _, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target}); err != nil {
		t.Fatal(err)
	}
	manifest, err := ReadManifest(ctx, store, target, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	manifest.Files[0].Encoding = "br-from-the-future" // corrupt on purpose
	body, digest, err := encodeJSON(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, target.Bucket, target.manifestKey(2), strings.NewReader(string(body)), int64(len(body)), digest); err != nil {
		t.Fatal(err)
	}
	latestBody := `{"generation":2,"manifestDigest":"` + digest + `"}`
	if err := store.Put(ctx, target.Bucket, target.latestKey(), strings.NewReader(latestBody), int64(len(latestBody)), ""); err != nil {
		t.Fatal(err)
	}
	other := t.TempDir()
	if _, err := Restore(ctx, store, RestoreOptions{Target: target, Roots: []string{other}}); err == nil {
		t.Fatal("restore must fail closed on an unknown encoding")
	}
}

// TestProgressTracksSync drives a sync and checks the tracker ends in "done"
// with totals recorded (the Job log reads exactly these fields).
func TestProgressTracksSync(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.txt"), strings.Repeat("data", 100), 0o644)
	writeFile(t, filepath.Join(root, "b.txt"), "more", 0o644)

	if got := SnapshotProgress(); got.Operation != "" {
		t.Fatalf("progress should start idle, got %+v", got)
	}

	syncer := &Syncer{Store: store}
	if _, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target}); err != nil {
		t.Fatal(err)
	}
	p := SnapshotProgress()
	if p.Operation != "sync" || p.Phase != "done" {
		t.Fatalf("progress = %+v, want sync/done", p)
	}
	if p.FilesTotal != 3 || p.FilesDone != 3 { // 2 files + 1 dir
		t.Fatalf("files: %d/%d, want 3/3", p.FilesDone, p.FilesTotal)
	}
	if p.BytesTotal < 400 {
		t.Fatalf("bytes total = %d, want >= 400", p.BytesTotal)
	}
}

// TestProgressReportsFailure keeps failures visible to the Job log.
func TestProgressReportsFailure(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.txt"), "a", 0o644)
	syncer := &Syncer{Store: NewMemStore(0)} // healthy first sync
	if _, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target}); err != nil {
		t.Fatal(err)
	}
	// Now fail the store on every write.
	broken := &Syncer{Store: NewMemStore(1)}
	if _, err := broken.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target}); err == nil {
		t.Fatal("expected failure")
	}
	if p := SnapshotProgress(); p.Phase != "failed" || p.LastError == "" {
		t.Fatalf("progress = %+v, want failed with LastError", p)
	}
}
