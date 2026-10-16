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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultPartSize is the multipart threshold and part size for uploads.
const DefaultPartSize int64 = 8 << 20

// SyncOptions configures one sync run.
type SyncOptions struct {
	// Roots are absolute container paths to sync (e.g. /workspace).
	Roots []string
	// Exclude holds doublestar patterns matched against paths relative to
	// each root (and against the absolute path).
	Exclude []string
	// Target identifies the sandbox's object namespace.
	Target Target
	// Verify re-hashes every file, ignoring fingerprints.
	Verify bool
	// Concurrency bounds hashing and upload workers (default 4).
	Concurrency int
	// PartSize is the multipart threshold/chunk size (default 8MiB).
	PartSize int64
}

// Syncer runs incremental syncs against one store.
type Syncer struct {
	Store ObjectStore
}

// SyncResult reports what one sync did.
type SyncResult struct {
	Generation    uint64    `json:"generation"`
	Stats         SyncStats `json:"stats"`
	UploadedBytes int64     `json:"uploadedBytes"`
	SkippedBlobs  int       `json:"skippedBlobs"`
	Manifest      *Manifest `json:"-"`
	Latest        Latest    `json:"latest"`
}

// Sync performs one incremental sync pass: read LATEST, diff the tree, upload
// missing blobs, write the new manifest and swap LATEST. It is idempotent:
// re-running against the same tree produces the same generation content and
// re-uploads nothing (content-addressed blobs).
func (s *Syncer) Sync(ctx context.Context, opts SyncOptions) (*SyncResult, error) {
	if len(opts.Roots) == 0 {
		return nil, errors.New("objsync: no sync roots")
	}
	start := time.Now()
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = 4
	}
	partSize := opts.PartSize
	if partSize <= 0 {
		partSize = DefaultPartSize
	}

	var old *Manifest
	latest, err := s.readLatest(ctx, opts.Target)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("read LATEST: %w", err)
	}
	if latest != nil {
		old, err = s.readManifest(ctx, opts.Target, latest.Generation, latest.ManifestDigest)
		if err != nil {
			return nil, fmt.Errorf("read manifest gen %d: %w", latest.Generation, err)
		}
	}

	walk, err := Walk(ctx, opts.Roots, opts.Exclude, old, opts.Verify, concurrency)
	if err != nil {
		return nil, fmt.Errorf("walk: %w", err)
	}

	oldHashes := map[string]struct{}{}
	if old != nil {
		for _, f := range old.Files {
			if f.SHA256 != "" {
				oldHashes[f.SHA256] = struct{}{}
			}
		}
	}

	generation := uint64(1)
	if latest != nil {
		generation = latest.Generation + 1
	}

	var uploaded, bytesUploaded atomic.Int64
	skipped := atomic.Int64{}
	if err := s.uploadBlobs(ctx, opts, walk.ToUpload, oldHashes, generation, partSize, concurrency, &uploaded, &bytesUploaded, &skipped); err != nil {
		return nil, err
	}

	manifest := &Manifest{
		FormatVersion: FormatVersionV1,
		Generation:    generation,
		Roots:         opts.Roots,
		CreatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		Files:         walk.Entries,
		Stats: SyncStats{
			FilesTotal:    len(walk.Entries),
			FilesChanged:  len(walk.ToUpload),
			FilesDeleted:  walk.DeletedCount,
			BytesScanned:  walk.BytesScanned,
			BytesUploaded: bytesUploaded.Load(),
		},
	}
	manifest.Sort()
	if err := manifest.Validate(); err != nil {
		return nil, fmt.Errorf("manifest invalid: %w", err)
	}

	// Idempotency fence: a no-op tree (identical entries to the committed
	// generation) commits nothing — retries of an already-successful sync and
	// zero-change pauses reuse the standing generation instead of stacking
	// identical ones.
	if old != nil && manifestsEquivalent(old, manifest) {
		Global.RecordSync(time.Since(start), 0, 0, true)
		return &SyncResult{
			Generation:    old.Generation,
			Stats:         manifest.Stats,
			UploadedBytes: 0,
			SkippedBlobs:  int(skipped.Load()),
			Manifest:      old,
			Latest:        *latest,
		}, nil
	}

	manifestBody, digest, err := encodeJSON(manifest)
	if err != nil {
		return nil, err
	}
	if err := s.Store.Put(ctx, opts.Target.Bucket, opts.Target.manifestKey(generation), bytes.NewReader(manifestBody), int64(len(manifestBody)), digest); err != nil {
		return nil, fmt.Errorf("put manifest: %w", err)
	}

	newLatest := Latest{
		Generation:     generation,
		ManifestDigest: digest,
		CompletedAt:    time.Now().UTC().Format(time.RFC3339Nano),
	}
	latestBody, _, err := encodeJSON(newLatest)
	if err != nil {
		return nil, err
	}
	if err := s.Store.Put(ctx, opts.Target.Bucket, opts.Target.latestKey(), bytes.NewReader(latestBody), int64(len(latestBody)), ""); err != nil {
		return nil, fmt.Errorf("put LATEST: %w", err)
	}

	manifest.Stats.DurationMS = time.Since(start).Milliseconds()
	Global.RecordSync(time.Since(start), bytesUploaded.Load(), int64(len(walk.ToUpload)), false)
	return &SyncResult{
		Generation:    generation,
		Stats:         manifest.Stats,
		UploadedBytes: bytesUploaded.Load(),
		SkippedBlobs:  int(skipped.Load()),
		Manifest:      manifest,
		Latest:        newLatest,
	}, nil
}

// uploadBlobs uploads the changed blobs that do not already exist remotely.
// Existence is checked in order of cost: hashes referenced by the previous
// manifest (in-memory set), then a store HEAD, then upload.
func (s *Syncer) uploadBlobs(
	ctx context.Context,
	opts SyncOptions,
	toUpload []FileEntry,
	oldHashes map[string]struct{},
	generation uint64,
	partSize int64,
	concurrency int,
	uploaded, bytesUploaded, skipped *atomic.Int64,
) error {
	type job struct {
		entry FileEntry
		key   string
	}
	jobs := make(chan job)
	errCh := make(chan error, concurrency)
	var wg sync.WaitGroup

	workers := concurrency
	if len(toUpload) < workers {
		workers = len(toUpload)
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := s.uploadOne(ctx, opts, j.entry, j.key, partSize, bytesUploaded); err != nil {
					errCh <- err
					return
				}
				uploaded.Add(1)
			}
		}()
	}

dispatch:
	for _, entry := range toUpload {
		if entry.SHA256 == "" {
			errCh <- fmt.Errorf("entry %s has no digest", entry.Path)
			break
		}
		if _, known := oldHashes[entry.SHA256]; known {
			skipped.Add(1)
			continue
		}
		key := opts.Target.blobKey(entry.SHA256)
		if _, exists, err := s.Store.Head(ctx, opts.Target.Bucket, key); err == nil && exists {
			skipped.Add(1)
			continue
		}
		select {
		case jobs <- job{entry: entry, key: key}:
		case <-ctx.Done():
			break dispatch
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errCh:
		return fmt.Errorf("upload blobs: %w", err)
	default:
	}
	return ctx.Err()
}

func (s *Syncer) uploadOne(ctx context.Context, opts SyncOptions, entry FileEntry, key string, partSize int64, bytesUploaded *atomic.Int64) error {
	switch entry.Type {
	case TypeSymlink:
		target, err := os.Readlink(entry.Path)
		if err != nil {
			return fmt.Errorf("readlink %s: %w", entry.Path, err)
		}
		body := []byte(target)
		if err := s.Store.Put(ctx, opts.Target.Bucket, key, bytes.NewReader(body), int64(len(body)), entry.SHA256); err != nil {
			return fmt.Errorf("put symlink blob %s: %w", entry.Path, err)
		}
		bytesUploaded.Add(int64(len(body)))
		return nil
	case TypeRegular:
		f, err := os.Open(entry.Path) //nolint:gosec // path comes from our own walk
		if err != nil {
			return fmt.Errorf("open %s: %w", entry.Path, err)
		}
		defer f.Close()
		if entry.Size < partSize {
			if err := s.Store.Put(ctx, opts.Target.Bucket, key, f, entry.Size, entry.SHA256); err != nil {
				return fmt.Errorf("put %s: %w", entry.Path, err)
			}
		} else {
			if err := s.Store.PutMultipart(ctx, opts.Target.Bucket, key, f, entry.Size, partSize); err != nil {
				return fmt.Errorf("multipart put %s: %w", entry.Path, err)
			}
		}
		bytesUploaded.Add(entry.Size)
		return nil
	default:
		return fmt.Errorf("cannot upload entry type %q (%s)", entry.Type, entry.Path)
	}
}

func (s *Syncer) readLatest(ctx context.Context, target Target) (*Latest, error) {
	return ReadLatest(ctx, s.Store, target)
}

func (s *Syncer) readManifest(ctx context.Context, target Target, generation uint64, wantDigest string) (*Manifest, error) {
	return ReadManifest(ctx, s.Store, target, generation, wantDigest)
}

// ReadLatest fetches and validates the LATEST pointer. Shared by the sync
// engine, the restore engine, and the sync Job's post-commit verification.
func ReadLatest(ctx context.Context, store ObjectStore, target Target) (*Latest, error) {
	var buf bytes.Buffer
	if _, err := store.Get(ctx, target.Bucket, target.latestKey(), &buf); err != nil {
		return nil, err
	}
	var latest Latest
	if err := json.Unmarshal(buf.Bytes(), &latest); err != nil {
		return nil, fmt.Errorf("decode LATEST: %w", err)
	}
	if latest.Generation == 0 || len(latest.ManifestDigest) != 64 {
		return nil, fmt.Errorf("invalid LATEST contents")
	}
	return &latest, nil
}

// ReadManifest fetches a generation manifest, optionally verifying it against
// an expected digest.
func ReadManifest(ctx context.Context, store ObjectStore, target Target, generation uint64, wantDigest string) (*Manifest, error) {
	var buf bytes.Buffer
	if _, err := store.Get(ctx, target.Bucket, target.manifestKey(generation), &buf); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(buf.Bytes())
	got := hex.EncodeToString(sum[:])
	if wantDigest != "" && got != wantDigest {
		return nil, fmt.Errorf("manifest gen %d digest mismatch: got %s want %s", generation, got, wantDigest)
	}
	var manifest Manifest
	if err := json.Unmarshal(buf.Bytes(), &manifest); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	return &manifest, nil
}

// manifestsEquivalent compares the file sets of two manifests, ignoring
// generation, top-level timestamps, and stats. File mtimeNano is compared
// with a small tolerance: filesystems like APFS round restored timestamps
// at sub-microsecond granularity, and treating that drift as a change would
// defeat the no-change fence on every restore cycle.
func manifestsEquivalent(a, b *Manifest) bool {
	if len(a.Files) != len(b.Files) {
		return false
	}
	const mtimeTolerance = 2_000 // ns; covers APFS rounding artefacts
	for i := range a.Files {
		x, y := a.Files[i], b.Files[i]
		x.MtimeNano, y.MtimeNano = 0, 0
		if x != y {
			return false
		}
		if diff := a.Files[i].MtimeNano - b.Files[i].MtimeNano; diff > mtimeTolerance || diff < -mtimeTolerance {
			return false
		}
	}
	return true
}

func encodeJSON(v any) ([]byte, string, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(body)
	return body, hex.EncodeToString(sum[:]), nil
}
