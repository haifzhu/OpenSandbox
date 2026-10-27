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
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
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
	// RetentionGenerations keeps the newest N generations; older ones and
	// their unreferenced blobs are collected after a successful commit.
	// 0 means DefaultRetentionGenerations.
	RetentionGenerations int
	// Compress enables zstd for content that shrinks by at least
	// compressionGainThreshold; incompressible data stays raw.
	Compress bool
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
func (s *Syncer) Sync(ctx context.Context, opts SyncOptions) (result *SyncResult, err error) {
	if len(opts.Roots) == 0 {
		return nil, errors.New("objsync: no sync roots")
	}
	start := time.Now()
	progressBegin("sync")
	defer func() { progressEnd(err) }()

	concurrency := opts.Concurrency
	if concurrency <= 0 {
		// Deliberately lower than restore's default: the dispatcher hands
		// workers in-memory payloads for compressible files (up to 64 MiB
		// before the gain threshold rejects them), so the worker count bounds
		// peak memory as well as I/O.
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

	progressPhase("walking")
	walk, err := Walk(ctx, opts.Roots, opts.Exclude, old, opts.Verify, concurrency)
	if err != nil {
		return nil, fmt.Errorf("walk: %w", err)
	}
	var totalBytes int64
	for i := range walk.Entries {
		if walk.Entries[i].Type == TypeRegular {
			totalBytes += walk.Entries[i].Size
		}
	}
	progressTotals("sync", len(walk.Entries), totalBytes)
	progressPhase("uploading")

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
	encodings := map[string]string{}
	// staged holds, per path, the digest/size of the bytes actually uploaded:
	// a file rewritten between the walk and the upload (a daemon appending to
	// its log) must be recorded by the content that was stored, or the
	// manifest points at a blob whose bytes never hash to the entry digest
	// and every restore of the generation fail-closes.
	staged := map[string]stagedUpload{}
	if err := s.uploadBlobs(ctx, opts, walk.ToUpload, oldHashes, generation, partSize, concurrency, &uploaded, &bytesUploaded, &skipped, encodings, staged); err != nil {
		return nil, err
	}
	// Stamp the storage encoding and the staged digest/size onto the manifest
	// entries. Entries not staged (unchanged since the previous generation)
	// keep the walk values, which the previous manifest already vouched for.
	for i := range walk.Entries {
		if st, ok := staged[walk.Entries[i].Path]; ok {
			walk.Entries[i].SHA256 = st.digest
			walk.Entries[i].Size = st.size
		}
		if enc, ok := encodings[walk.Entries[i].Path]; ok {
			walk.Entries[i].Encoding = enc
		}
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

	progressPhase("manifest")
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

	progressPhase("gc")
	// Retention GC: trim old generations and their orphaned blobs. Best
	// effort — a GC failure must not fail a committed sync.
	if gc, gcErr := GC(ctx, s.Store, opts.Target, opts.RetentionGenerations); gcErr != nil {
		fmt.Fprintf(os.Stderr, "objsync: gc skipped: %v\n", gcErr)
	} else if gc.GenerationsDeleted > 0 || gc.BlobsDeleted > 0 {
		Global.RecordGC(gc.GenerationsDeleted, gc.BlobsDeleted)
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
//
// staged (like encodings) is an out-param the caller merges into the manifest
// entries: every entry processed here — uploaded or skipped as already known —
// is recorded with the digest of the bytes that were (or would have been)
// stored, which may differ from the walk-time digest for hot files.
func (s *Syncer) uploadBlobs(
	ctx context.Context,
	opts SyncOptions,
	toUpload []FileEntry,
	oldHashes map[string]struct{},
	generation uint64,
	partSize int64,
	concurrency int,
	uploaded, bytesUploaded, skipped *atomic.Int64,
	encodings map[string]string,
	staged map[string]stagedUpload,
) error {
	type job struct {
		entry FileEntry
		st    stagedUpload
		key   string
	}
	jobs := make(chan job)
	var wg sync.WaitGroup

	// The dispatcher and the workers share one cancellation. A worker that
	// returns on error stops receiving, and jobs is unbuffered: without this
	// the dispatcher blocks forever on jobs <- and the real object-store
	// error only surfaces when the caller's deadline cancels ctx. One failure
	// must stop dispatch and tear the workers down immediately.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		mu       sync.Mutex
		firstErr error
	)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		mu.Unlock()
	}
	stopErr := func() error {
		mu.Lock()
		defer mu.Unlock()
		return firstErr
	}

	workers := concurrency
	if len(toUpload) < workers {
		workers = len(toUpload)
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := s.uploadOne(ctx, opts, j.entry, j.st, j.key, partSize, bytesUploaded); err != nil {
					fail(err)
					return
				}
				uploaded.Add(1)
				progressFile(j.entry.Path, j.st.size)
			}
		}()
	}

dispatch:
	for _, entry := range toUpload {
		if stopErr() != nil {
			break
		}
		if entry.SHA256 == "" {
			fail(fmt.Errorf("entry %s has no digest", entry.Path))
			break
		}
		// Staging reads the bytes and digests EXACTLY what will be uploaded —
		// never the walk-time digest — so the blob key and the manifest entry
		// cannot disagree for a file that changed in between.
		st, err := s.stagePayload(entry, opts.Compress)
		if err != nil {
			fail(err)
			break
		}
		if encodings != nil {
			encodings[entry.Path] = st.encoding
		}
		if staged != nil {
			staged[entry.Path] = st
		}
		if _, known := oldHashes[st.digest]; known && st.encoding == "" {
			// A previous generation already stored this content raw.
			skipped.Add(1)
			continue
		}
		key := opts.Target.blobKeyFor(st.digest, st.encoding)
		if _, exists, err := s.Store.Head(ctx, opts.Target.Bucket, key); err == nil && exists {
			skipped.Add(1)
			continue
		}
		select {
		case jobs <- job{entry: entry, st: st, key: key}:
		case <-ctx.Done():
			break dispatch
		}
	}
	close(jobs)
	wg.Wait()
	if err := stopErr(); err != nil {
		return fmt.Errorf("upload blobs: %w", err)
	}
	return ctx.Err()
}

// zstdEncoder is shared: EncodeAll is safe for concurrent use, and the
// encoder holds no per-stream state between calls.
var zstdEncoder, _ = zstd.NewWriter(nil,
	zstd.WithEncoderLevel(zstd.SpeedDefault),
	zstd.WithEncoderConcurrency(1))

// stagedUpload is one entry's upload-ready content: digest and size describe
// EXACTLY the bytes that will be stored (the raw form for compressed blobs),
// never an earlier walk-time read. A hot file — a daemon appending to its log
// several times a second — changes between the walk and the upload; using the
// stale digest to address the blob commits a generation whose restore
// fail-closes with "digest mismatch", bricking every later resume of that
// sandbox.
type stagedUpload struct {
	digest   string
	size     int64 // raw byte size the digest covers
	encoding string
	// payload is the stored form when the bytes are held in memory (files up
	// to compressMaxSize, the same bound the compression path always paid);
	// nil means stream from disk, re-reading exactly size bytes.
	payload []byte
}

// stagePayload reads the entry and returns it together with the digest of
// exactly those bytes. Files up to compressMaxSize are read whole; larger
// files are hash-streamed and later uploaded as a bounded prefix — an
// append-only writer cannot retroactively change the bytes a prefix covers,
// so the digest stays truthful even while the tail keeps moving.
func (s *Syncer) stagePayload(entry FileEntry, compress bool) (stagedUpload, error) {
	switch entry.Type {
	case TypeSymlink:
		target, err := os.Readlink(entry.Path) //nolint:gosec // path comes from our own walk
		if err != nil {
			return stagedUpload{}, fmt.Errorf("readlink %s: %w", entry.Path, err)
		}
		body := []byte(target)
		return stagedUpload{digest: sha256Hex(body), size: int64(len(body)), payload: body}, nil
	case TypeRegular:
		info, err := os.Stat(entry.Path) //nolint:gosec // path comes from our own walk
		if err != nil {
			return stagedUpload{}, fmt.Errorf("stat %s: %w", entry.Path, err)
		}
		if info.Size() > compressMaxSize {
			return hashFileCurrent(entry.Path)
		}
		raw, err := os.ReadFile(entry.Path) //nolint:gosec // path comes from our own walk
		if err != nil {
			return stagedUpload{}, fmt.Errorf("read %s: %w", entry.Path, err)
		}
		st := stagedUpload{digest: sha256Hex(raw), size: int64(len(raw)), payload: raw}
		if compress && st.size >= compressMinSize {
			if compressed := zstdEncoder.EncodeAll(raw, nil); float64(len(compressed)) <= float64(st.size)*compressionGainThreshold {
				Global.RecordCompression(int64(len(compressed)), st.size)
				st.encoding, st.payload = EncodingZstd, compressed
			}
		}
		return st, nil
	default:
		return stagedUpload{}, fmt.Errorf("cannot stage entry type %q (%s)", entry.Type, entry.Path)
	}
}

// hashFileCurrent stages a file too large to hold in memory: it hashes
// whatever the file holds right now; the upload re-reads exactly that many
// bytes, so appends racing the upload only extend a tail the digest never
// covered.
func hashFileCurrent(path string) (stagedUpload, error) {
	f, err := os.Open(path) //nolint:gosec // path comes from our own walk
	if err != nil {
		return stagedUpload{}, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return stagedUpload{}, fmt.Errorf("hash %s: %w", path, err)
	}
	return stagedUpload{digest: hex.EncodeToString(h.Sum(nil)), size: n}, nil
}

func (s *Syncer) uploadOne(ctx context.Context, opts SyncOptions, entry FileEntry, st stagedUpload, key string, partSize int64, bytesUploaded *atomic.Int64) error {
	if st.payload != nil {
		// Bytes staged in memory: what is uploaded is what was digested.
		signDigest := ""
		if st.encoding == "" {
			signDigest = st.digest
		}
		if err := s.Store.Put(ctx, opts.Target.Bucket, key, bytes.NewReader(st.payload), int64(len(st.payload)), signDigest); err != nil {
			return fmt.Errorf("put %s: %w", entry.Path, err)
		}
		bytesUploaded.Add(st.size)
		return nil
	}

	// Streamed (files above the in-memory bound): re-read exactly st.size
	// bytes and verify what left the disk still hashes to the staged digest.
	f, err := os.Open(entry.Path) //nolint:gosec // path comes from our own walk
	if err != nil {
		return fmt.Errorf("open %s: %w", entry.Path, err)
	}
	defer f.Close()
	h := sha256.New()
	r := io.TeeReader(io.LimitReader(f, st.size), h)
	if st.size < partSize {
		if err := s.Store.Put(ctx, opts.Target.Bucket, key, r, st.size, st.digest); err != nil {
			return fmt.Errorf("put %s: %w", entry.Path, err)
		}
	} else {
		if err := s.Store.PutMultipart(ctx, opts.Target.Bucket, key, r, st.size, partSize); err != nil {
			return fmt.Errorf("multipart put %s: %w", entry.Path, err)
		}
	}
	if hex.EncodeToString(h.Sum(nil)) != st.digest {
		// The file was rewritten (not appended) mid-upload, so the blob at
		// this key does not hold the addressed content. Delete it before
		// anything can commit a manifest pointing here; the sync errors out
		// and the next pause retries against the rewritten file.
		if delErr := s.Store.Delete(ctx, opts.Target.Bucket, key); delErr != nil {
			return fmt.Errorf("file %s changed while uploading and blob cleanup failed: %w", entry.Path, delErr)
		}
		return fmt.Errorf("file %s changed while uploading; discarded blob", entry.Path)
	}
	bytesUploaded.Add(st.size)
	return nil
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
