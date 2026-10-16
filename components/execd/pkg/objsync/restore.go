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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

// RestoreOptions configures one restore run.
type RestoreOptions struct {
	// Target identifies the sandbox's object namespace.
	Target Target
	// Roots are the absolute paths restores may write under. Every manifest
	// entry must fall inside one of them; anything else fails closed — a
	// manifest must never be able to direct writes at arbitrary paths.
	Roots []string
	// Concurrency bounds download workers (default 4).
	Concurrency int
	// Generation pins a specific generation; 0 means LATEST.
	Generation uint64
}

// Restore materializes the latest (or pinned) generation under the roots.
// Files are downloaded to a sibling temp file, digest-verified, then renamed
// into place; any mismatch aborts the whole restore.
func Restore(ctx context.Context, store ObjectStore, opts RestoreOptions) (*Manifest, error) {
	if len(opts.Roots) == 0 {
		return nil, errors.New("objsync: no restore roots")
	}
	restoreStart := time.Now()
	progressBegin("restore")
	var restoreErr error
	defer func() { progressEnd(restoreErr) }()
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = 4
	}

	generation := opts.Generation
	wantDigest := ""
	if generation == 0 {
		latest, err := ReadLatest(ctx, store, opts.Target)
		if err != nil {
			return nil, fmt.Errorf("read LATEST: %w", err)
		}
		generation = latest.Generation
		wantDigest = latest.ManifestDigest
	}

	manifest, err := ReadManifest(ctx, store, opts.Target, generation, wantDigest)
	if err != nil {
		return nil, fmt.Errorf("read manifest gen %d: %w", generation, err)
	}
	if err := manifest.Validate(); err != nil {
		return nil, fmt.Errorf("manifest invalid: %w", err)
	}
	for _, root := range opts.Roots {
		if err := os.MkdirAll(root, 0o755); err != nil {
			return nil, fmt.Errorf("mkdir root %s: %w", root, err)
		}
	}

	progressPhase("downloading")
	progressTotals("restore", len(manifest.Files), manifest.Stats.BytesScanned)
	// Directories first (depth order by construction: Walk emits parents
	// before children and manifest.Sort is path-lexicographic, which for
	// clean absolute paths still orders parents before their children).
	for _, entry := range manifest.Files {
		if entry.Type != TypeDir {
			continue
		}
		if err := ensureUnderRoots(entry.Path, opts.Roots); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(entry.Path, os.FileMode(entry.Mode)); err != nil {
			return nil, fmt.Errorf("mkdir %s: %w", entry.Path, err)
		}
		writeMetadata(entry.Path, entry.UID, entry.GID)
	}

	type item struct {
		entry FileEntry
	}
	work := make(chan item)
	errCh := make(chan error, concurrency)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range work {
				if err := restoreEntry(ctx, store, opts, generation, it.entry); err != nil {
					errCh <- err
					return
				}
				progressFile(it.entry.Path, it.entry.Size)
			}
		}()
	}

dispatch:
	for _, entry := range manifest.Files {
		if entry.Type == TypeDir {
			continue
		}
		select {
		case work <- item{entry: entry}:
		case <-ctx.Done():
			break dispatch
		}
	}
	close(work)
	wg.Wait()
	select {
	case err := <-errCh:
		restoreErr = err
		return nil, err
	default:
	}
	// Directory mtimes are restored last: writing children refreshes the
	// parent directory mtime, so setting them earlier would be undone. This
	// keeps dir fingerprints stable across restore cycles, which is what
	// lets the no-change idempotency fence hold. Failures are not silent:
	// a skipped directory silently defeats the fence and turns every pause
	// into a full re-upload.
	var chtimesFailed []string
	for _, entry := range manifest.Files {
		if entry.Type != TypeDir {
			continue
		}
		if err := os.Chtimes(entry.Path, timeOf(entry), timeOf(entry)); err != nil {
			chtimesFailed = append(chtimesFailed, entry.Path)
		}
	}
	if len(chtimesFailed) > 0 {
		fmt.Fprintf(os.Stderr, "objsync: restore: directory mtime restore failed for %d entries (idempotency fence degraded): %v\n",
			len(chtimesFailed), chtimesFailed)
	}
	Global.RecordRestore(time.Since(restoreStart), manifest.Stats.BytesScanned, int64(len(manifest.Files)))
	return manifest, ctx.Err()
}

func restoreEntry(ctx context.Context, store ObjectStore, opts RestoreOptions, generation uint64, entry FileEntry) error {
	if err := ensureUnderRoots(entry.Path, opts.Roots); err != nil {
		return err
	}
	if entry.SHA256 == "" {
		return fmt.Errorf("entry %s missing digest", entry.Path)
	}
	key := opts.Target.blobKeyFor(entry.SHA256, entry.Encoding)

	if entry.Type == TypeSymlink {
		var buf bytes.Buffer
		if _, err := store.Get(ctx, opts.Target.Bucket, key, &buf); err != nil {
			return fmt.Errorf("fetch symlink blob %s: %w", entry.Path, err)
		}
		if sha256Hex(buf.Bytes()) != entry.SHA256 {
			return fmt.Errorf("symlink %s target digest mismatch", entry.Path)
		}
		if err := os.Remove(entry.Path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove stale symlink %s: %w", entry.Path, err)
		}
		if err := os.Symlink(buf.String(), entry.Path); err != nil {
			return fmt.Errorf("symlink %s: %w", entry.Path, err)
		}
		// The symlink's own lstat mtime is part of its fingerprint; leaving
		// it at creation time would break the no-change idempotency fence
		// on every restore cycle (a new generation per pause, 0 bytes).
		if err := lutimes(entry.Path, timeOf(entry), timeOf(entry)); err != nil {
			return fmt.Errorf("lutimes symlink %s: %w", entry.Path, err)
		}
		return nil
	}

	tmp, err := os.CreateTemp(filepath.Dir(entry.Path), ".objsync-restore-*")
	if err != nil {
		return fmt.Errorf("temp file for %s: %w", entry.Path, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after successful rename

	h := sha256.New()
	if err := fetchBlob(ctx, store, opts.Target.Bucket, key, entry.Encoding, io.MultiWriter(tmp, h)); err != nil {
		tmp.Close()
		return fmt.Errorf("fetch blob %s: %w", entry.Path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp %s: %w", tmpName, err)
	}
	if hex.EncodeToString(h.Sum(nil)) != entry.SHA256 {
		return fmt.Errorf("file %s digest mismatch (corrupt blob or bucket tampering)", entry.Path)
	}
	if err := os.Chmod(tmpName, os.FileMode(entry.Mode)); err != nil {
		return fmt.Errorf("chmod %s: %w", tmpName, err)
	}
	writeMetadata(tmpName, entry.UID, entry.GID)
	if err := os.Chtimes(tmpName, timeOf(entry), timeOf(entry)); err != nil {
		return fmt.Errorf("chtimes %s: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, entry.Path); err != nil {
		return fmt.Errorf("place %s: %w", entry.Path, err)
	}
	return nil
}

// ensureUnderRoots fails closed unless path is strictly below one of roots.
func ensureUnderRoots(path string, roots []string) error {
	for _, root := range roots {
		root = filepath.Clean(root)
		if path != root && filepath.IsAbs(path) {
			if rel, err := filepath.Rel(root, path); err == nil && rel != ".." && !filepath.IsAbs(rel) && !startsWithDotDot(rel) {
				return nil
			}
		}
	}
	return fmt.Errorf("manifest path %s escapes restore roots", path)
}

func startsWithDotDot(rel string) bool {
	return len(rel) >= 2 && rel[0] == '.' && rel[1] == '.'
}

func timeOf(entry FileEntry) time.Time {
	sec := entry.MtimeNano / int64(time.Second)
	nsec := entry.MtimeNano % int64(time.Second)
	return time.Unix(sec, nsec)
}

// fetchBlob streams a blob into w, transparently decompressing when the
// manifest says the payload is compressed. The digest check downstream is
// always computed over the ORIGINAL content, so integrity is verified the
// same way for both encodings.
func fetchBlob(ctx context.Context, store ObjectStore, bucket, key, encoding string, w io.Writer) error {
	switch encoding {
	case "":
		_, err := store.Get(ctx, bucket, key, w)
		return err
	case EncodingZstd:
		// Stream through a pipe so large blobs never sit in memory.
		pr, pw := io.Pipe()
		go func() {
			_, err := store.Get(ctx, bucket, key, pw)
			_ = pw.CloseWithError(err)
		}()
		dec, err := zstd.NewReader(pr)
		if err != nil {
			return fmt.Errorf("init zstd reader: %w", err)
		}
		defer dec.Close()
		if _, err := io.Copy(w, dec); err != nil {
			return fmt.Errorf("decompress: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unsupported blob encoding %q (this manifest was written by a newer writer)", encoding)
	}
}
