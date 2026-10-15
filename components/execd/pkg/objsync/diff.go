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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/bmatcuk/doublestar/v4"
)

// WalkResult is the outcome of scanning and diffing the sync roots against the
// previous manifest.
type WalkResult struct {
	// Entries is the complete current listing (dirs, files, symlinks) with
	// digests filled in.
	Entries []FileEntry
	// ToUpload lists entries whose blobs may need to be uploaded (new or
	// changed content). The store-side existence check happens in the sync
	// phase, so unchanged-but-moved files also land here and dedupe remotely.
	ToUpload []FileEntry
	// DeletedCount is how many previous entries vanished from the tree.
	DeletedCount int
	// BytesScanned is the total size of hashed files.
	BytesScanned int64
}

// stableFingerprint is the rsync-style quick-check identity. Identical
// fingerprints against the previous manifest let the walker reuse the old
// digest without reading the file.
func stableFingerprint(e *FileEntry) string {
	return fmt.Sprintf("%s|%d|%d|%d|%d|%d", e.Type, e.Size, e.Mode, e.UID, e.GID, e.MtimeNano)
}

// Walk scans roots, diffs against old (may be nil) and digests new or changed
// content. verify=true re-hashes every file regardless of fingerprint
// (periodic safety net against fingerprint collisions). Files that keep
// changing under the walker fail the walk: a manifest must describe a stable
// tree, and pause-time task quiescing is the mechanism that guarantees it.
func Walk(ctx context.Context, roots, exclude []string, old *Manifest, verify bool, concurrency int) (*WalkResult, error) {
	if concurrency <= 0 {
		concurrency = 4
	}
	oldIndex := map[string]FileEntry{}
	if old != nil {
		for _, f := range old.Files {
			oldIndex[f.Path] = f
		}
	}

	var entries []FileEntry
	var toHash []FileEntry
	var bytesScanned int64

	for _, root := range roots {
		root = filepath.Clean(root)
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return fmt.Errorf("walk %s: %w", path, err)
			}
			if path == root {
				return nil // the root itself is a mount point, not an entry
			}
			rel, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			if excluded(exclude, root, rel) {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}

			entry, hashErr := statEntry(path, d)
			if hashErr != nil {
				return hashErr
			}
			// Symlink targets are hashed inline: reading a link is a single
			// cheap syscall, and their blob is the target string itself.
			if entry.Type == TypeSymlink {
				target, readErr := os.Readlink(path)
				if readErr != nil {
					return fmt.Errorf("readlink %s: %w", path, readErr)
				}
				entry.SHA256 = sha256Hex([]byte(target))
				entries = append(entries, entry)
				toHash = append(toHash, entry) // target blobs are tiny; existence check is cheap
				return nil
			}

			if prev, ok := oldIndex[path]; ok && !verify && stableFingerprint(&prev) == stableFingerprint(&entry) {
				entry.SHA256 = prev.SHA256 // unchanged: reuse digest, never open the file
				entries = append(entries, entry)
				return nil
			}
			entries = append(entries, entry)
			if entry.Type == TypeRegular {
				toHash = append(toHash, entry)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	// Hash new/changed regular files with a bounded worker pool, guarding
	// against files mutated mid-scan (stat before and after hashing).
	digests, err := hashEntries(ctx, toHash, concurrency)
	if err != nil {
		return nil, err
	}
	toHashSet := make(map[string]struct{}, len(toHash))
	for _, e := range toHash {
		toHashSet[e.Path] = struct{}{}
	}
	var uploads []FileEntry
	for i := range entries {
		if d, ok := digests[entries[i].Path]; ok {
			entries[i].SHA256 = d
		}
		if _, want := toHashSet[entries[i].Path]; want {
			uploads = append(uploads, entries[i])
		}
	}

	current := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		current[e.Path] = struct{}{}
		if e.Type == TypeRegular {
			bytesScanned += e.Size
		}
	}
	deleted := 0
	if old != nil {
		for _, f := range old.Files {
			if _, ok := current[f.Path]; !ok {
				deleted++
			}
		}
	}

	return &WalkResult{
		Entries:      entries,
		ToUpload:     uploads,
		DeletedCount: deleted,
		BytesScanned: bytesScanned,
	}, nil
}

// hashEntries digests entries missing a SHA256, re-statting before and after
// so a file changed mid-hash is retried a bounded number of times. It returns
// path → digest.
func hashEntries(ctx context.Context, entries []FileEntry, concurrency int) (map[string]string, error) {
	type result struct {
		path   string
		digest string
	}
	work := make(chan FileEntry)
	results := make(chan result, len(entries))
	errCh := make(chan error, concurrency)

	workers := concurrency
	if len(entries) < workers {
		workers = len(entries)
	}
	if workers == 0 {
		return map[string]string{}, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for entry := range work {
				digest, err := hashStable(entry)
				if err != nil {
					errCh <- err
					continue
				}
				results <- result{path: entry.Path, digest: digest}
			}
		}()
	}

	for _, e := range entries {
		if e.Type != TypeRegular || e.SHA256 != "" {
			continue
		}
		select {
		case work <- e:
		case <-ctx.Done():
			close(work)
			wg.Wait()
			return nil, ctx.Err()
		}
	}
	close(work)
	wg.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}
	digests := make(map[string]string, len(results))
	for len(results) > 0 {
		r := <-results
		digests[r.path] = r.digest
	}
	return digests, nil
}

// hashStable hashes the file at entry.Path and verifies it did not change
// while being read (fingerprint captured at walk time vs post-hash stat).
func hashStable(before FileEntry) (string, error) {
	const attempts = 3
	for attempt := 0; attempt < attempts; attempt++ {
		f, err := os.Open(before.Path) //nolint:gosec // path comes from our own walk
		if err != nil {
			return "", fmt.Errorf("open %s: %w", before.Path, err)
		}
		h := sha256.New()
		_, copyErr := io.Copy(h, f)
		f.Close()
		if copyErr != nil {
			return "", fmt.Errorf("read %s: %w", before.Path, copyErr)
		}
		fi, err := os.Lstat(before.Path)
		if err != nil {
			return "", fmt.Errorf("lstat %s: %w", before.Path, err)
		}
		after, err := entryFromFileInfo(before.Path, fi)
		if err != nil {
			return "", err
		}
		if stableFingerprint(&before) == stableFingerprint(&after) {
			return hex.EncodeToString(h.Sum(nil)), nil
		}
		// The file changed under us: hash again against the new fingerprint.
		before = after
	}
	return "", fmt.Errorf("file %s kept changing during sync", before.Path)
}

// excluded reports whether rel (relative to root) or its absolute path
// matches any doublestar exclude pattern. Matching a directory prunes its
// whole subtree (handled by the caller via filepath.SkipDir).
func excluded(patterns []string, root, rel string) bool {
	abs := filepath.Join(root, rel)
	for _, p := range patterns {
		if ok, _ := doublestar.Match(p, rel); ok {
			return true
		}
		if ok, _ := doublestar.Match(p, abs); ok {
			return true
		}
	}
	return false
}

// statEntry builds a FileEntry from a walked dir entry.
func statEntry(path string, d fs.DirEntry) (FileEntry, error) {
	fi, err := d.Info()
	if err != nil {
		return FileEntry{}, fmt.Errorf("stat %s: %w", path, err)
	}
	return entryFromFileInfo(path, fi)
}

func entryFromFileInfo(path string, fi os.FileInfo) (FileEntry, error) {
	entry := FileEntry{
		Path:      path,
		Mode:      uint32(fi.Mode().Perm()),
		MtimeNano: fi.ModTime().UnixNano(),
		UID:       fileOwner(fi),
		GID:       fileGroup(fi),
	}
	switch {
	case fi.Mode().IsRegular():
		entry.Type = TypeRegular
		entry.Size = fi.Size()
	case fi.IsDir():
		entry.Type = TypeDir
	case fi.Mode()&os.ModeSymlink != 0:
		entry.Type = TypeSymlink
	default:
		return FileEntry{}, fmt.Errorf("skipping special file %s (mode %s): not syncable", path, fi.Mode())
	}
	return entry, nil
}
