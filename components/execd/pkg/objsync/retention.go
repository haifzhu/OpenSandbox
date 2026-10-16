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
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// DefaultRetentionGenerations is how many generations a workspace keeps when
// the pause policy does not say otherwise. Blobs referenced by a retained
// generation are never deleted, so resume of any retained generation stays
// possible.
const DefaultRetentionGenerations = 3

// GCStats reports what one garbage-collection pass removed.
type GCStats struct {
	GenerationsDeleted int   `json:"generationsDeleted"`
	BlobsDeleted       int   `json:"blobsDeleted"`
	BytesFreed         int64 `json:"bytesFreed"`
}

// GC trims a sandbox's object namespace to the newest `keep` generations and
// deletes blobs that no retained generation references.
//
// It needs NO list permission: generation keys are deterministic
// (gen/<N>/manifest.json) and contiguous, so the collector walks back from the
// current generation until a key is missing. That keeps the required bucket
// permissions to Get/Head/Put/Delete, which is all an object-store pause
// should need.
//
// Safety: a blob is deleted only when it appears in NO retained manifest, and
// an unreadable retained manifest aborts the pass — every surviving generation
// stays restorable.
func GC(ctx context.Context, store ObjectStore, target Target, keep int) (*GCStats, error) {
	if keep < 1 {
		keep = DefaultRetentionGenerations
	}
	stats := &GCStats{}

	latest, err := ReadLatest(ctx, store, target)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return stats, nil // nothing committed yet
		}
		return stats, fmt.Errorf("read LATEST: %w", err)
	}
	current := latest.Generation
	if current <= uint64(keep) {
		return stats, nil // within the retention window
	}

	// 1. Collect every blob hash the survivors reference. An unreadable
	//    survivor aborts: deleting anything then would risk orphaning data.
	referenced := map[string]struct{}{}
	for i := 0; i < keep; i++ {
		gen := current - uint64(i)
		manifest, err := ReadManifest(ctx, store, target, gen, "")
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				break // gap in the sequence; nothing further to protect
			}
			return stats, fmt.Errorf("read retained manifest gen %d: %w", gen, err)
		}
		for _, f := range manifest.Files {
			if f.SHA256 != "" {
				referenced[f.SHA256] = struct{}{}
			}
		}
	}

	// 2. Walk back through the doomed generations, deleting each one's
	//    manifest and any blob the survivors do not reference.
	walked := 0
	for gen := current - uint64(keep); gen >= 1 && walked < maxGCWalkBack; gen-- {
		walked++
		manifest, err := ReadManifest(ctx, store, target, gen, "")
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				break
			}
			return stats, fmt.Errorf("read doomed manifest gen %d: %w", gen, err)
		}
		seen := map[string]struct{}{}
		for _, f := range manifest.Files {
			if f.SHA256 == "" {
				continue
			}
			if _, ok := referenced[f.SHA256]; ok {
				continue
			}
			if _, dup := seen[f.SHA256]; dup {
				continue
			}
			seen[f.SHA256] = struct{}{}
			key := target.blobKeyFor(f.SHA256, f.Encoding)
			if err := store.Delete(ctx, target.Bucket, key); err != nil {
				return stats, fmt.Errorf("delete blob %s: %w", f.SHA256[:12], err)
			}
			stats.BlobsDeleted++
		}
		if err := store.Delete(ctx, target.Bucket, target.manifestKey(gen)); err != nil {
			return stats, fmt.Errorf("delete manifest gen %d: %w", gen, err)
		}
		stats.GenerationsDeleted++
	}
	return stats, nil
}

// maxGCWalkBack bounds the back-walk when a namespace somehow has a very long
// history; the next pass continues from the retained window.
const maxGCWalkBack = 200

// parseGenManifest parses the portion of a manifest key after the target's
// "gen/" prefix, i.e. "<N>/manifest.json".
func parseGenManifest(rel string) (uint64, bool) {
	slash := strings.Index(rel, "/")
	if slash <= 0 || rel[slash+1:] != "manifest.json" {
		return 0, false
	}
	gen, err := strconv.ParseUint(rel[:slash], 10, 64)
	if err != nil {
		return 0, false
	}
	return gen, true
}

// GCSnapshot is the persisted record of the last GC pass, surfaced through the
// metrics endpoint.
type GCSnapshot struct {
	GenerationsDeleted int   `json:"generationsDeleted"`
	BlobsDeleted       int   `json:"blobsDeleted"`
	LastRunUnix        int64 `json:"lastRunUnix"`
}
