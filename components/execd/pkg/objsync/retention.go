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
	"fmt"
	"sort"
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
// Safety: a blob is deleted only when it appears in NO retained manifest, so
// every generation that survives GC remains fully restorable. Runs after a
// successful sync (best-effort: callers log and continue on error).
//
// Requires list permission on the prefix (oss:ListObjects / s3:ListBucket).
// Without it, GC returns an error and the caller skips it — objects then
// accumulate and rely on the bucket lifecycle policy.
func GC(ctx context.Context, store ObjectStore, target Target, keep int) (*GCStats, error) {
	if keep < 1 {
		keep = DefaultRetentionGenerations
	}

	stats := &GCStats{}

	// 1. Find all generation manifests.
	genKeys, err := listAll(ctx, store, target, target.genPrefix(), "")
	if err != nil {
		return nil, fmt.Errorf("list generations: %w", err)
	}
	genPrefix := target.genPrefix()
	gens := map[uint64]string{} // generation -> full manifest key
	for _, key := range genKeys {
		rel, ok := strings.CutPrefix(key, genPrefix)
		if !ok {
			continue
		}
		gen, ok := parseGenManifest(rel)
		if !ok {
			continue
		}
		gens[gen] = key
	}
	if len(gens) <= keep {
		return stats, nil // nothing to trim
	}

	// 2. Decide which generations survive (highest numbers win).
	numbers := make([]uint64, 0, len(gens))
	for n := range gens {
		numbers = append(numbers, n)
	}
	sort.Slice(numbers, func(i, j int) bool { return numbers[i] > numbers[j] })
	kept := map[uint64]struct{}{}
	for i := 0; i < keep && i < len(numbers); i++ {
		kept[numbers[i]] = struct{}{}
	}

	// 3. Collect every blob hash the survivors reference.
	referenced := map[string]struct{}{}
	for gen := range kept {
		manifest, err := ReadManifest(ctx, store, target, gen, "")
		if err != nil {
			// A retained manifest we cannot read means we must not delete
			// anything: abort rather than risk removing a needed blob.
			return nil, fmt.Errorf("read retained manifest gen %d: %w", gen, err)
		}
		for _, f := range manifest.Files {
			if f.SHA256 != "" {
				referenced[f.SHA256] = struct{}{}
			}
		}
	}

	// 4. Delete the expired manifests.
	for gen, key := range gens {
		if _, ok := kept[gen]; ok {
			continue
		}
		if err := store.Delete(ctx, target.Bucket, key); err != nil {
			return stats, fmt.Errorf("delete manifest gen %d: %w", gen, err)
		}
		stats.GenerationsDeleted++
	}

	// 5. Delete blobs no survivor references.
	blobKeys, err := listAll(ctx, store, target, target.blobPrefix(), "")
	if err != nil {
		// Manifests are already trimmed; report what we did and surface the
		// blob-phase failure so the caller can log it.
		return stats, fmt.Errorf("list blobs: %w", err)
	}
	for _, key := range blobKeys {
		hash := key[strings.LastIndex(key, "/")+1:]
		if _, ok := referenced[hash]; ok {
			continue
		}
		if err := store.Delete(ctx, target.Bucket, key); err != nil {
			return stats, fmt.Errorf("delete blob %s: %w", hash, err)
		}
		stats.BlobsDeleted++
	}
	return stats, nil
}

// listAll pages through every key under prefix.
func listAll(ctx context.Context, store ObjectStore, target Target, prefix, _ string) ([]string, error) {
	full := prefix
	var all []string
	marker := ""
	for {
		keys, next, err := store.List(ctx, target.Bucket, full, marker, 1000)
		if err != nil {
			return nil, err
		}
		all = append(all, keys...)
		if next == "" {
			return all, nil
		}
		marker = next
	}
}

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
