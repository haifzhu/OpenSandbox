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
	"os"
	"path/filepath"
	"testing"
)

// TestRestoreAppliesOwnershipLastAndDeepestFirst pins the ordering contract.
// Restore runs as root with only CAP_CHOWN: it must own every path it writes
// or whose timestamps it sets. Handing a directory to the workload uid before
// its children exist makes the remaining mkdir/chmod/chtimes calls fail with
// EPERM, so ownership is applied after all content, children before parents.
func TestRestoreAppliesOwnershipLastAndDeepestFirst(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore(0)
	target := mustTarget(t)
	root := t.TempDir()

	deep := filepath.Join(root, "a", "b")
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(deep, "file.txt"), "x", 0o644)
	writeFile(t, filepath.Join(root, "top.txt"), "y", 0o644)

	syncer := &Syncer{Store: store}
	if _, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target}); err != nil {
		t.Fatal(err)
	}
	wipeTree(t, root)

	original := chownPath
	t.Cleanup(func() { chownPath = original })
	var order []string
	chownPath = func(path string, _, _ int) error {
		order = append(order, path)
		return nil
	}
	if _, err := Restore(ctx, store, RestoreOptions{Target: target, Roots: []string{root}}); err != nil {
		t.Fatal(err)
	}

	index := func(p string) int {
		for i, v := range order {
			if v == p {
				return i
			}
		}
		return -1
	}
	child := index(filepath.Join(deep, "file.txt"))
	parent := index(filepath.Join(root, "a"))
	if child < 0 || parent < 0 {
		t.Fatalf("ownership pass must cover every entry: %v", order)
	}
	if child > parent {
		t.Fatalf("ownership must be applied child-first, got %v", order)
	}
	if last := index(filepath.Join(root, "top.txt")); last < 0 {
		t.Fatalf("top-level file missing from ownership pass: %v", order)
	}
}
