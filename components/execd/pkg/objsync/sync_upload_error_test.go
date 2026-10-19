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
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestSyncUploadFailureReturnsPromptly pins the dispatcher's error path. jobs
// is unbuffered, so a worker that returns on error stops receiving: once every
// worker has failed, a dispatcher that does not watch for failure blocks on
// jobs <- forever, and the caller only learns the real object-store error when
// its own deadline cancels the context. Seven files with four workers
// guarantees more entries than workers, which is the shape that used to hang.
func TestSyncUploadFailureReturnsPromptly(t *testing.T) {
	root := t.TempDir()
	// Distinct content per file: identical bytes dedupe to a single blob, and
	// then only one worker ever fails and the dispatcher shape under test
	// never appears.
	for i := 0; i < 7; i++ {
		writeFile(t, filepath.Join(root, fmt.Sprintf("f%d.txt", i)), fmt.Sprintf("content-%d", i), 0o644)
	}
	target := mustTarget(t)

	// failAfter=1 lets the first blob through and fails every later Put, so
	// all four workers exit well before the queue drains.
	syncer := &Syncer{Store: NewMemStore(1)}

	done := make(chan error, 1)
	go func() {
		_, err := syncer.Sync(context.Background(), SyncOptions{Roots: []string{root}, Target: target})
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected the injected store failure to fail the sync")
		}
		if !strings.Contains(err.Error(), "injected failure") {
			t.Fatalf("error lost the store cause: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Sync blocked after an upload error; the dispatcher is stuck on jobs <-")
	}
}
