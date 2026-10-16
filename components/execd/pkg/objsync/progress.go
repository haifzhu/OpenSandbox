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
	"sync"
	"time"
)

// Progress is a point-in-time view of a sync or restore in flight. A 2 GB
// first sync takes minutes; without this the only feedback is a silent wait.
type Progress struct {
	Operation   string `json:"operation"` // "sync" | "restore" | ""
	Phase       string `json:"phase"`     // walking|hashing|uploading|manifest|gc|downloading|done|failed|idle
	FilesTotal  int    `json:"filesTotal"`
	FilesDone   int    `json:"filesDone"`
	BytesTotal  int64  `json:"bytesTotal"`
	BytesDone   int64  `json:"bytesDone"`
	CurrentFile string `json:"currentFile,omitempty"`
	StartedAt   string `json:"startedAt,omitempty"`
	ElapsedMs   int64  `json:"elapsedMs"`
	LastError   string `json:"lastError,omitempty"`
}

type progressTracker struct {
	mu  sync.Mutex
	p   Progress
	beg time.Time
}

var progressState = &progressTracker{}

// SnapshotProgress returns the current progress (zero-value idle when nothing
// is running). Safe to call from any goroutine.
func SnapshotProgress() Progress {
	progressState.mu.Lock()
	defer progressState.mu.Unlock()
	p := progressState.p
	if !progressState.beg.IsZero() {
		p.ElapsedMs = time.Since(progressState.beg).Milliseconds()
	}
	return p
}

func progressBegin(op string) {
	progressState.mu.Lock()
	defer progressState.mu.Unlock()
	progressState.beg = time.Now()
	progressState.p = Progress{
		Operation: op,
		Phase:     "starting",
		StartedAt: progressState.beg.UTC().Format(time.RFC3339),
	}
}

func progressPhase(phase string) {
	progressState.mu.Lock()
	defer progressState.mu.Unlock()
	progressState.p.Phase = phase
}

func progressTotals(op string, files int, bytes int64) {
	progressState.mu.Lock()
	defer progressState.mu.Unlock()
	progressState.p.Operation = op
	progressState.p.FilesTotal = files
	progressState.p.BytesTotal = bytes
}

func progressFile(name string, bytes int64) {
	progressState.mu.Lock()
	defer progressState.mu.Unlock()
	progressState.p.FilesDone++
	progressState.p.BytesDone += bytes
	progressState.p.CurrentFile = name
}

func progressEnd(err error) {
	progressState.mu.Lock()
	defer progressState.mu.Unlock()
	if err != nil {
		progressState.p.Phase = "failed"
		progressState.p.LastError = err.Error()
		return
	}
	progressState.p.Phase = "done"
}
