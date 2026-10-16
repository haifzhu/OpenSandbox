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

package controller

import (
	"testing"

	"github.com/alibaba/opensandbox/execd/pkg/objsync"
)

// TestBuildSyncOptionsDefaults pins the wiring between the wire request and
// the engine: adaptive compression is ON unless the caller opts out, and the
// retention window passes through. A missed default here is invisible at
// runtime (syncs succeed, just never compress), so it needs a direct test.
func TestBuildSyncOptionsDefaults(t *testing.T) {
	target := objsync.Target{Bucket: "b", Base: "p/1"}

	got := buildSyncOptions(snapshotSyncRequest{
		Target: "s3://b/p/1", Roots: []string{"/workspace"},
	}, target)
	if !got.Compress {
		t.Fatal("compression must default to enabled")
	}
	if got.RetentionGenerations != 0 {
		t.Fatalf("retention default should defer to the library, got %d", got.RetentionGenerations)
	}

	off := false
	got = buildSyncOptions(snapshotSyncRequest{
		Target: "s3://b/p/1", Roots: []string{"/workspace"},
		Compress: &off, Retention: 7,
	}, target)
	if got.Compress {
		t.Fatal("explicit compress=false must be honoured")
	}
	if got.RetentionGenerations != 7 {
		t.Fatalf("retention = %d, want 7", got.RetentionGenerations)
	}
}
