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

package web

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func newGateRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(mutatingSyncGateMiddleware())
	r.POST("/command", func(c *gin.Context) { c.Status(http.StatusOK) })
	r.GET("/command/status/x", func(c *gin.Context) { c.Status(http.StatusOK) })
	r.POST("/internal/snapshot/sync", func(c *gin.Context) { c.Status(http.StatusOK) })
	return r
}

func TestSyncGateRejectsMutatingRequestsDuringSync(t *testing.T) {
	r := newGateRouter()

	release, ok := beginWorkspaceSync(time.Second)
	if !ok {
		t.Fatal("expected to acquire the sync gate")
	}
	defer release()

	// Mutating exec request → fast 503.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/command", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("POST /command during sync = %d, want 503", w.Code)
	}

	// Read-only and internal control-plane paths stay available.
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/command/status/x"},
		{http.MethodPost, "/internal/snapshot/sync"},
	} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("%s %s during sync = %d, want 200", tc.method, tc.path, w.Code)
		}
	}
}

func TestSyncGateDrainsInFlightRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(mutatingSyncGateMiddleware())

	started := make(chan struct{})
	release := make(chan struct{})
	r.POST("/command", func(c *gin.Context) {
		close(started)
		<-release // simulate a long-running command holding the read lock
		c.Status(http.StatusOK)
	})
	go func() {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/command", nil))
	}()
	<-started

	// Bounded drain: the sync cannot start while the command runs.
	if _, ok := beginWorkspaceSync(50 * time.Millisecond); ok {
		t.Fatal("sync acquired while an in-flight command holds the gate")
	}

	// Command finishes → a sync with a generous window acquires.
	close(release)
	got, ok := beginWorkspaceSync(2 * time.Second)
	if !ok {
		t.Fatal("sync failed to acquire after drain")
	}
	got()
}
