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
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// workspaceSyncGate implements the pause-time write-quiesce contract
// (OSEP draft, §7): while a workspace sync runs, new mutating/exec requests
// are rejected and in-flight ones are given a bounded drain window.
//
//   - mutating requests hold the read lock for their whole handling, so
//     concurrent commands keep working normally;
//   - the sync handler takes the write lock with a bounded wait, which
//     naturally drains all in-flight mutating requests;
//   - while the sync holds the write lock, new mutating requests fail fast
//     with 503 (the sync Job retries with backoff).
type syncGate struct {
	mu sync.RWMutex
}

var theWorkspaceSyncGate syncGate

// gatedPathPrefixes are the request path prefixes whose mutating methods
// participate in the gate. Read-only endpoints (status, logs, downloads,
// health) stay available during sync.
var gatedPathPrefixes = []string{
	"/command",
	"/session",
	"/pty",
	"/code",
	"/v1/isolated",
	"/files",
	"/directories",
}

func isGatedRequest(method, path string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch:
	default:
		return false
	}
	if strings.HasPrefix(path, "/internal/") {
		return false
	}
	for _, prefix := range gatedPathPrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

// mutatingSyncGateMiddleware gates mutating sandbox requests around a
// workspace sync. It must wrap the whole request (c.Next() inside the read
// lock) so streaming handlers hold their read side until the stream ends.
func mutatingSyncGateMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !isGatedRequest(c.Request.Method, c.Request.URL.Path) {
			c.Next()
			return
		}
		if !theWorkspaceSyncGate.mu.TryRLock() {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"error": "workspace sync in progress; retry shortly",
			})
			return
		}
		defer theWorkspaceSyncGate.mu.RUnlock()
		c.Next()
	}
}

// beginWorkspaceSync acquires exclusive access for a sync, waiting up to
// drainWait for in-flight mutating requests to finish. On timeout it returns
// ok=false and a janitor releases the eventually-acquired lock without ever
// holding it, so a caller that gave up cannot deadlock later syncs.
func beginWorkspaceSync(drainWait time.Duration) (release func(), ok bool) {
	acquired := make(chan struct{})
	go func() {
		theWorkspaceSyncGate.mu.Lock()
		close(acquired)
	}()
	select {
	case <-acquired:
		return theWorkspaceSyncGate.mu.Unlock, true
	case <-time.After(drainWait):
		go func() {
			<-acquired
			theWorkspaceSyncGate.mu.Unlock()
		}()
		return nil, false
	}
}
