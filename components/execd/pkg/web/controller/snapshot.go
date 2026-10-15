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
	"errors"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/alibaba/opensandbox/execd/pkg/objsync"
)

// Credentials for the object store are read from the execd process
// environment (injected from a Kubernetes Secret by the control plane); they
// are never passed over the HTTP request.
const (
	envAccessKeyID     = "OBJSYNC_ACCESS_KEY_ID"
	envSecretAccessKey = "OBJSYNC_SECRET_ACCESS_KEY"
	envSessionToken    = "OBJSYNC_SESSION_TOKEN"
)

// snapshotSyncRequest is the payload of POST /internal/snapshot/sync, called
// by the pause sync Job (pod-network internal, same trust level as
// /internal/init).
type snapshotSyncRequest struct {
	// Target is the canonical s3://bucket/prefix/... namespace of this
	// sandbox.
	Target string `json:"target" binding:"required"`
	// Roots are absolute container paths to sync.
	Roots []string `json:"roots" binding:"required,min=1"`
	// Exclude holds doublestar patterns relative to each root.
	Exclude []string `json:"exclude,omitempty"`
	// Endpoint/Region/ForcePathStyle address the S3-compatible backend.
	Endpoint       string `json:"endpoint,omitempty"`
	Region         string `json:"region,omitempty"`
	ForcePathStyle bool   `json:"forcePathStyle,omitempty"`
	// Verify re-hashes everything (periodic integrity sweep).
	Verify bool `json:"verify,omitempty"`
	// Concurrency bounds hash/upload workers.
	Concurrency int `json:"concurrency,omitempty"`
}

type snapshotSyncResponse struct {
	Generation    uint64            `json:"generation"`
	Stats         objsync.SyncStats `json:"stats"`
	UploadedBytes int64             `json:"uploadedBytes"`
	SkippedBlobs  int               `json:"skippedBlobs"`
	// Latest echoes the committed pointer so the sync Job can independently
	// verify LATEST against the object store before reporting success.
	Latest objsync.Latest `json:"latest"`
}

// SnapshotController serves the internal pause-sync entrypoint.
type SnapshotController struct {
	ctx *gin.Context
}

// NewSnapshotController builds the controller for one request.
func NewSnapshotController(ctx *gin.Context) *SnapshotController {
	return &SnapshotController{ctx: ctx}
}

// SyncWorkspace runs one incremental workspace sync. Failures answer 500 with
// the error text: the sync Job retries with backoff and the sandbox stays
// running (pause aborts, nothing is committed).
func (c *SnapshotController) SyncWorkspace() {
	var req snapshotSyncRequest
	if err := c.ctx.ShouldBindJSON(&req); err != nil {
		c.ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	for i, root := range req.Roots {
		if !strings.HasPrefix(root, "/") {
			c.ctx.JSON(http.StatusBadRequest, gin.H{"error": "roots must be absolute paths"})
			return
		}
		req.Roots[i] = strings.TrimRight(root, "/")
	}

	target, err := objsync.ParseTarget(req.Target)
	if err != nil {
		c.ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	store := objsync.NewS3Store(objsync.S3Config{
		Endpoint:       req.Endpoint,
		Region:         req.Region,
		AccessKey:      os.Getenv(envAccessKeyID),
		SecretKey:      os.Getenv(envSecretAccessKey),
		SessionToken:   os.Getenv(envSessionToken),
		ForcePathStyle: req.ForcePathStyle,
	})

	syncer := &objsync.Syncer{Store: store}
	result, err := syncer.Sync(c.ctx.Request.Context(), objsync.SyncOptions{
		Roots:       req.Roots,
		Exclude:     req.Exclude,
		Target:      target,
		Verify:      req.Verify,
		Concurrency: req.Concurrency,
	})
	if err != nil {
		// Context cancellation propagates as a non-retryable 499-style
		// answer; everything else is retryable at the Job level.
		status := http.StatusInternalServerError
		if errors.Is(err, c.ctx.Request.Context().Err()) {
			status = StatusClientClosedRequest
		}
		c.ctx.JSON(status, gin.H{"error": err.Error()})
		return
	}
	c.ctx.JSON(http.StatusOK, snapshotSyncResponse{
		Generation:    result.Generation,
		Stats:         result.Stats,
		UploadedBytes: result.UploadedBytes,
		SkippedBlobs:  result.SkippedBlobs,
		Latest:        result.Latest,
	})
}

// StatusClientClosedRequest mirrors nginx's 499 for cancelled request bodies.
const StatusClientClosedRequest = 499
