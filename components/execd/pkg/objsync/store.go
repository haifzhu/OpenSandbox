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
	"io"
)

// ErrNotFound is returned by ObjectStore reads for missing keys.
var ErrNotFound = errors.New("objsync: object not found")

// ObjectStore is the minimal S3-compatible surface the sync engine needs.
// Implementations must be safe for concurrent use.
type ObjectStore interface {
	// Get streams the object into w and returns its size. Missing objects
	// return ErrNotFound.
	Get(ctx context.Context, bucket, key string, w io.Writer) (int64, error)
	// Put stores size bytes from r under key. sha256Hex, when non-empty, is
	// the payload digest used for request signing (content addressing means
	// the sync engine always knows it).
	Put(ctx context.Context, bucket, key string, r io.Reader, size int64, sha256Hex string) error
	// PutMultipart uploads size bytes using the multipart API in partSize
	// chunks. Used for objects at or above the multipart threshold.
	PutMultipart(ctx context.Context, bucket, key string, r io.Reader, size, partSize int64) error
	// Head reports whether an object exists and its size.
	Head(ctx context.Context, bucket, key string) (size int64, exists bool, err error)
	// Delete removes an object; deleting a missing object is not an error.
	Delete(ctx context.Context, bucket, key string) error
	// List returns up to max keys under prefix after the marker and the next
	// marker (empty when done).
	List(ctx context.Context, bucket, prefix, marker string, max int) (keys []string, nextMarker string, err error)
}
