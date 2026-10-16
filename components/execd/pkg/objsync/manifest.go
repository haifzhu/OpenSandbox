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

// Package objsync implements the s3-workspace pause provider data plane: an
// incremental, content-addressed workspace synchronizer backed by any
// S3-compatible object store (AWS S3, Aliyun OSS S3 endpoint, MinIO, Ceph).
//
// Layout (every key lives under the resolved prefix, by construction):
//
//	s3://{bucket}/{prefix}/sandboxes/{namespace}/{sandboxId}/LATEST
//	s3://{bucket}/{prefix}/sandboxes/{namespace}/{sandboxId}/gen/{N}/manifest.json
//	s3://{bucket}/{prefix}/sandboxes/{namespace}/{sandboxId}/blobs/{h[0:2]}/{sha256}
//
// Blobs live in a SHARED content-addressed pool outside the per-generation
// namespace: identical content is stored once and referenced by every
// generation that includes it (this is what makes delta syncs cheap — a new
// manifest only uploads hashes the pool lacks). LATEST is the only commit
// point: it is overwritten solely after the full manifest and all referenced
// blobs are durable. Restores read LATEST and never observe partial
// generations.
package objsync

import (
	"fmt"
	"sort"
	"strings"
)

const (
	// FormatVersionV1 is the manifest format produced by this implementation.
	FormatVersionV1 = "s3-workspace-v1"

	// EncodingZstd marks a blob whose payload is a zstd frame of the file.
	EncodingZstd = "zstd"
	// compressMinSize / compressMaxSize bound which files are considered for
	// compression: tiny files gain nothing, huge files would need a second
	// on-disk copy (they take the streaming multipart path instead).
	compressMinSize int64 = 4 << 10  // 4 KiB
	compressMaxSize int64 = 64 << 20 // 64 MiB
	// compressionGainThreshold keeps incompressible payloads raw.
	compressionGainThreshold = 0.90

	latestKeySuffix   = "LATEST"
	manifestKeyFormat = "gen/%d/manifest.json"
	blobKeyFormat     = "blobs/%s/%s"
)

// FileEntry is one filesystem object in a manifest. Path is the absolute path
// inside the sandbox container (e.g. /workspace/app/main.go).
type FileEntry struct {
	Path      string `json:"path"`
	Type      string `json:"type"` // "reg" | "dir" | "link"
	Size      int64  `json:"size"`
	Mode      uint32 `json:"mode"` // permission bits only (0777 style)
	UID       int    `json:"uid"`
	GID       int    `json:"gid"`
	MtimeNano int64  `json:"mtimeNano"`
	// SHA256 identifies the blob: file content digest for "reg", the digest of
	// the link target string for "link", empty for "dir".
	SHA256 string `json:"sha256,omitempty"`
	// Encoding is how the blob payload is stored: "" = raw, "zstd" =
	// zstd-compressed. It is part of the blob key, so identical content
	// stored under different encodings can never collide.
	Encoding string `json:"encoding,omitempty"`
}

// File type constants.
const (
	TypeRegular = "reg"
	TypeDir     = "dir"
	TypeSymlink = "link"
)

// SyncStats summarizes one sync run.
type SyncStats struct {
	FilesTotal    int   `json:"filesTotal"`
	FilesChanged  int   `json:"filesChanged"`
	FilesDeleted  int   `json:"filesDeleted"`
	BytesScanned  int64 `json:"bytesScanned"`
	BytesUploaded int64 `json:"bytesUploaded"`
	DurationMS    int64 `json:"durationMs"`
}

// Manifest is the complete listing of one generation. It is fully
// self-describing: restoring generation N requires only manifest N.
type Manifest struct {
	FormatVersion string      `json:"formatVersion"`
	Generation    uint64      `json:"generation"`
	Roots         []string    `json:"roots"`
	CreatedAt     string      `json:"createdAt"`
	Files         []FileEntry `json:"files"`
	Stats         SyncStats   `json:"stats"`
}

// Latest is the pointer object marking the current committed generation. It is
// overwritten only after a generation is fully durable.
type Latest struct {
	Generation     uint64 `json:"generation"`
	ManifestDigest string `json:"manifestDigest"`
	CompletedAt    string `json:"completedAt"`
}

// Validate checks manifest invariants.
func (m *Manifest) Validate() error {
	if m.FormatVersion != FormatVersionV1 {
		return fmt.Errorf("unsupported manifest format %q", m.FormatVersion)
	}
	seen := make(map[string]struct{}, len(m.Files))
	for i := range m.Files {
		f := &m.Files[i]
		if !strings.HasPrefix(f.Path, "/") {
			return fmt.Errorf("manifest path %q must be absolute", f.Path)
		}
		if _, dup := seen[f.Path]; dup {
			return fmt.Errorf("duplicate manifest path %q", f.Path)
		}
		seen[f.Path] = struct{}{}
		switch f.Type {
		case TypeRegular:
			if len(f.SHA256) != 64 {
				return fmt.Errorf("file %q missing content digest", f.Path)
			}
		case TypeSymlink:
			if len(f.SHA256) != 64 {
				return fmt.Errorf("symlink %q missing target digest", f.Path)
			}
		case TypeDir:
			// dirs carry no digest
		default:
			return fmt.Errorf("file %q has unknown type %q", f.Path, f.Type)
		}
	}
	return nil
}

// Sort orders files by path so manifests are byte-stable for identical trees.
func (m *Manifest) Sort() {
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
}

// Target identifies one sandbox's object-storage namespace.
type Target struct {
	Bucket string
	// Base is the object key prefix (without leading/trailing slash) under
	// which LATEST and gen/ live. All keys are derived from it; the client
	// cannot address keys outside it.
	Base string
}

// ParseTarget parses the canonical target URL
// s3://bucket/{prefix}/sandboxes/{namespace}/{sandboxId}. The prefix may be
// empty; trailing/leading slashes are normalized.
func ParseTarget(raw string) (Target, error) {
	rest, ok := strings.CutPrefix(raw, "s3://")
	if !ok || rest == "" {
		return Target{}, fmt.Errorf("target %q must look like s3://bucket/prefix/...", raw)
	}
	if strings.Contains(rest, "//") {
		return Target{}, fmt.Errorf("target %q contains empty path segments", raw)
	}
	bucket, path, _ := strings.Cut(rest, "/")
	if bucket == "" {
		return Target{}, fmt.Errorf("target %q has empty bucket", raw)
	}
	base := strings.Trim(path, "/")
	return Target{Bucket: bucket, Base: base}, nil
}

func (t Target) String() string {
	if t.Base == "" {
		return "s3://" + t.Bucket
	}
	return "s3://" + t.Bucket + "/" + t.Base
}

func (t Target) latestKey() string {
	return t.join(latestKeySuffix)
}

func (t Target) manifestKey(generation uint64) string {
	return t.join(fmt.Sprintf(manifestKeyFormat, generation))
}

func (t Target) genPrefix() string  { return t.join("gen/") }
func (t Target) blobPrefix() string { return t.join("blobs/") }

func (t Target) blobKey(sha256 string) string {
	return t.blobKeyFor(sha256, "")
}

// blobKeyFor builds the blob key for a given encoding. Raw blobs keep the
// legacy key shape ("blobs/xx/<sha>") so existing data stays readable;
// compressed blobs carry a suffix and can never be mistaken for raw ones.
func (t Target) blobKeyFor(sha256, encoding string) string {
	if encoding == "" {
		return t.join(fmt.Sprintf(blobKeyFormat, sha256[:2], sha256))
	}
	return t.join(fmt.Sprintf(blobKeyFormat, sha256[:2], sha256) + "." + encoding)
}

// join builds an object key under the target base. Building every key through
// this function is what makes "nothing outside the prefix" structural.
func (t Target) join(rel string) string {
	if t.Base == "" {
		return rel
	}
	return t.Base + "/" + rel
}
