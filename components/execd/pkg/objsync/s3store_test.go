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
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeS3 is a minimal path-style S3 backend used to exercise S3Store over
// real HTTP: object CRUD, listing XML, and multipart reassembly. Signature
// correctness itself is pinned separately by the AWS documentation vector.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte         // "bucket/key"
	mpus    map[string]map[int][]byte // uploadID -> partNumber -> data
	nextID  int
}

func newFakeS3() *fakeS3 {
	return &fakeS3{objects: map[string][]byte{}, mpus: map[string]map[int][]byte{}, nextID: 100}
}

func (f *fakeS3) handler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			t.Errorf("request missing Authorization header")
		}
		path := strings.TrimPrefix(r.URL.Path, "/")
		bucket, key, _ := strings.Cut(path, "/")
		if bucket == "" {
			http.Error(w, "no bucket", http.StatusBadRequest)
			return
		}
		query := r.URL.Query()

		switch {
		case r.Method == http.MethodGet && key == "" && query.Get("list-type") == "2":
			f.list(w, bucket, query)
		case r.Method == http.MethodGet:
			f.get(w, r, bucket, key)
		case r.Method == http.MethodHead:
			f.head(w, bucket, key)
		case r.Method == http.MethodPut && query.Get("uploadId") != "":
			f.uploadPart(w, r, bucket, key, query)
		case r.Method == http.MethodPut:
			f.put(w, r, bucket, key)
		case r.Method == http.MethodPost && query.Has("uploads"):
			f.createMPU(w, bucket, key)
		case r.Method == http.MethodPost && query.Get("uploadId") != "":
			f.completeMPU(w, r, bucket, key, query)
		case r.Method == http.MethodDelete && query.Get("uploadId") != "":
			f.mu.Lock()
			delete(f.mpus, query.Get("uploadId"))
			f.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodDelete:
			f.mu.Lock()
			delete(f.objects, bucket+"/"+key)
			f.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unsupported", http.StatusNotImplemented)
		}
	})
}

func (f *fakeS3) put(w http.ResponseWriter, r *http.Request, bucket, key string) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.objects[bucket+"/"+key] = body
	f.mu.Unlock()
	w.Header().Set("ETag", `"etag"`)
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) get(w http.ResponseWriter, r *http.Request, bucket, key string) {
	f.mu.Lock()
	data, ok := f.objects[bucket+"/"+key]
	f.mu.Unlock()
	if !ok {
		writeS3Error(w, http.StatusNotFound, "NoSuchKey", "key not found")
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (f *fakeS3) head(w http.ResponseWriter, bucket, key string) {
	f.mu.Lock()
	data, ok := f.objects[bucket+"/"+key]
	f.mu.Unlock()
	if !ok {
		writeS3Error(w, http.StatusNotFound, "NoSuchKey", "key not found")
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) list(w http.ResponseWriter, bucket string, query url.Values) {
	prefix := query.Get("prefix")
	f.mu.Lock()
	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		b, rest, _ := strings.Cut(k, "/")
		if b == bucket && strings.HasPrefix(rest, prefix) {
			keys = append(keys, rest)
		}
	}
	f.mu.Unlock()
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>`)
	for _, k := range keys {
		b.WriteString("<Contents><Key>" + k + "</Key></Contents>")
	}
	b.WriteString("</ListBucketResult>")
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(b.String()))
}

func (f *fakeS3) createMPU(w http.ResponseWriter, bucket, key string) {
	f.mu.Lock()
	f.nextID++
	id := "mpu-" + strconv.Itoa(f.nextID)
	f.mpus[id] = map[int][]byte{}
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>` + id + `</UploadId></InitiateMultipartUploadResult>`))
}

func (f *fakeS3) uploadPart(w http.ResponseWriter, r *http.Request, bucket, key string, query url.Values) {
	partNumber, _ := strconv.Atoi(query.Get("partNumber"))
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.mpus[query.Get("uploadId")][partNumber] = body
	f.mu.Unlock()
	w.Header().Set("ETag", `"part-etag-`+strconv.Itoa(partNumber)+`"`)
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) completeMPU(w http.ResponseWriter, r *http.Request, bucket, key string, query url.Values) {
	var req struct {
		Parts []struct {
			PartNumber int    `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		} `xml:"Part"`
	}
	body, _ := io.ReadAll(r.Body)
	if err := xml.Unmarshal(body, &req); err != nil {
		writeS3Error(w, http.StatusBadRequest, "MalformedXML", err.Error())
		return
	}
	f.mu.Lock()
	parts := f.mpus[query.Get("uploadId")]
	var assembled []byte
	for _, p := range req.Parts {
		assembled = append(assembled, parts[p.PartNumber]...)
	}
	f.objects[bucket+"/"+key] = assembled
	delete(f.mpus, query.Get("uploadId"))
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(`<CompleteMultipartUploadResult></CompleteMultipartUploadResult>`))
}

func writeS3Error(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`<Error><Code>` + code + `</Code><Message>` + message + `</Message></Error>`))
}

func TestS3StoreHTTPEndToEnd(t *testing.T) {
	fake := newFakeS3()
	server := httptest.NewServer(fake.handler(t))
	defer server.Close()

	store := NewS3Store(S3Config{
		Endpoint:       server.URL,
		Region:         "us-east-1",
		AccessKey:      "testak",
		SecretKey:      "testsk",
		ForcePathStyle: true,
	})
	ctx := context.Background()

	// PUT + GET roundtrip with known digest signing.
	payload := []byte("hello s3")
	if err := store.Put(ctx, "b", "k1", bytes.NewReader(payload), int64(len(payload)), sha256Hex(payload)); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if n, err := store.Get(ctx, "b", "k1", &out); err != nil || n != int64(len(payload)) || !bytes.Equal(out.Bytes(), payload) {
		t.Fatalf("get roundtrip: n=%d err=%v equal=%v", n, err, bytes.Equal(out.Bytes(), payload))
	}

	// HEAD + missing HEAD.
	if size, exists, err := store.Head(ctx, "b", "k1"); err != nil || !exists || size != int64(len(payload)) {
		t.Fatalf("head: size=%d exists=%v err=%v", size, exists, err)
	}
	if _, exists, err := store.Head(ctx, "b", "missing"); err != nil || exists {
		t.Fatalf("head missing: exists=%v err=%v", exists, err)
	}

	// GET missing → ErrNotFound.
	if _, err := store.Get(ctx, "b", "missing", &out); err != ErrNotFound {
		t.Fatalf("get missing = %v, want ErrNotFound", err)
	}

	// Multipart reassembly (two parts).
	big := bytes.Repeat([]byte("x"), 5<<20+3) // > 5MiB minimum part size
	if err := store.PutMultipart(ctx, "b", "big", bytes.NewReader(big), int64(len(big)), 5<<20); err != nil {
		t.Fatal(err)
	}
	var bigOut bytes.Buffer
	if _, err := store.Get(ctx, "b", "big", &bigOut); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bigOut.Bytes(), big) {
		t.Fatalf("multipart roundtrip mismatch: got %d bytes want %d", bigOut.Len(), len(big))
	}

	// Listing.
	keys, next, err := store.List(ctx, "b", "", "", 100)
	if err != nil || next != "" || len(keys) != 2 {
		t.Fatalf("list: keys=%v next=%q err=%v", keys, next, err)
	}

	// DELETE.
	if err := store.Delete(ctx, "b", "k1"); err != nil {
		t.Fatal(err)
	}
	if _, exists, _ := store.Head(ctx, "b", "k1"); exists {
		t.Fatal("delete did not remove the object")
	}
}

// TestSyncOverHTTP runs the full sync→restore cycle through S3Store against
// the fake S3 backend, proving the store satisfies the engine end to end.
func TestSyncOverHTTP(t *testing.T) {
	fake := newFakeS3()
	server := httptest.NewServer(fake.handler(t))
	defer server.Close()

	store := NewS3Store(S3Config{
		Endpoint:       server.URL,
		Region:         "us-east-1",
		AccessKey:      "testak",
		SecretKey:      "testsk",
		ForcePathStyle: true,
	})
	ctx := context.Background()
	target := Target{Bucket: "b", Base: "p/1"}
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "a.txt"), "aaa", 0o644)
	writeFile(t, filepath.Join(root, "big.bin"), strings.Repeat("y", 9<<20), 0o644)

	syncer := &Syncer{Store: store}
	if _, err := syncer.Sync(ctx, SyncOptions{Roots: []string{root}, Target: target}); err != nil {
		t.Fatal(err)
	}

	want := snapshotTree(t, root)
	wipeTree(t, root)
	if _, err := Restore(ctx, store, RestoreOptions{Target: target, Roots: []string{root}}); err != nil {
		t.Fatal(err)
	}
	compareTrees(t, want, snapshotTree(t, root))
}
