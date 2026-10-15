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
	"sort"
	"strings"
	"sync"
)

// errInjected is the failure MemStore raises once its write budget is spent.
var errInjected = errors.New("memstore: injected failure")

// MemStore is an in-memory ObjectStore for unit tests. Buckets are created
// implicitly. FailAfter, when > 0, makes writes fail once the quota is hit so
// tests can inject partial-upload failures.
type MemStore struct {
	mu        sync.Mutex
	objects   map[string][]byte // "bucket/key" -> content
	failAfter int
	writes    int
}

// NewMemStore returns an empty MemStore. failAfter <= 0 means unlimited.
func NewMemStore(failAfter int) *MemStore {
	return &MemStore{objects: map[string][]byte{}, failAfter: failAfter}
}

func (m *MemStore) Get(_ context.Context, bucket, key string, w io.Writer) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.objects[bucket+"/"+key]
	if !ok {
		return 0, ErrNotFound
	}
	if _, err := w.Write(v); err != nil {
		return 0, err
	}
	return int64(len(v)), nil
}

func (m *MemStore) Put(_ context.Context, bucket, key string, r io.Reader, size int64, _ string) error {
	if err := m.budget(); err != nil {
		return err
	}
	data, err := io.ReadAll(io.LimitReader(r, size))
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[bucket+"/"+key] = data
	return nil
}

func (m *MemStore) PutMultipart(ctx context.Context, bucket, key string, r io.Reader, size, _ int64) error {
	return m.Put(ctx, bucket, key, r, size, "")
}

func (m *MemStore) Head(_ context.Context, bucket, key string) (int64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.objects[bucket+"/"+key]
	return int64(len(v)), ok, nil
}

func (m *MemStore) Delete(_ context.Context, bucket, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, bucket+"/"+key)
	return nil
}

func (m *MemStore) List(_ context.Context, bucket, prefix, marker string, max int) ([]string, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for k := range m.objects {
		b, rest, found := strings.Cut(k, "/")
		if !found || b != bucket || len(rest) == 0 {
			continue
		}
		if strings.HasPrefix(rest, prefix) && rest > marker {
			keys = append(keys, rest)
		}
	}
	sort.Strings(keys)
	if max > 0 && len(keys) > max {
		return keys[:max], keys[max-1], nil
	}
	return keys, "", nil
}

func (m *MemStore) budget() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes++
	if m.failAfter > 0 && m.writes > m.failAfter {
		return errInjected
	}
	return nil
}

// Keys returns a sorted snapshot of all keys in a bucket (test helper).
func (m *MemStore) Keys(bucket string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for k := range m.objects {
		if b, rest, found := strings.Cut(k, "/"); found && b == bucket && len(rest) > 0 {
			keys = append(keys, rest)
		}
	}
	sort.Strings(keys)
	return keys
}
