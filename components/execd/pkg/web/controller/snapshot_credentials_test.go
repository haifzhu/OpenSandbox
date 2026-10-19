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
	"os"
	"path/filepath"
	"testing"
)

func writeCredsDir(t *testing.T, accessKey, secretKey string) string {
	t.Helper()
	dir := t.TempDir()
	for name, value := range map[string]string{
		"accessKeyId":     accessKey,
		"secretAccessKey": secretKey,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// TestSyncCredentialsRequestWins pins the precedence that makes sandbox-local
// syncs work: execd runs as the workload's uid and cannot read the root-only
// projection, so the values carried by the sync call must beat both the
// projected files and the process environment.
func TestSyncCredentialsRequestWins(t *testing.T) {
	t.Setenv("OBJSYNC_CREDS_DIR", writeCredsDir(t, "FILE_AK\n", "FILE_SK\n"))
	t.Setenv("OBJSYNC_ACCESS_KEY_ID", "ENV_AK")
	t.Setenv("OBJSYNC_SECRET_ACCESS_KEY", "ENV_SK")

	ak, sk, tok, err := syncCredentials(snapshotSyncRequest{
		AccessKeyID:     "REQ_AK",
		SecretAccessKey: "REQ_SK",
		SessionToken:    "REQ_TOK",
	})
	if err != nil {
		t.Fatal(err)
	}
	if ak != "REQ_AK" || sk != "REQ_SK" || tok != "REQ_TOK" {
		t.Fatalf("request credentials ignored: ak=%q sk=%q tok=%q", ak, sk, tok)
	}
}

// TestSyncCredentialsFallsBackToFilesThenEnv keeps the infra-container paths
// working: restore/purge jobs still use env, and a root-run execd still reads
// the projection.
func TestSyncCredentialsFallsBackToFilesThenEnv(t *testing.T) {
	t.Setenv("OBJSYNC_ACCESS_KEY_ID", "ENV_AK")
	t.Setenv("OBJSYNC_SECRET_ACCESS_KEY", "ENV_SK")

	t.Setenv("OBJSYNC_CREDS_DIR", writeCredsDir(t, "FILE_AK\n", "FILE_SK\n"))
	ak, sk, _, err := syncCredentials(snapshotSyncRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if ak != "FILE_AK" || sk != "FILE_SK" {
		t.Fatalf("projected files should win over env: ak=%q sk=%q", ak, sk)
	}

	// An empty projection directory makes the file lookup miss.
	t.Setenv("OBJSYNC_CREDS_DIR", t.TempDir())
	ak, sk, _, err = syncCredentials(snapshotSyncRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if ak != "ENV_AK" || sk != "ENV_SK" {
		t.Fatalf("env fallback not used: ak=%q sk=%q", ak, sk)
	}
}

// TestSyncCredentialsRejectsMissingAndIncomplete: an empty credential set must
// fail loudly. Falling back to anonymous requests turns a configuration bug
// into an object-store 403 that looks exactly like a permissions problem.
func TestSyncCredentialsRejectsMissingAndIncomplete(t *testing.T) {
	t.Setenv("OBJSYNC_CREDS_DIR", t.TempDir())
	t.Setenv("OBJSYNC_ACCESS_KEY_ID", "")
	t.Setenv("OBJSYNC_SECRET_ACCESS_KEY", "")
	t.Setenv("OBJSYNC_SESSION_TOKEN", "")

	if _, _, _, err := syncCredentials(snapshotSyncRequest{}); err == nil {
		t.Fatal("empty credentials must be rejected instead of going anonymous")
	}
	if _, _, _, err := syncCredentials(snapshotSyncRequest{AccessKeyID: "only-ak"}); err == nil {
		t.Fatal("a half-filled request credential pair must be rejected")
	}
}
