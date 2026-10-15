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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sandboxv1alpha1 "github.com/alibaba/OpenSandbox/sandbox-k8s/apis/sandbox/v1alpha1"
)

// TestWorkspaceVolumeNameContract pins the cross-repo volume-name algorithm.
// The create-time server injection (workspace_pause.py) must derive the same
// names; both sides reference these exact vectors.
func TestWorkspaceVolumeNameContract(t *testing.T) {
	vectors := map[string]string{
		"/workspace":    "ws-c52ddf6553",
		"/root/.lingma": "ws-27b691f987",
	}
	for root, want := range vectors {
		if got := workspaceVolumeName(root); got != want {
			t.Errorf("workspaceVolumeName(%q) = %q, want %q (server-side contract broken)", root, got, want)
		}
	}
}

func s3WorkspaceSnapshot() *sandboxv1alpha1.SandboxSnapshot {
	return &sandboxv1alpha1.SandboxSnapshot{
		Status: sandboxv1alpha1.SandboxSnapshotStatus{
			Format: sandboxv1alpha1.SandboxSnapshotFormatS3WorkspaceV1,
			Workspace: &sandboxv1alpha1.WorkspaceSnapshot{
				Target:                "s3://bucket/p/sandboxes/ns/sbx",
				Generation:            7,
				Roots:                 []string{"/root/.lingma", "/workspace"},
				Endpoint:              "http://minio.svc:9000",
				Region:                "us-east-1",
				ForcePathStyle:        true,
				CredentialsSecretName: "s3-creds",
			},
		},
	}
}

func baseTemplate() *corev1.PodTemplateSpec {
	return &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "sandbox", Image: "base:1"}},
		},
	}
}

func TestInjectWorkspaceRestore(t *testing.T) {
	template := baseTemplate()
	snapshot := s3WorkspaceSnapshot()

	if err := injectWorkspaceRestore(template, snapshot, "syncer:1"); err != nil {
		t.Fatalf("inject: %v", err)
	}

	// Init container present with the pinned generation and both roots.
	var init *corev1.Container
	for i := range template.Spec.InitContainers {
		if template.Spec.InitContainers[i].Name == workspaceRestoreInitName {
			init = &template.Spec.InitContainers[i]
		}
	}
	if init == nil {
		t.Fatalf("restore init container not injected")
	}
	joined := strings.Join(init.Args, " ")
	for _, want := range []string{"--generation 7", "--path-style", "/workspace,/root/.lingma", "s3://bucket/p/sandboxes/ns/sbx"} {
		if !strings.Contains(joined, want) {
			t.Errorf("init args missing %q: %v", want, init.Args)
		}
	}
	if init.Image != "syncer:1" {
		t.Errorf("init image = %q, want syncer:1", init.Image)
	}

	// Main container image is NOT replaced (the defining property of the
	// s3-workspace provider vs rootfs commit).
	if template.Spec.Containers[0].Image != "base:1" {
		t.Errorf("base image was replaced: %q", template.Spec.Containers[0].Image)
	}

	// Volumes match the contract names and the main container mounts them.
	main := template.Spec.Containers[0]
	mounted := map[string]string{}
	for _, m := range main.VolumeMounts {
		mounted[m.MountPath] = m.Name
	}
	for _, root := range snapshot.Status.Workspace.Roots {
		name, ok := mounted[root]
		if !ok {
			t.Fatalf("main container missing mount at %s", root)
		}
		if name != workspaceVolumeName(root) {
			t.Errorf("mount %s uses volume %q, contract wants %q", root, name, workspaceVolumeName(root))
		}
	}

	// Credentials projected from the recorded secret.
	found := map[string]bool{}
	for _, env := range init.Env {
		found[env.Name] = true
	}
	for _, name := range []string{"OBJSYNC_ACCESS_KEY_ID", "OBJSYNC_SECRET_ACCESS_KEY", "OBJSYNC_SESSION_TOKEN"} {
		if !found[name] {
			t.Errorf("init container missing env %s", name)
		}
	}

	// Idempotent: injecting twice does not duplicate anything.
	volumesBefore := len(template.Spec.Volumes)
	initsBefore := len(template.Spec.InitContainers)
	if err := injectWorkspaceRestore(template, snapshot, "syncer:1"); err != nil {
		t.Fatalf("second inject: %v", err)
	}
	if len(template.Spec.Volumes) != volumesBefore || len(template.Spec.InitContainers) != initsBefore {
		t.Errorf("second injection was not idempotent")
	}
}

func TestInjectWorkspaceRestoreSkipsOtherFormats(t *testing.T) {
	template := baseTemplate()
	snapshot := s3WorkspaceSnapshot()
	snapshot.Status.Format = sandboxv1alpha1.SandboxSnapshotFormatRootfsV1
	if err := injectWorkspaceRestore(template, snapshot, "syncer:1"); err != nil {
		t.Fatalf("rootfs snapshot must be a no-op, got %v", err)
	}
	if len(template.Spec.InitContainers) != 0 {
		t.Errorf("rootfs snapshot must not gain a restore init container")
	}
}

func TestInjectWorkspaceRestoreRequiresWorkspaceResult(t *testing.T) {
	template := baseTemplate()
	snapshot := s3WorkspaceSnapshot()
	snapshot.Status.Workspace = nil
	if err := injectWorkspaceRestore(template, snapshot, "syncer:1"); err == nil {
		t.Fatal("missing workspace result must fail closed")
	}
}

func podWithAnnotations(annotations map[string]string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: annotations}}
}
