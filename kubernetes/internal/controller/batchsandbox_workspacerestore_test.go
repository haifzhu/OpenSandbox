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
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sandboxv1alpha1 "github.com/alibaba/OpenSandbox/sandbox-k8s/apis/sandbox/v1alpha1"
	snapshotcontract "github.com/alibaba/OpenSandbox/sandbox-k8s/internal/snapshot"
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
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				snapshotcontract.AnnotationS3PauseCredsSecret: "s3-creds",
			},
		},
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
	for _, want := range []string{"--generation 7", "--path-style", "--roots /root/.lingma,/workspace", "s3://bucket/p/sandboxes/ns/sbx"} {
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

	// Idempotent: injecting twice does not duplicate anything, and a snapshot
	// from a NEW cycle must REPLACE the pinned generation (the template is
	// persistent; a skip would replay stale generations forever).
	volumesBefore := len(template.Spec.Volumes)
	initsBefore := len(template.Spec.InitContainers)
	snapshot.Status.Workspace.Generation = 42
	if err := injectWorkspaceRestore(template, snapshot, "syncer:1"); err != nil {
		t.Fatalf("second inject: %v", err)
	}
	if len(template.Spec.Volumes) != volumesBefore || len(template.Spec.InitContainers) != initsBefore {
		t.Errorf("second injection duplicated containers or volumes")
	}
	for _, c := range template.Spec.InitContainers {
		if c.Name == workspaceRestoreInitName {
			joined := strings.Join(c.Args, " ")
			if !strings.Contains(joined, "--generation 42") {
				t.Errorf("existing restore init was not refreshed: %v", c.Args)
			}
		}
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

// TestInjectWorkspaceRestoreCredsFallback pins the fallback: a template
// without the creds annotation still gets credentials from the snapshot
// status (sandboxes created before the annotation existed).
func TestInjectWorkspaceRestoreCredsFallback(t *testing.T) {
	template := &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "sandbox", Image: "base:1"}},
		},
	}
	snapshot := s3WorkspaceSnapshot()
	if err := injectWorkspaceRestore(template, snapshot, "syncer:1"); err != nil {
		t.Fatalf("inject: %v", err)
	}
	var init *corev1.Container
	for i := range template.Spec.InitContainers {
		if template.Spec.InitContainers[i].Name == workspaceRestoreInitName {
			init = &template.Spec.InitContainers[i]
		}
	}
	if init == nil {
		t.Fatal("restore init container not injected")
	}
	found := map[string]bool{}
	for _, env := range init.Env {
		found[env.Name] = true
	}
	if !found["OBJSYNC_ACCESS_KEY_ID"] || !found["OBJSYNC_SECRET_ACCESS_KEY"] {
		t.Fatal("credentials fallback to snapshot status did not apply")
	}
}

// TestInjectWorkspaceRestoreLifecycleAcrossCycles pins the full-flow
// invariant the cluster bug exposed: the pod template PERSISTS across
// pause/resume cycles, so the restore init injected in cycle 1 (pinned to
// that cycle's generation) must be refreshed to the LATEST generation on
// every subsequent resume — otherwise every resume replays stale data
// forever while all pause-side checks stay green.
func TestInjectWorkspaceRestoreLifecycleAcrossCycles(t *testing.T) {
	template := baseTemplate()

	// Cycle 1: pause commits generation 5, resume injects restore pinned to 5.
	snapshot := s3WorkspaceSnapshot()
	snapshot.Status.Workspace.Generation = 5
	if err := injectWorkspaceRestore(template, snapshot, "syncer:1"); err != nil {
		t.Fatalf("cycle 1 inject: %v", err)
	}

	// Cycle 2 and 3: data changed, pauses committed generations 6 then 7.
	// The SAME template object (persisted on the BatchSandbox) is injected
	// again on each resume.
	for _, gen := range []uint64{6, 7} {
		snapshot.Status.Workspace.Generation = gen
		if err := injectWorkspaceRestore(template, snapshot, "syncer:1"); err != nil {
			t.Fatalf("cycle gen=%d inject: %v", gen, err)
		}
		init := findRestoreInit(t, template)
		joined := strings.Join(init.Args, " ")
		if !strings.Contains(joined, fmt.Sprintf("--generation %d", gen)) {
			t.Fatalf("resume must restore the LATEST generation %d, got args: %v", gen, init.Args)
		}
		if strings.Count(joined, "--generation") != 1 {
			t.Fatalf("restore init duplicated: %v", template.Spec.InitContainers)
		}
	}
}

func findRestoreInit(t *testing.T, template *corev1.PodTemplateSpec) *corev1.Container {
	t.Helper()
	for i := range template.Spec.InitContainers {
		if template.Spec.InitContainers[i].Name == workspaceRestoreInitName {
			return &template.Spec.InitContainers[i]
		}
	}
	t.Fatal("restore init container missing")
	return nil
}

// TestWorkspaceInfraContainersDeclareResources pins ResourceQuota
// compatibility: namespaces commonly require every container (including
// init containers and Job pods) to declare requests AND limits, so the
// restore init, sync trigger, and purge containers must never ship bare.
func TestWorkspaceInfraContainersDeclareResources(t *testing.T) {
	template := baseTemplate()
	snapshot := s3WorkspaceSnapshot()
	if err := injectWorkspaceRestore(template, snapshot, "syncer:1"); err != nil {
		t.Fatalf("inject: %v", err)
	}
	init := findRestoreInit(t, template)
	if init.Resources.Requests.Cpu().IsZero() || init.Resources.Limits.Memory().IsZero() {
		t.Fatalf("restore init container must declare requests and limits: %+v", init.Resources)
	}

	// Sync trigger container.
	r := newTestSnapshotReconciler()
	snap := s3WorkspaceSnapshot()
	job, err := r.buildSyncJob(snap, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "sbx-0"}}, &snapshotcontract.S3WorkspaceContract{
		Target: "s3://b/p", Roots: []string{"/workspace"}, ExecDPort: 44772,
	})
	if err != nil {
		t.Fatalf("buildSyncJob: %v", err)
	}
	sync := job.Spec.Template.Spec.Containers[0]
	if sync.Resources.Requests.Cpu().IsZero() || sync.Resources.Limits.Memory().IsZero() {
		t.Fatalf("sync trigger container must declare requests and limits: %+v", sync.Resources)
	}
}

// TestWorkspaceRestoreKeepsChownCapability pins ownership restoration. The
// manifest records each entry's uid/gid and workspace-restore chowns to them,
// but the container drops ALL capabilities: without CAP_CHOWN every chown
// returns EPERM, the best-effort restore swallows it, and restored directories
// stay root-owned — a non-root workload then cannot write its own workspace
// (observed: mkdir .task_roots/.pending-*: permission denied).
func TestWorkspaceRestoreKeepsChownCapability(t *testing.T) {
	template := baseTemplate()
	if err := injectWorkspaceRestore(template, s3WorkspaceSnapshot(), "syncer:1"); err != nil {
		t.Fatalf("inject: %v", err)
	}
	sc := findRestoreInit(t, template).SecurityContext
	if sc == nil || sc.Capabilities == nil {
		t.Fatal("restore init container must declare capabilities")
	}
	added := false
	for _, c := range sc.Capabilities.Add {
		if c == corev1.Capability("CHOWN") {
			added = true
		}
	}
	if !added {
		t.Fatalf("restore init container must add CHOWN, got add=%v", sc.Capabilities.Add)
	}
	for _, c := range sc.Capabilities.Drop {
		if c == corev1.Capability("CHOWN") {
			t.Fatal("CHOWN must not be both dropped and added")
		}
	}
}

// s3WorkspaceContractTemplate is the pod template the server stamps for a
// sandbox that opted into the s3-workspace provider — the create path's only
// input.
func s3WorkspaceContractTemplate() *corev1.PodTemplateSpec {
	template := baseTemplate()
	template.Annotations[snapshotcontract.AnnotationCheckpointProvider] = "s3-workspace"
	template.Annotations[snapshotcontract.AnnotationS3PauseTarget] = "s3://bucket/p/sandboxes/ns/sbx"
	template.Annotations[snapshotcontract.AnnotationS3PausePaths] = `["/workspace"]`
	template.Annotations[snapshotcontract.AnnotationS3PauseEndpoint] = "http://minio.svc:9000"
	template.Annotations[snapshotcontract.AnnotationS3PauseRegion] = "us-east-1"
	template.Annotations[snapshotcontract.AnnotationS3PausePathStyle] = "true"
	template.Annotations[snapshotcontract.AnnotationExecDPort] = "44772"
	return template
}

func mustContract(t *testing.T, template *corev1.PodTemplateSpec) *snapshotcontract.S3WorkspaceContract {
	t.Helper()
	contract, err := snapshotcontract.S3WorkspaceContractFromAnnotations(template.Annotations)
	if err != nil {
		t.Fatalf("parse contract: %v", err)
	}
	return contract
}

// TestInjectWorkspaceRestoreOnCreate pins the create-path behaviour: a sandbox
// being created with the s3-workspace provider gets the restore init container
// pinned to LATEST (generation 0), so a rebuilt sandbox finds its predecessor's
// workspace without anyone having to decide in advance that it should.
func TestInjectWorkspaceRestoreOnCreate(t *testing.T) {
	template := s3WorkspaceContractTemplate()

	if err := injectWorkspaceRestoreOnCreate(template, mustContract(t, template), "syncer:1"); err != nil {
		t.Fatalf("inject: %v", err)
	}

	init := findRestoreInit(t, template)
	if init == nil {
		t.Fatal("restore init container not injected")
	}
	joined := strings.Join(init.Args, " ")
	for _, want := range []string{
		"--generation 0", // LATEST, not a pinned generation: a create has no snapshot
		"--path-style",
		"--roots /workspace",
		"--endpoint http://minio.svc:9000",
		"--region us-east-1",
		"s3://bucket/p/sandboxes/ns/sbx",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("init args missing %q: %v", want, init.Args)
		}
	}
	if init.Image != "syncer:1" {
		t.Errorf("init image = %q, want syncer:1", init.Image)
	}
	// Same defining property as the resume path: the base image is untouched.
	if template.Spec.Containers[0].Image != "base:1" {
		t.Errorf("base image was replaced: %q", template.Spec.Containers[0].Image)
	}
	// Credentials come from the create-time annotation, which is all a create has.
	if len(init.Env) == 0 {
		t.Error("restore init container has no credential env; the create path must project them from the annotation")
	}
	// The sync root got a volume and the main container mounts it.
	if len(template.Spec.Volumes) != 1 || template.Spec.Volumes[0].Name != workspaceVolumeName("/workspace") {
		t.Errorf("volumes = %+v, want the /workspace volume", template.Spec.Volumes)
	}
	if !containerHasMount(&template.Spec.Containers[0], "/workspace") {
		t.Error("main container does not mount /workspace; restored data would be invisible to it")
	}
}

// TestInjectWorkspaceRestoreOnCreateLeavesAResumePinnedContainerAlone: resume
// replaces the init container in place on purpose (it must re-pin to its own
// generation). The create path must not clobber that, or a resumed sandbox
// would silently restore a different generation than the one it was resumed
// against.
func TestInjectWorkspaceRestoreOnCreateLeavesAResumePinnedContainerAlone(t *testing.T) {
	template := s3WorkspaceContractTemplate()
	if err := injectWorkspaceRestore(template, s3WorkspaceSnapshot(), "syncer:1"); err != nil {
		t.Fatalf("resume inject: %v", err)
	}

	if err := injectWorkspaceRestoreOnCreate(template, mustContract(t, template), "syncer:1"); err != nil {
		t.Fatalf("create inject: %v", err)
	}

	init := findRestoreInit(t, template)
	if init == nil {
		t.Fatal("restore init container vanished")
	}
	if !strings.Contains(strings.Join(init.Args, " "), "--generation 7") {
		t.Errorf("create-path injection clobbered the resume-pinned generation: %v", init.Args)
	}
}

// TestInjectWorkspaceRestoreOnCreateRejectsARootlessContract: a contract that
// cannot restore must fail the reconcile rather than create a pod whose first
// pause would overwrite the target's committed generation with an empty one.
func TestInjectWorkspaceRestoreOnCreateRejectsARootlessContract(t *testing.T) {
	template := s3WorkspaceContractTemplate()
	if err := injectWorkspaceRestoreOnCreate(template, &snapshotcontract.S3WorkspaceContract{
		Target: "s3://bucket/p/sandboxes/ns/sbx",
	}, "syncer:1"); err == nil {
		t.Fatal("a contract with no roots should be rejected")
	}
}

// TestInjectWorkspaceRestoreOnCreateIgnoresANilContract: a sandbox that never
// opted in carries no contract, and must be left completely alone.
func TestInjectWorkspaceRestoreOnCreateIgnoresANilContract(t *testing.T) {
	template := baseTemplate()
	if err := injectWorkspaceRestoreOnCreate(template, nil, "syncer:1"); err != nil {
		t.Fatalf("inject with no contract = %v, want nil", err)
	}
	if len(template.Spec.InitContainers) != 0 {
		t.Errorf("init containers = %+v, want none", template.Spec.InitContainers)
	}
}
