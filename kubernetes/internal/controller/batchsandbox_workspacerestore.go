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
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"

	sandboxv1alpha1 "github.com/alibaba/OpenSandbox/sandbox-k8s/apis/sandbox/v1alpha1"
	snapshotcontract "github.com/alibaba/OpenSandbox/sandbox-k8s/internal/snapshot"
)

const (
	workspaceRestoreInitName = "opensandbox-workspace-restore"
	workspaceVolumePrefix    = "ws-"

	// Secret keys the control plane projects into restore init containers.
	workspaceCredsKeyAccess     = "accessKeyId"
	workspaceCredsKeySecret     = "secretAccessKey"
	workspaceCredsKeySessionTok = "sessionToken"

	envAccessKeyID     = "OBJSYNC_ACCESS_KEY_ID"
	envSecretAccessKey = "OBJSYNC_SECRET_ACCESS_KEY"
	envSessionToken    = "OBJSYNC_SESSION_TOKEN"
)

// workspaceRestorePlan is one resolved restore init container's inputs. It is
// deliberately independent of where the values came from: a resume reads them
// from the pause snapshot, a create reads them from the pod template's
// s3-workspace contract. Both must produce an identical container, so the
// builder lives here once.
type workspaceRestorePlan struct {
	Target         string
	Roots          []string
	Generation     uint64 // 0 = LATEST
	Endpoint       string
	Region         string
	ForcePathStyle bool
	CredsSecret    string
}

func (p workspaceRestorePlan) container(syncImage string, mounts []corev1.VolumeMount) corev1.Container {
	c := corev1.Container{
		Name:            workspaceRestoreInitName,
		Image:           syncImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"/usr/local/bin/workspace-restore"},
		Args: []string{
			"--target", p.Target,
			"--roots", strings.Join(p.Roots, ","),
			"--generation", fmt.Sprintf("%d", p.Generation),
			"--endpoint", p.Endpoint,
			"--region", p.Region,
		},
		VolumeMounts: mounts,
		Env:          workspaceCredsEnv(p.CredsSecret),
		Resources:    workspaceInfraResources,
		SecurityContext: &corev1.SecurityContext{
			RunAsUser:                ptrToInt64(0),
			RunAsNonRoot:             ptrToBool(false),
			AllowPrivilegeEscalation: ptrToBool(false),
			// CAP_CHOWN is required: the manifest carries each entry's original
			// uid/gid, and restoring them is what keeps the workspace writable
			// for the sandbox workload. This container runs as root but with
			// every capability dropped, so without it chown fails with EPERM,
			// the best-effort restore ignores that, and every restored
			// directory stays root-owned — a non-root workload then cannot
			// create files in its own workspace.
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"},
				Add:  []corev1.Capability{"CHOWN"},
			},
		},
	}
	if p.ForcePathStyle {
		c.Args = append(c.Args, "--path-style")
	}
	return c
}

// ensureWorkspaceRoots gives every sync root a shared volume and a mount in the
// main container, and returns the init container's mounts.
//
// The create-time template already carries this layout (the server injects it
// when the sandbox opts into the s3-workspace provider), so this is an
// idempotent safety net; the operative step is installing the init container.
func ensureWorkspaceRoots(template *corev1.PodTemplateSpec, roots []string) []corev1.VolumeMount {
	main := &template.Spec.Containers[0]
	mounts := make([]corev1.VolumeMount, 0, len(roots))
	for _, root := range roots {
		volumeName := workspaceVolumeName(root)
		ensureWorkspaceVolume(&template.Spec, volumeName)
		if !containerHasMount(main, root) {
			main.VolumeMounts = append(main.VolumeMounts, corev1.VolumeMount{Name: volumeName, MountPath: root})
		}
		mounts = append(mounts, corev1.VolumeMount{Name: volumeName, MountPath: root})
	}
	return mounts
}

// placeWorkspaceRestoreInit installs the restore init container, replacing an
// existing one in place.
//
// The pod template is persistent across pause/resume cycles, so a restore init
// injected by a PREVIOUS cycle may already be there — pinned to that cycle's
// generation. Idempotency here means REPLACE-in-place with the current values;
// skipping would replay stale generations forever (observed: every resume
// restored gen 1).
func placeWorkspaceRestoreInit(template *corev1.PodTemplateSpec, restore corev1.Container, mounts []corev1.VolumeMount) {
	for i := range template.Spec.InitContainers {
		if template.Spec.InitContainers[i].Name == workspaceRestoreInitName {
			restore.VolumeMounts = mergeRestoreMounts(template.Spec.InitContainers[i].VolumeMounts, mounts)
			template.Spec.InitContainers[i] = restore
			return
		}
	}
	template.Spec.InitContainers = append(template.Spec.InitContainers, restore)
}

// injectWorkspaceRestore adapts an s3-workspace-v1 resume: the main container
// keeps its original base image (no snapshot image swap) and each synced root
// gets a shared volume plus a workspace-restore init container that pulls the
// committed generation from object storage before the sandbox starts.
func injectWorkspaceRestore(template *corev1.PodTemplateSpec, snapshot *sandboxv1alpha1.SandboxSnapshot, syncImage string) error {
	if snapshot.Status.Format != sandboxv1alpha1.SandboxSnapshotFormatS3WorkspaceV1 {
		return nil
	}
	ws := snapshot.Status.Workspace
	if ws == nil {
		return fmt.Errorf("s3-workspace-v1 snapshot has no workspace result")
	}
	if len(ws.Roots) == 0 {
		return fmt.Errorf("s3-workspace-v1 snapshot has no roots")
	}
	if len(template.Spec.Containers) == 0 {
		return fmt.Errorf("workspace restore requires a container in the pod template")
	}

	roots := append([]string(nil), ws.Roots...)
	sort.Strings(roots)
	mounts := ensureWorkspaceRoots(template, roots)

	plan := workspaceRestorePlan{
		Target:         ws.Target,
		Roots:          roots,
		Generation:     ws.Generation,
		Endpoint:       ws.Endpoint,
		Region:         ws.Region,
		ForcePathStyle: ws.ForcePathStyle,
		// Credentials prefer the create-time template annotation; the snapshot
		// status value is the fallback for sandboxes whose template predates
		// the annotation.
		CredsSecret: workspaceCredsSecretName(template, ws),
	}
	placeWorkspaceRestoreInit(template, plan.container(syncImage, mounts), mounts)
	return nil
}

// injectWorkspaceRestoreOnCreate installs the restore init container on a
// freshly created s3-workspace sandbox, from the contract the server stamped
// onto the pod template.
//
// It runs UNCONDITIONALLY: it does not ask whether the target holds anything.
// That question is a property of the object store — answerable only there, and
// only at restore time. A control-plane probe would be a cached guess with a
// TOCTOU window: probe says "empty", a concurrent pause commits a generation,
// the restore is skipped, and the sandbox's own next pause then overwrites that
// generation with an empty one. Restoring against LATEST instead means a
// rebuilt sandbox picks up its predecessor's workspace with nobody having to
// decide in advance, and a first-incarnation sandbox pays one object read
// (the restore no-ops when LATEST is absent).
//
// Generation 0 = LATEST: a create has no pause snapshot to pin a generation
// from, and LATEST is exactly what a rebuilt sandbox should converge on.
// Unlike the resume path, this one only installs when the container is ABSENT.
// Resume REPLACES in place on purpose (it must re-pin to its own generation),
// and clobbering that here would silently change what a resumed sandbox
// restores. A create is the only case where the container cannot already be
// there for a reason, so a present one means "somebody already decided".
func injectWorkspaceRestoreOnCreate(template *corev1.PodTemplateSpec, contract *snapshotcontract.S3WorkspaceContract, syncImage string) error {
	if contract == nil {
		return nil
	}
	for i := range template.Spec.InitContainers {
		if template.Spec.InitContainers[i].Name == workspaceRestoreInitName {
			return nil
		}
	}
	if len(contract.Roots) == 0 {
		return fmt.Errorf("s3-workspace contract has no roots")
	}
	if len(template.Spec.Containers) == 0 {
		return fmt.Errorf("workspace restore requires a container in the pod template")
	}

	roots := append([]string(nil), contract.Roots...)
	sort.Strings(roots)
	mounts := ensureWorkspaceRoots(template, roots)

	plan := workspaceRestorePlan{
		Target:         contract.Target,
		Roots:          roots,
		Generation:     0, // LATEST
		Endpoint:       contract.Endpoint,
		Region:         contract.Region,
		ForcePathStyle: contract.ForcePathStyle,
		CredsSecret:    strings.TrimSpace(template.Annotations[snapshotcontract.AnnotationS3PauseCredsSecret]),
	}
	placeWorkspaceRestoreInit(template, plan.container(syncImage, mounts), mounts)
	return nil
}

// mergeRestoreMounts keeps any extra mounts the previous injection carried
// (future-proof) while ensuring every sync root is mounted.
func mergeRestoreMounts(existing, wanted []corev1.VolumeMount) []corev1.VolumeMount {
	merged := append([]corev1.VolumeMount(nil), existing...)
	for _, m := range wanted {
		found := false
		for _, x := range merged {
			if x.MountPath == m.MountPath {
				found = true
				break
			}
		}
		if !found {
			merged = append(merged, m)
		}
	}
	return merged
}

// workspaceVolumeName derives the workspace volume name from a mount path.
//
// CONTRACT (cross-repo): the create-time server injection (server/
// opensandbox_server/services/k8s/workspace_pause.py::_workspace_volume_name)
// derives the identical name with "ws-" + hex(sha256(root))[:10]. The two
// sides must stay byte-identical or restored data lands in a volume the main
// container cannot see; both sides pin the algorithm with a shared test
// vector ("/workspace" -> ws-c52ddf6553).
func workspaceVolumeName(root string) string {
	sum := sha256.Sum256([]byte(root))
	return workspaceVolumePrefix + fmt.Sprintf("%x", sum)[:10]
}

func ensureWorkspaceVolume(spec *corev1.PodSpec, name string) {
	for i := range spec.Volumes {
		if spec.Volumes[i].Name == name {
			return
		}
	}
	spec.Volumes = append(spec.Volumes, corev1.Volume{
		Name:         name,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	})
}

func containerHasMount(container *corev1.Container, path string) bool {
	for _, m := range container.VolumeMounts {
		if m.MountPath == path {
			return true
		}
	}
	return false
}

// workspaceCredsSecretName resolves the credentials Secret for a resume:
// template annotation first, snapshot status as fallback.
func workspaceCredsSecretName(template *corev1.PodTemplateSpec, ws *sandboxv1alpha1.WorkspaceSnapshot) string {
	if name := template.Annotations[snapshotcontract.AnnotationS3PauseCredsSecret]; name != "" {
		return name
	}
	return ws.CredentialsSecretName
}

// workspaceCredsEnv builds the ordered OBJSYNC_* env projection for a
// credentials Secret. Shared by the restore init container, the pause sync
// Job, and the deletion purge Job — keep the single copy authoritative.
func workspaceCredsEnv(secretName string) []corev1.EnvVar {
	if secretName == "" {
		return nil
	}
	return []corev1.EnvVar{
		{
			Name: envAccessKeyID,
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				Key:                  workspaceCredsKeyAccess,
			}},
		},
		{
			Name: envSecretAccessKey,
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				Key:                  workspaceCredsKeySecret,
			}},
		},
		{
			Name: envSessionToken,
			ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				Key:                  workspaceCredsKeySessionTok,
				Optional:             ptrToBool(true),
			}},
		},
	}
}

// ensureWorkspaceRestoreForCreate installs the workspace-restore init container
// on the pod template of an s3-workspace sandbox that is about to have pods
// created for it.
//
// The provider check is the same one the pause path uses, so a sandbox that
// never opted into s3-workspace is untouched, and a malformed contract fails
// the reconcile instead of creating a pod whose first pause would overwrite the
// target's committed generation with an empty one.
func (r *BatchSandboxReconciler) ensureWorkspaceRestoreForCreate(batchSbx *sandboxv1alpha1.BatchSandbox) error {
	if batchSbx == nil || batchSbx.Spec.Template == nil {
		return nil
	}
	annotations := batchSbx.Spec.Template.Annotations
	provider := strings.ToLower(strings.TrimSpace(annotations[snapshotcontract.AnnotationCheckpointProvider]))
	if !snapshotcontract.IsS3WorkspaceProvider(provider) {
		return nil
	}
	contract, err := snapshotcontract.S3WorkspaceContractFromAnnotations(annotations)
	if err != nil {
		return fmt.Errorf("invalid s3-workspace contract on the pod template: %w", err)
	}
	return injectWorkspaceRestoreOnCreate(batchSbx.Spec.Template, contract, r.WorkspaceSyncImage)
}
