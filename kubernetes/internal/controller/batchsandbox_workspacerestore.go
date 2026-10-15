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

// injectWorkspaceRestore adapts an s3-workspace-v1 resume: the main container
// keeps its original base image (no snapshot image swap) and each synced root
// gets a shared volume plus a workspace-restore init container that pulls the
// committed generation from object storage before the sandbox starts.
//
// The create-time template already carries the workspace volume layout (the
// server injects it when the sandbox opts into the s3-workspace provider), so
// volume/mount injection here is an idempotent safety net, and the operative
// step is appending the restore init container.
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

	for _, c := range template.Spec.InitContainers {
		if c.Name == workspaceRestoreInitName {
			return nil // idempotent
		}
	}

	main := &template.Spec.Containers[0]
	var mounts []corev1.VolumeMount
	for _, root := range roots {
		volumeName := workspaceVolumeName(root)
		ensureWorkspaceVolume(&template.Spec, volumeName)
		if !containerHasMount(main, root) {
			main.VolumeMounts = append(main.VolumeMounts, corev1.VolumeMount{Name: volumeName, MountPath: root})
		}
		mounts = append(mounts, corev1.VolumeMount{Name: volumeName, MountPath: root})
	}

	restore := corev1.Container{
		Name:            workspaceRestoreInitName,
		Image:           syncImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"/usr/local/bin/workspace-restore"},
		Args: []string{
			"--target", ws.Target,
			"--roots", strings.Join(roots, ","),
			"--generation", fmt.Sprintf("%d", ws.Generation),
			"--endpoint", ws.Endpoint,
			"--region", ws.Region,
		},
		VolumeMounts: mounts,
		Env:          workspaceCredsEnv(template.Annotations[snapshotcontract.AnnotationS3PauseCredsSecret]),
		SecurityContext: &corev1.SecurityContext{
			RunAsUser:                ptrToInt64(0),
			RunAsNonRoot:             ptrToBool(false),
			AllowPrivilegeEscalation: ptrToBool(false),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	}
	if ws.ForcePathStyle {
		restore.Args = append(restore.Args, "--path-style")
	}
	template.Spec.InitContainers = append(template.Spec.InitContainers, restore)
	return nil
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
