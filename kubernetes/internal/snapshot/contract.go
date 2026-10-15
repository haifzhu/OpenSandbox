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

package snapshot

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

const (
	AnnotationCheckpointProvider    = "sandbox.opensandbox.io/checkpoint-provider"
	AnnotationQEMUContainer         = "sandbox.opensandbox.io/qemu-container"
	annotationQEMUQMPSocket         = "sandbox.opensandbox.io/qemu-qmp-socket"
	annotationQEMULaunchManifest    = "sandbox.opensandbox.io/qemu-launch-manifest"
	annotationQEMURequiredNodeClass = "sandbox.opensandbox.io/qemu-required-node-class"
	LabelQEMUNodeClass              = "sandbox.opensandbox.io/qemu-node-class"

	// S3-workspace provider contract: pause persists the configured workspace
	// paths to S3-compatible object storage via execd instead of committing
	// the container rootfs to an OCI registry.
	AnnotationS3PauseTarget      = "sandbox.opensandbox.io/s3-pause-target"       // s3://bucket/prefix/...
	AnnotationS3PausePaths       = "sandbox.opensandbox.io/s3-pause-paths"        // JSON array of absolute paths
	AnnotationS3PauseExclude     = "sandbox.opensandbox.io/s3-pause-exclude"      // JSON array of doublestar patterns
	AnnotationS3PauseEndpoint    = "sandbox.opensandbox.io/s3-pause-endpoint"     // optional S3-compatible endpoint
	AnnotationS3PauseRegion      = "sandbox.opensandbox.io/s3-pause-region"       // optional region
	AnnotationS3PausePathStyle   = "sandbox.opensandbox.io/s3-pause-path-style"   // "true" forces path-style addressing
	AnnotationS3PauseCredsSecret = "sandbox.opensandbox.io/s3-pause-creds-secret" // Secret with accessKeyId/secretAccessKey/sessionToken
	AnnotationExecDPort          = "sandbox.opensandbox.io/execd-port"            // execd API port on the sandbox pod

	ProviderRootfs      = "rootfs"
	ProviderQEMU        = "qemu"
	ProviderS3Workspace = "s3-workspace"
)

// IsS3WorkspaceProvider reports whether a raw checkpoint-provider annotation
// value selects the s3-workspace provider. Normalization lives here so every
// caller (snapshot contract parsing, workspace cleanup, future webhooks)
// agrees on what counts; hand-written values like "S3-Workspace" must not
// split the provider decision from its cleanup obligations.
func IsS3WorkspaceProvider(value string) bool {
	return strings.ToLower(strings.TrimSpace(value)) == ProviderS3Workspace
}

// QEMUContract is the explicit workload contract between a QEMU-in-runc image
// and the OpenSandbox snapshot worker. OpenSandbox never detects QEMU by
// scanning process names.
type QEMUContract struct {
	ContainerName      string   `json:"containerName"`
	QMPSocketPath      string   `json:"qmpSocketPath"`
	LaunchManifestPath string   `json:"launchManifestPath"`
	RequiredNodeClass  string   `json:"requiredNodeClass,omitempty"`
	VolumeMountPaths   []string `json:"volumeMountPaths,omitempty"`
}

// S3WorkspaceContract selects the s3-workspace pause provider: the sync Job
// drives execd inside the sandbox to sync Roots (minus Exclude) into Target.
type S3WorkspaceContract struct {
	Target         string   `json:"target"`                   // s3://bucket/prefix/sandboxes/{ns}/{id}
	Roots          []string `json:"roots"`                    // absolute container paths
	Exclude        []string `json:"exclude,omitempty"`        // doublestar patterns
	Endpoint       string   `json:"endpoint,omitempty"`       // S3-compatible endpoint
	Region         string   `json:"region,omitempty"`         // region (default us-east-1)
	ForcePathStyle bool     `json:"forcePathStyle,omitempty"` // MinIO/Ceph path-style addressing
	ExecDPort      int      `json:"execdPort"`                // execd API port on the sandbox pod
}

// WorkloadContract describes the checkpoint provider selected by a Pod.
type WorkloadContract struct {
	Provider    string               `json:"provider"`
	QEMU        *QEMUContract        `json:"qemu,omitempty"`
	S3Workspace *S3WorkspaceContract `json:"s3Workspace,omitempty"`
}

// ContractFromPod returns the explicit checkpoint contract. Pods without a
// provider annotation retain the existing rootfs-only behavior.
func ContractFromPod(pod *corev1.Pod) (WorkloadContract, error) {
	if pod == nil {
		return WorkloadContract{}, fmt.Errorf("pod is required")
	}

	provider := strings.ToLower(strings.TrimSpace(pod.Annotations[AnnotationCheckpointProvider]))
	if provider == "" || provider == ProviderRootfs {
		return WorkloadContract{Provider: ProviderRootfs}, nil
	}
	if IsS3WorkspaceProvider(provider) {
		if _, qemu := pod.Annotations[AnnotationQEMUContainer]; qemu {
			return WorkloadContract{}, fmt.Errorf("s3-workspace checkpoints are incompatible with QEMU workloads (%s is set)", AnnotationQEMUContainer)
		}
		contract, err := s3WorkspaceContractFromPod(pod)
		if err != nil {
			return WorkloadContract{}, err
		}
		return WorkloadContract{Provider: ProviderS3Workspace, S3Workspace: contract}, nil
	}
	if provider != ProviderQEMU {
		return WorkloadContract{}, fmt.Errorf("unsupported checkpoint provider %q", provider)
	}

	contract := &QEMUContract{
		ContainerName:      strings.TrimSpace(pod.Annotations[AnnotationQEMUContainer]),
		QMPSocketPath:      strings.TrimSpace(pod.Annotations[annotationQEMUQMPSocket]),
		LaunchManifestPath: strings.TrimSpace(pod.Annotations[annotationQEMULaunchManifest]),
		RequiredNodeClass:  strings.TrimSpace(pod.Annotations[annotationQEMURequiredNodeClass]),
	}
	if contract.ContainerName == "" {
		return WorkloadContract{}, fmt.Errorf("%s is required for qemu checkpoints", AnnotationQEMUContainer)
	}
	if !podHasContainer(pod, contract.ContainerName) {
		return WorkloadContract{}, fmt.Errorf("qemu checkpoint container %q does not exist", contract.ContainerName)
	}
	contract.VolumeMountPaths = containerVolumePaths(pod, contract.ContainerName)
	if err := validateAbsoluteContainerPath(annotationQEMUQMPSocket, contract.QMPSocketPath); err != nil {
		return WorkloadContract{}, err
	}
	if err := validateAbsoluteContainerPath(annotationQEMULaunchManifest, contract.LaunchManifestPath); err != nil {
		return WorkloadContract{}, err
	}

	return WorkloadContract{Provider: ProviderQEMU, QEMU: contract}, nil
}

// s3WorkspaceContractFromPod parses and validates the s3-workspace provider
// annotations on the sandbox pod template.
func s3WorkspaceContractFromPod(pod *corev1.Pod) (*S3WorkspaceContract, error) {
	contract := &S3WorkspaceContract{
		Target:         strings.TrimSpace(pod.Annotations[AnnotationS3PauseTarget]),
		Endpoint:       strings.TrimSpace(pod.Annotations[AnnotationS3PauseEndpoint]),
		Region:         strings.TrimSpace(pod.Annotations[AnnotationS3PauseRegion]),
		ForcePathStyle: strings.TrimSpace(pod.Annotations[AnnotationS3PausePathStyle]) == "true",
	}

	if contract.Target == "" || !strings.HasPrefix(contract.Target, "s3://") {
		return nil, fmt.Errorf("%s must be an s3:// target for s3-workspace checkpoints", AnnotationS3PauseTarget)
	}
	if raw := strings.TrimSpace(pod.Annotations[AnnotationS3PausePaths]); raw != "" {
		if err := json.Unmarshal([]byte(raw), &contract.Roots); err != nil {
			return nil, fmt.Errorf("%s must be a JSON array of absolute paths: %w", AnnotationS3PausePaths, err)
		}
	}
	if len(contract.Roots) == 0 {
		return nil, fmt.Errorf("%s is required for s3-workspace checkpoints", AnnotationS3PausePaths)
	}
	for _, root := range contract.Roots {
		if !filepath.IsAbs(root) || filepath.Clean(root) != root {
			return nil, fmt.Errorf("s3-workspace root %q must be a clean absolute path", root)
		}
	}

	if raw := strings.TrimSpace(pod.Annotations[AnnotationS3PauseExclude]); raw != "" {
		if err := json.Unmarshal([]byte(raw), &contract.Exclude); err != nil {
			return nil, fmt.Errorf("%s must be a JSON array of patterns: %w", AnnotationS3PauseExclude, err)
		}
	}

	port := 0
	if raw := strings.TrimSpace(pod.Annotations[AnnotationExecDPort]); raw != "" {
		var err error
		if port, err = strconv.Atoi(raw); err != nil || port <= 0 || port > 65535 {
			return nil, fmt.Errorf("%s must be a valid port number", AnnotationExecDPort)
		}
	}
	if port == 0 {
		return nil, fmt.Errorf("%s is required for s3-workspace checkpoints", AnnotationExecDPort)
	}
	contract.ExecDPort = port

	return contract, nil
}

func containerVolumePaths(pod *corev1.Pod, containerName string) []string {
	for i := range pod.Spec.Containers {
		container := &pod.Spec.Containers[i]
		if container.Name != containerName {
			continue
		}
		paths := make([]string, 0, len(container.VolumeMounts)+len(container.VolumeDevices))
		for _, mount := range container.VolumeMounts {
			paths = append(paths, mount.MountPath)
		}
		for _, device := range container.VolumeDevices {
			paths = append(paths, device.DevicePath)
		}
		return paths
	}
	return nil
}

func podHasContainer(pod *corev1.Pod, name string) bool {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == name {
			return true
		}
	}
	return false
}

func validateAbsoluteContainerPath(annotation, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required for qemu checkpoints", annotation)
	}
	if !filepath.IsAbs(value) || filepath.Clean(value) != value {
		return fmt.Errorf("%s must be a clean absolute path", annotation)
	}
	return nil
}
