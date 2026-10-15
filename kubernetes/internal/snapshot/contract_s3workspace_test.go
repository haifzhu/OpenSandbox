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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestContractFromPodS3Workspace(t *testing.T) {
	pod := &corev1.Pod{}
	pod.Annotations = map[string]string{
		AnnotationCheckpointProvider: "s3-workspace",
		AnnotationS3PauseTarget:      "s3://bucket/lingma/sandboxes/ns/sbx",
		AnnotationS3PausePaths:       `["/workspace","/root/.lingma"]`,
		AnnotationS3PauseExclude:     `["node_modules","**/*.log"]`,
		AnnotationS3PauseEndpoint:    "http://minio.svc:9000",
		AnnotationS3PauseRegion:      "us-east-1",
		AnnotationS3PausePathStyle:   "true",
		AnnotationExecDPort:          "44772",
	}

	contract, err := ContractFromPod(pod)
	if err != nil {
		t.Fatalf("ContractFromPod: %v", err)
	}
	if contract.Provider != ProviderS3Workspace || contract.S3Workspace == nil {
		t.Fatalf("provider = %q, want %q with contract", contract.Provider, ProviderS3Workspace)
	}
	s3 := contract.S3Workspace
	if s3.Target != "s3://bucket/lingma/sandboxes/ns/sbx" || !s3.ForcePathStyle || s3.ExecDPort != 44772 {
		t.Errorf("contract mismatch: %+v", s3)
	}
	if len(s3.Roots) != 2 {
		t.Errorf("roots = %v, want 2 entries", s3.Roots)
	}
	if len(s3.Exclude) != 2 {
		t.Errorf("exclude = %v, want 2 entries", s3.Exclude)
	}
}

func TestContractFromPodS3WorkspaceValidation(t *testing.T) {
	cases := []struct {
		name        string
		overrides   map[string]string
		wantErrPart string
	}{
		{"missing target", map[string]string{AnnotationS3PauseTarget: ""}, "s3-pause-target"},
		{"non-s3 target", map[string]string{AnnotationS3PauseTarget: "gs://bucket/x"}, "s3://"},
		{"missing paths", map[string]string{AnnotationS3PausePaths: ""}, "s3-pause-paths"},
		{"bad paths json", map[string]string{AnnotationS3PausePaths: "{oops"}, "JSON array"},
		{"relative path", map[string]string{AnnotationS3PausePaths: `["workspace"]`}, "clean absolute"},
		{"missing execd port", map[string]string{AnnotationExecDPort: ""}, "execd-port"},
		{"bad execd port", map[string]string{AnnotationExecDPort: "not-a-port"}, "port"},
	}
	for _, tc := range cases {
		pod := &corev1.Pod{}
		pod.Annotations = map[string]string{
			AnnotationCheckpointProvider: "s3-workspace",
			AnnotationS3PauseTarget:      "s3://bucket/p",
			AnnotationS3PausePaths:       `["/workspace"]`,
			AnnotationExecDPort:          "44772",
		}
		for k, v := range tc.overrides {
			if v == "" {
				delete(pod.Annotations, k)
			} else {
				pod.Annotations[k] = v
			}
		}
		_, err := ContractFromPod(pod)
		if err == nil {
			t.Errorf("%s: expected error", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.wantErrPart) {
			t.Errorf("%s: error %q does not mention %q", tc.name, err, tc.wantErrPart)
		}
	}
}

func TestContractFromPodDefaultIsRootfs(t *testing.T) {
	pod := &corev1.Pod{}
	contract, err := ContractFromPod(pod)
	if err != nil || contract.Provider != ProviderRootfs {
		t.Fatalf("empty annotations must keep rootfs default, got %+v, %v", contract, err)
	}
	pod.Annotations = map[string]string{AnnotationCheckpointProvider: "rootfs"}
	if contract, err := ContractFromPod(pod); err != nil || contract.Provider != ProviderRootfs {
		t.Fatalf("explicit rootfs must stay rootfs, got %+v, %v", contract, err)
	}
}
