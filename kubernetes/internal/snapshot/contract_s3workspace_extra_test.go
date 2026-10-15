package snapshot

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestS3WorkspaceRejectsQEMUContract(t *testing.T) {
	pod := &corev1.Pod{}
	pod.Annotations = map[string]string{
		"sandbox.opensandbox.io/checkpoint-provider": "s3-workspace",
		"sandbox.opensandbox.io/s3-pause-target":     "s3://b/p",
		"sandbox.opensandbox.io/s3-pause-paths":      `["/workspace"]`,
		"sandbox.opensandbox.io/execd-port":          "44772",
		"sandbox.opensandbox.io/qemu-container":      "vm",
	}
	if _, err := ContractFromPod(pod); err == nil {
		t.Fatal("s3-workspace must be rejected when a QEMU contract is present")
	}
}

func TestIsS3WorkspaceProviderNormalization(t *testing.T) {
	for _, yes := range []string{"s3-workspace", " S3-Workspace ", "S3-WORKSPACE"} {
		if !IsS3WorkspaceProvider(yes) {
			t.Errorf("IsS3WorkspaceProvider(%q) = false, want true", yes)
		}
	}
	for _, no := range []string{"", "rootfs", "qemu", "s3-workspacex"} {
		if IsS3WorkspaceProvider(no) {
			t.Errorf("IsS3WorkspaceProvider(%q) = true, want false", no)
		}
	}
}
