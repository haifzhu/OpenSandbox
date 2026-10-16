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
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	sandboxv1alpha1 "github.com/alibaba/OpenSandbox/sandbox-k8s/apis/sandbox/v1alpha1"
	snapshotcontract "github.com/alibaba/OpenSandbox/sandbox-k8s/internal/snapshot"
)

const workspaceSyncContainerName = "sync"

// workspaceInfraResources are the resource requests/limits applied to the
// s3-workspace infrastructure containers (sync trigger, restore init, purge).
// Namespaces under a ResourceQuota commonly require every container to
// declare both requests and limits; these modest values are I/O-bound work.
var workspaceInfraResources = corev1.ResourceRequirements{
	Requests: corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("100m"),
		corev1.ResourceMemory: resource.MustParse("128Mi"),
	},
	Limits: corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("500m"),
		corev1.ResourceMemory: resource.MustParse("512Mi"),
	},
}

// isS3WorkspaceSnapshot reports whether the snapshot persists via the
// s3-workspace provider.
func isS3WorkspaceSnapshot(snapshot *sandboxv1alpha1.SandboxSnapshot) bool {
	return snapshot.Status.Format == sandboxv1alpha1.SandboxSnapshotFormatS3WorkspaceV1
}

// handlePendingS3Workspace drives the s3-workspace pause path: instead of a
// privileged same-node commit Job over containerd, an unprivileged sync Job
// calls execd inside the sandbox pod, which performs the incremental object
// store sync. No hostPath mounts, no node pinning, PSS-restricted compatible.
func (r *SandboxSnapshotReconciler) handlePendingS3Workspace(
	ctx context.Context,
	snapshot *sandboxv1alpha1.SandboxSnapshot,
	pod *corev1.Pod,
	contract *snapshotcontract.S3WorkspaceContract,
	snapshotFormat sandboxv1alpha1.SandboxSnapshotFormat,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if contract == nil {
		msg := "s3-workspace contract missing"
		_ = r.updateSnapshotStatus(ctx, snapshot, sandboxv1alpha1.SandboxSnapshotPhaseFailed, "InvalidCheckpointContract", msg)
		return ctrl.Result{}, nil
	}
	if r.WorkspaceSyncImage == "" {
		msg := "workspace-sync-image not configured in controller manager"
		_ = r.updateSnapshotStatus(ctx, snapshot, sandboxv1alpha1.SandboxSnapshotPhaseFailed, "SyncImageNotConfigured", msg)
		return ctrl.Result{}, nil
	}
	if pod.Status.PodIP == "" {
		// The sync Job addresses execd on the pod IP; without it there is
		// nothing to do but wait for the pod to report one.
		log.Info("Source pod has no IP yet; requeueing", "pod", pod.Name)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	if err := r.persistResolvedData(ctx, snapshot, pod.Name, pod.Spec.NodeName, snapshotFormat, nil); err != nil {
		return ctrl.Result{}, err
	}
	snapshot.Status.SourcePodName = pod.Name
	snapshot.Status.SourceNodeName = pod.Spec.NodeName
	snapshot.Status.Format = snapshotFormat

	job, err := r.buildSyncJob(snapshot, pod, contract)
	if err != nil {
		msg := fmt.Sprintf("failed to build sync job: %v", err)
		_ = r.updateSnapshotStatus(ctx, snapshot, sandboxv1alpha1.SandboxSnapshotPhaseFailed, "BuildSyncJobFailed", msg)
		return ctrl.Result{}, nil
	}
	if err := r.Create(ctx, job); err != nil {
		log.Error(err, "Failed to create sync job")
		r.Recorder.Eventf(snapshot, corev1.EventTypeWarning, "FailedCreateJob", "Failed to create sync job: %v", err)
		return ctrl.Result{}, err
	}
	log.Info("Created workspace sync job", "job", job.Name)
	r.Recorder.Eventf(snapshot, corev1.EventTypeNormal, "CreatedJob", "Created workspace sync job: %s", job.Name)
	_ = r.updateSnapshotStatus(ctx, snapshot, sandboxv1alpha1.SandboxSnapshotPhaseCommitting, "Syncing", "Workspace sync job created")
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

// buildSyncJob creates the unprivileged sync trigger Job. The Job container
// runs workspace-sync, which POSTs the sync request to execd on the source
// pod and writes the S3Result JSON to its termination message.
func (r *SandboxSnapshotReconciler) buildSyncJob(
	snapshot *sandboxv1alpha1.SandboxSnapshot,
	pod *corev1.Pod,
	contract *snapshotcontract.S3WorkspaceContract,
) (*batchv1.Job, error) {
	request := snapshotcontract.S3Request{
		Version:         snapshotcontract.RequestVersionV1,
		TargetURL:       contract.Target,
		SourcePodIP:     pod.Status.PodIP,
		ExecDPort:       contract.ExecDPort,
		Roots:           contract.Roots,
		ExcludePatterns: contract.Exclude,
		Endpoint:        contract.Endpoint,
		Region:          contract.Region,
		ForcePathStyle:  contract.ForcePathStyle,
		CredsSecretName: pod.Annotations[snapshotcontract.AnnotationS3PauseCredsSecret],
	}
	requestData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal s3-workspace request: %w", err)
	}

	// Project the object-store credentials so the sync Job can independently
	// verify LATEST after execd reports success (commit-point verification).
	env := workspaceCredsEnv(pod.Annotations[snapshotcontract.AnnotationS3PauseCredsSecret])

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.getJobName(snapshot),
			Namespace: snapshot.Namespace,
			Labels: map[string]string{
				labelSandboxSnapshotName: snapshot.Name,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptrToInt32(DefaultCommitJobBackoffLimit),
			TTLSecondsAfterFinished: ptrToInt32(int32(defaultTTLSecondsAfterFinished)),
			ActiveDeadlineSeconds:   ptrToInt64(int64(r.getCommitJobTimeout().Seconds())),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:    corev1.RestartPolicyNever,
					ImagePullSecrets: r.imageCommitterPullSecrets(),
					Containers: []corev1.Container{
						{
							Name:            workspaceSyncContainerName,
							Image:           r.WorkspaceSyncImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Command:         []string{"/usr/local/bin/workspace-sync"},
							Args: []string{
								"--request-base64", base64.StdEncoding.EncodeToString(requestData),
							},
							Env:       env,
							Resources: workspaceInfraResources,
							// Unprivileged by design: this Job only performs an
							// HTTP call against execd on the pod network.
							SecurityContext: &corev1.SecurityContext{
								AllowPrivilegeEscalation: ptrToBool(false),
								Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
							},
						},
					},
					// No nodeName pin: the sync is addressable over the pod
					// network and may run on any node.
				},
			},
		},
	}
	if err := ctrl.SetControllerReference(snapshot, job, r.Scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference: %w", err)
	}
	return job, nil
}

// updateSnapshotStatusFromSucceededSyncJob fills Workspace status from the
// sync Job termination message.
func (r *SandboxSnapshotReconciler) updateSnapshotStatusFromSucceededSyncJob(ctx context.Context, snapshot *sandboxv1alpha1.SandboxSnapshot, job *batchv1.Job) error {
	result, found, err := r.getCommitJobResult(ctx, snapshot.Namespace, job.Name)
	if err != nil {
		return err
	}
	if !found || result.S3Workspace == nil {
		return fmt.Errorf("successful workspace sync job did not report s3Workspace result")
	}
	s3 := result.S3Workspace

	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		latest := &sandboxv1alpha1.SandboxSnapshot{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: snapshot.Namespace, Name: snapshot.Name}, latest); err != nil {
			return err
		}
		if isTerminalSnapshotPhase(latest.Status.Phase) && latest.Status.Phase != sandboxv1alpha1.SandboxSnapshotPhaseSucceed {
			return nil
		}
		latest.Status.Workspace = &sandboxv1alpha1.WorkspaceSnapshot{
			Target:                s3.Target,
			Generation:            s3.Generation,
			Roots:                 s3.Roots,
			Endpoint:              s3.Endpoint,
			Region:                s3.Region,
			ForcePathStyle:        s3.ForcePathStyle,
			FilesTotal:            s3.FilesTotal,
			BytesUploaded:         s3.BytesUploaded,
			SyncSeconds:           s3.SyncSeconds,
			CredentialsSecretName: s3.CredsSecretName,
		}
		latest.Status.Phase = sandboxv1alpha1.SandboxSnapshotPhaseSucceed
		applySnapshotPhaseConditions(&latest.Status, "", "")

		s3wsMetrics.syncBytes.Add(float64(s3.BytesUploaded))
		s3wsMetrics.pauseDuration.Observe(float64(s3.SyncSeconds))

		return r.Status().Update(ctx, latest)
	})
}
