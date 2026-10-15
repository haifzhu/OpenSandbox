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
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	sandboxv1alpha1 "github.com/alibaba/OpenSandbox/sandbox-k8s/apis/sandbox/v1alpha1"
	snapshotcontract "github.com/alibaba/OpenSandbox/sandbox-k8s/internal/snapshot"
	"github.com/alibaba/OpenSandbox/sandbox-k8s/internal/utils"
)

// workspacePurgeTimeout bounds one purge Job run.
const workspacePurgeTimeout = 10 * time.Minute

// isS3WorkspaceBatchSandbox reports whether the sandbox opted into the
// s3-workspace pause provider via its template annotations.
func isS3WorkspaceBatchSandbox(bs *sandboxv1alpha1.BatchSandbox) bool {
	if bs.Spec.Template == nil {
		return false
	}
	return snapshotcontract.IsS3WorkspaceProvider(
		bs.Spec.Template.Annotations[snapshotcontract.AnnotationCheckpointProvider])
}

// handleWorkspaceCleanup owns the workspace-cleanup finalizer on s3-workspace
// sandboxes:
//
//   - live sandboxes always carry the finalizer, so deletion is guaranteed a
//     reconcile pass to run the object-storage cleanup before the object
//     disappears;
//   - on deletion a standalone purge Job removes the sandbox's object prefix.
//     The Job carries no owner reference on purpose: it must outlive the
//     BatchSandbox being finalized. Purge is best-effort — object-storage
//     bucket lifecycle rules remain the documented backstop.
//
// Returns handled=true when the caller should stop this reconcile pass.
func (r *BatchSandboxReconciler) handleWorkspaceCleanup(ctx context.Context, bs *sandboxv1alpha1.BatchSandbox) (bool, error) {
	log := logf.FromContext(ctx)

	if !isS3WorkspaceBatchSandbox(bs) {
		return false, nil
	}

	if bs.DeletionTimestamp == nil {
		if !controllerutil.ContainsFinalizer(bs, finalizerWorkspaceCleanup) {
			if err := utils.UpdateFinalizer(r.Client, bs, utils.AddFinalizerOpType, finalizerWorkspaceCleanup); err != nil {
				return true, err
			}
			log.Info("Added workspace cleanup finalizer")
			return true, nil
		}
		return false, nil
	}

	if !controllerutil.ContainsFinalizer(bs, finalizerWorkspaceCleanup) {
		// Sandbox predates the finalizer (or another actor removed it):
		// nothing to sequence here.
		return false, nil
	}

	if err := r.ensureWorkspacePurgeJob(ctx, bs); err != nil {
		// Keep the finalizer so the next reconcile retries.
		return true, err
	}
	if err := utils.UpdateFinalizer(r.Client, bs, utils.RemoveFinalizerOpType, finalizerWorkspaceCleanup); err != nil {
		return true, err
	}
	log.Info("Released workspace cleanup finalizer")
	return true, nil
}

// ensureWorkspacePurgeJob creates (idempotently) the purge Job that deletes
// every object under the sandbox's s3-workspace prefix.
func (r *BatchSandboxReconciler) ensureWorkspacePurgeJob(ctx context.Context, bs *sandboxv1alpha1.BatchSandbox) error {
	log := logf.FromContext(ctx)
	annotations := bs.Spec.Template.Annotations
	target := annotations[snapshotcontract.AnnotationS3PauseTarget]
	if target == "" {
		return nil
	}
	if r.WorkspaceSyncImage == "" {
		// Best-effort semantics: without the syncer image there is nothing to
		// run; the bucket lifecycle policy is the backstop.
		log.Info("workspace-sync-image not configured; skipping object purge", "target", target)
		return nil
	}

	jobName := fmt.Sprintf("%s-wspurge", bs.Name)
	existing := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: bs.Namespace, Name: jobName}, existing); err == nil {
		return nil // already created (or completed; same target, idempotent)
	} else if !errors.IsNotFound(err) {
		return err
	}

	container := corev1.Container{
		Name:            workspaceSyncContainerName,
		Image:           r.WorkspaceSyncImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"/usr/local/bin/workspace-sync"},
		Args: []string{
			"--purge-target", target,
			"--endpoint", annotations[snapshotcontract.AnnotationS3PauseEndpoint],
			"--region", annotations[snapshotcontract.AnnotationS3PauseRegion],
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptrToBool(false),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	}
	if annotations[snapshotcontract.AnnotationS3PausePathStyle] == "true" {
		container.Args = append(container.Args, "--path-style")
	}
	container.Env = workspaceCredsEnv(annotations[snapshotcontract.AnnotationS3PauseCredsSecret])

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: bs.Namespace,
			Labels:    map[string]string{labelBatchSandboxNameKey: bs.Name},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptrToInt32(1),
			TTLSecondsAfterFinished: ptrToInt32(int32(defaultTTLSecondsAfterFinished)),
			ActiveDeadlineSeconds:   ptrToInt64(int64(workspacePurgeTimeout.Seconds())),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{container},
				},
			},
		},
	}
	if err := r.Create(ctx, job); err != nil {
		if errors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("create workspace purge job: %w", err)
	}
	log.Info("Created workspace purge job", "job", job.Name, "target", target)
	r.Recorder.Eventf(bs, corev1.EventTypeNormal, "CreatedPurgeJob", "Workspace purge job created for %s", target)
	return nil
}
