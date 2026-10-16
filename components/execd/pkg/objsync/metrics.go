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

package objsync

import (
	"sync/atomic"
	"time"
)

// Metrics holds cumulative counters and the last observed durations for the
// s3-workspace data plane. The execd HTTP handler exposes these via
// /metrics/json; they are race-free (atomics only).
type Metrics struct {
	// Sync (pause-side) counters.
	SyncTotal         atomic.Int64
	SyncNoopTotal     atomic.Int64 // idempotency fence hits (no new generation)
	SyncFailTotal     atomic.Int64
	SyncDurationMs    atomic.Int64 // last sync wall time
	SyncUploadedBytes atomic.Int64 // cumulative across all syncs
	SyncFilesChanged  atomic.Int64 // cumulative

	// Restore (resume-side) counters.
	RestoreTotal      atomic.Int64
	RestoreFailTotal  atomic.Int64
	RestoreDurationMs atomic.Int64 // last restore wall time
	RestoreBytes      atomic.Int64 // cumulative
	RestoreFiles      atomic.Int64 // cumulative

	// Object store request metrics.
	OssRequests  atomic.Int64
	OssErrors    atomic.Int64
	OssLatencyMs atomic.Int64 // last request latency (rolling)

	// Sync gate (write quiesce).
	GateRejections atomic.Int64
	GateDrainMs    atomic.Int64 // last drain wait

	// Multipart per-part timing (last part, for bottleneck diagnosis).
	MpuPartMs atomic.Int64

	// Retention GC.
	GCRuns            atomic.Int64
	GCGenerations     atomic.Int64
	GCBlobs           atomic.Int64
	GCCompressedBytes atomic.Int64
	GCRawBytes        atomic.Int64
}

// Global is the process-wide metrics instance; execd is single-sandbox-per-pod.
var Global = &Metrics{}

// SyncResult records the outcome of one sync pass.
func (m *Metrics) RecordSync(dur time.Duration, uploadedBytes, filesChanged int64, noop bool) {
	m.SyncTotal.Add(1)
	if noop {
		m.SyncNoopTotal.Add(1)
	}
	m.SyncDurationMs.Store(dur.Milliseconds())
	m.SyncUploadedBytes.Add(uploadedBytes)
	m.SyncFilesChanged.Add(filesChanged)
}

func (m *Metrics) RecordSyncFail() { m.SyncFailTotal.Add(1) }
func (m *Metrics) RecordRestore(dur time.Duration, bytes, files int64) {
	m.RestoreTotal.Add(1)
	m.RestoreDurationMs.Store(dur.Milliseconds())
	m.RestoreBytes.Add(bytes)
	m.RestoreFiles.Add(files)
}
func (m *Metrics) RecordRestoreFail() { m.RestoreFailTotal.Add(1) }
func (m *Metrics) RecordOss(dur time.Duration, isErr bool) {
	m.OssRequests.Add(1)
	if isErr {
		m.OssErrors.Add(1)
	}
	m.OssLatencyMs.Store(dur.Milliseconds())
}
func (m *Metrics) RecordGateRejection()              { m.GateRejections.Add(1) }
func (m *Metrics) RecordGateDrain(dur time.Duration) { m.GateDrainMs.Store(dur.Milliseconds()) }
func (m *Metrics) RecordMpuPart(dur time.Duration)   { m.MpuPartMs.Store(dur.Milliseconds()) }
func (m *Metrics) RecordGC(generations, blobs int) {
	m.GCRuns.Add(1)
	m.GCGenerations.Add(int64(generations))
	m.GCBlobs.Add(int64(blobs))
}
func (m *Metrics) RecordCompression(compressed, raw int64) {
	m.GCCompressedBytes.Add(compressed)
	m.GCRawBytes.Add(raw)
}

// Snapshot returns a plain map suitable for JSON serialisation.
func (m *Metrics) Snapshot() map[string]int64 {
	return map[string]int64{
		"objsync_sync_total":          m.SyncTotal.Load(),
		"objsync_sync_noop_total":     m.SyncNoopTotal.Load(),
		"objsync_sync_fail_total":     m.SyncFailTotal.Load(),
		"objsync_sync_duration_ms":    m.SyncDurationMs.Load(),
		"objsync_sync_uploaded_bytes": m.SyncUploadedBytes.Load(),
		"objsync_sync_files_changed":  m.SyncFilesChanged.Load(),
		"objsync_restore_total":       m.RestoreTotal.Load(),
		"objsync_restore_fail_total":  m.RestoreFailTotal.Load(),
		"objsync_restore_duration_ms": m.RestoreDurationMs.Load(),
		"objsync_restore_bytes":       m.RestoreBytes.Load(),
		"objsync_restore_files":       m.RestoreFiles.Load(),
		"objsync_oss_requests":        m.OssRequests.Load(),
		"objsync_oss_errors":          m.OssErrors.Load(),
		"objsync_oss_latency_ms":      m.OssLatencyMs.Load(),
		"objsync_gate_rejections":     m.GateRejections.Load(),
		"objsync_gate_drain_ms":       m.GateDrainMs.Load(),
		"objsync_mpu_part_ms":         m.MpuPartMs.Load(),
		"objsync_gc_runs":             m.GCRuns.Load(),
		"objsync_gc_generations":      m.GCGenerations.Load(),
		"objsync_gc_blobs":            m.GCBlobs.Load(),
		"objsync_compressed_bytes":    m.GCCompressedBytes.Load(),
		"objsync_raw_bytes":           m.GCRawBytes.Load(),
	}
}
