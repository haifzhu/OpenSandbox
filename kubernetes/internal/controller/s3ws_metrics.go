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
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// s3wsMetrics are the s3-workspace Prometheus metrics, registered on the
// controller-runtime metrics registry (exposed at :8080/metrics). They give
// the aggregate, cluster-wide view that Grafana dashboards and alerting
// rules consume; per-sandbox detail lives in execd's /metrics endpoint
// (queried on demand for debugging, not scraped).
var s3wsMetrics = struct {
	sandboxesByState *prometheus.GaugeVec
	snapshotsByPhase *prometheus.GaugeVec
	pauseDuration    prometheus.Histogram
	resumeDuration   prometheus.Histogram
	syncBytes        prometheus.Counter
	syncFailures     *prometheus.CounterVec
	circuitBreaker   prometheus.Counter
	purgeJobs        *prometheus.CounterVec
}{
	sandboxesByState: prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "s3ws_sandboxes",
			Help: "Number of s3-workspace sandboxes by BatchSandbox phase.",
		},
		[]string{"state"},
	),

	pauseDuration: prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "s3ws_pause_duration_seconds",
			Help:    "Wall time from pause dispatch to Paused (snapshot committed + pod released).",
			Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600},
		},
	),
	resumeDuration: prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "s3ws_resume_duration_seconds",
			Help:    "Wall time from resume dispatch to Running (restore init + pod ready).",
			Buckets: []float64{1, 5, 10, 30, 60, 120, 300},
		},
	),
	syncBytes: prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "s3ws_sync_uploaded_bytes_total",
			Help: "Cumulative bytes uploaded to object storage across all syncs.",
		},
	),
	syncFailures: prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3ws_sync_failures_total",
			Help: "Sync Job failures by reason.",
		},
		[]string{"reason"},
	),
	circuitBreaker: prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "s3ws_circuit_breaker_rejections_total",
			Help: "Pause requests rejected by the circuit breaker (409).",
		},
	),
	purgeJobs: prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3ws_purge_jobs_total",
			Help: "Object-storage purge Job outcomes.",
		},
		[]string{"status"},
	),
}

func init() {
	// Fix the snapshotsByPhase definition (placeholder above was incomplete).
	s3wsMetrics.snapshotsByPhase = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "s3ws_snapshot_phase",
			Help: "Number of SandboxSnapshot CRs by phase.",
		},
		[]string{"phase"},
	)

	metrics.Registry.MustRegister(
		s3wsMetrics.sandboxesByState,
		s3wsMetrics.snapshotsByPhase,
		s3wsMetrics.pauseDuration,
		s3wsMetrics.resumeDuration,
		s3wsMetrics.syncBytes,
		s3wsMetrics.syncFailures,
		s3wsMetrics.circuitBreaker,
		s3wsMetrics.purgeJobs,
	)
}
