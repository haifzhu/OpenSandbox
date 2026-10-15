# Copyright 2026 The OpenSandbox Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""s3-workspace pause policy injection (OSEP draft: S3-Compatible Workspace
Pause).

When a sandbox opts into ``pausePolicy.provider = S3Workspace`` the create
path stamps the checkpoint-provider contract onto the pod template and gives
every sync root a shared volume from the very first incarnation, so pause
(sync + delete pod) and resume (restore init container + same volumes) keep
an identical mount layout across the sandbox lifecycle.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:  # pragma: no cover
    from opensandbox_server.api.schema import PausePolicy
    from opensandbox_server.config import AppConfig, PauseS3Config

logger = logging.getLogger("opensandbox_server.workspace_pause")

ANNOTATION_CHECKPOINT_PROVIDER = "sandbox.opensandbox.io/checkpoint-provider"
ANNOTATION_S3_PAUSE_TARGET = "sandbox.opensandbox.io/s3-pause-target"
ANNOTATION_S3_PAUSE_PATHS = "sandbox.opensandbox.io/s3-pause-paths"
ANNOTATION_S3_PAUSE_EXCLUDE = "sandbox.opensandbox.io/s3-pause-exclude"
ANNOTATION_S3_PAUSE_ENDPOINT = "sandbox.opensandbox.io/s3-pause-endpoint"
ANNOTATION_S3_PAUSE_REGION = "sandbox.opensandbox.io/s3-pause-region"
ANNOTATION_S3_PAUSE_PATH_STYLE = "sandbox.opensandbox.io/s3-pause-path-style"
ANNOTATION_S3_PAUSE_CREDS_SECRET = "sandbox.opensandbox.io/s3-pause-creds-secret"
ANNOTATION_EXECD_PORT = "sandbox.opensandbox.io/execd-port"

# execd listens on a fixed container port; only the host mapping is dynamic.
DEFAULT_EXECD_PORT = 44772

WORKSPACE_CREDS_KEYS = ("accessKeyId", "secretAccessKey", "sessionToken")


class WorkspacePauseError(ValueError):
    """Invalid s3-workspace pause configuration (maps to HTTP 400)."""


def build_workspace_pause_target(
    config: "PauseS3Config",
    pause_policy: "PausePolicy",
    namespace: str,
    sandbox_id: str,
) -> str:
    """Compose the canonical s3://bucket/prefix/sandboxes/{ns}/{id} target.

    All keys the sync engine can ever address derive from this prefix — the
    structural guarantee that a sandbox cannot read or write outside its own
    namespace.
    """
    bucket = getattr(pause_policy, "bucket", None) or _cfg(config, "bucket")
    if not bucket:
        raise WorkspacePauseError(
            "pausePolicy S3Workspace requires a bucket (request or [pause.s3] config)"
        )
    # Request-level overrides must hit a NON-EMPTY allowlist: an unset
    # allowlist rejects every override instead of allowing everything, so a
    # default deployment never widens where tenants may write.
    if getattr(pause_policy, "bucket", None):
        allowed_buckets = _cfg(config, "allowed_buckets") or []
        if bucket not in allowed_buckets:
            raise WorkspacePauseError(
                "pausePolicy.bucket overrides require a matching entry in "
                "[pause.s3] allowed_buckets"
            )

    prefix = getattr(pause_policy, "prefix", None) or _cfg(config, "base_prefix") or ""
    prefix = prefix.strip("/")
    if getattr(pause_policy, "prefix", None):
        allowed_prefixes = _cfg(config, "allowed_prefixes") or []
        if prefix not in allowed_prefixes:
            raise WorkspacePauseError(
                "pausePolicy.prefix overrides require a matching entry in "
                "[pause.s3] allowed_prefixes"
            )

    parts = [p for p in (prefix, "sandboxes", namespace, sandbox_id) if p]
    return f"s3://{bucket}/{'/'.join(parts)}"


def apply_workspace_pause(
    pod_spec: Dict[str, Any],
    annotations: Dict[str, str],
    pause_policy: "PausePolicy",
    app_config: Optional["AppConfig"],
    namespace: str,
    sandbox_id: str,
    request_volume_mount_paths: Optional[List[str]] = None,
) -> None:
    """Stamp the s3-workspace contract onto a pod spec (in place).

    Raises WorkspacePauseError (ValueError) on invalid or conflicting input;
    callers treat that as a create-time 400 before any CR is touched.
    """
    config = _pause_s3_config(app_config)

    roots = list(pause_policy.paths or _cfg(config, "default_paths") or ["/workspace"])
    if not roots:
        raise WorkspacePauseError("pausePolicy S3Workspace resolved to no paths")
    for root in roots:
        if not root.startswith("/") or "//" in root or root != root.rstrip("/"):
            raise WorkspacePauseError(
                f"pausePolicy paths must be clean absolute paths, got {root!r}"
            )
    if len(set(roots)) != len(roots):
        raise WorkspacePauseError("pausePolicy paths contain duplicates")
    if "/" in roots:
        raise WorkspacePauseError("pausePolicy paths must not include '/' itself")

    for mount_path in request_volume_mount_paths or []:
        if mount_path in roots:
            raise WorkspacePauseError(
                f"pausePolicy path {mount_path!r} conflicts with an explicit volume mount; "
                "choose a different path or drop the volume"
            )

    creds_secret = (
        getattr(pause_policy, "credentials_secret_name", None)
        or _cfg(config, "credentials_secret")
    )
    if not creds_secret:
        raise WorkspacePauseError(
            "pausePolicy S3Workspace requires credentialsSecretName "
            "(request or [pause.s3] config)"
        )

    target = build_workspace_pause_target(config, pause_policy, namespace, sandbox_id)

    annotations[ANNOTATION_CHECKPOINT_PROVIDER] = "s3-workspace"
    annotations[ANNOTATION_S3_PAUSE_TARGET] = target
    annotations[ANNOTATION_S3_PAUSE_PATHS] = json.dumps(roots)
    if pause_policy.exclude_patterns:
        annotations[ANNOTATION_S3_PAUSE_EXCLUDE] = json.dumps(pause_policy.exclude_patterns)
    endpoint = _cfg(config, "endpoint")
    region = _cfg(config, "region")
    if endpoint:
        annotations[ANNOTATION_S3_PAUSE_ENDPOINT] = endpoint
    if region:
        annotations[ANNOTATION_S3_PAUSE_REGION] = region
    if _cfg(config, "force_path_style"):
        annotations[ANNOTATION_S3_PAUSE_PATH_STYLE] = "true"
    annotations[ANNOTATION_S3_PAUSE_CREDS_SECRET] = creds_secret
    annotations[ANNOTATION_EXECD_PORT] = str(
        _cfg(config, "execd_port") or DEFAULT_EXECD_PORT
    )

    # Volume layout from the first incarnation: pause only syncs and deletes
    # the pod; resume re-mounts identical volumes and restores into them.
    containers = pod_spec.get("containers") or []
    if not containers:
        raise WorkspacePauseError("pod spec has no containers")
    main = containers[0]
    mounts = main.setdefault("volumeMounts", [])
    existing_paths = {m.get("mountPath") for m in mounts}
    for root in roots:
        if root in existing_paths:
            continue
        volume_name = _workspace_volume_name(root)
        pod_spec.setdefault("volumes", []).append(
            {"name": volume_name, "emptyDir": {}}
        )
        mounts.append({"name": volume_name, "mountPath": root})

    # Credentials are delivered as ROOT-ONLY FILES (mode 0400) projected from
    # the Secret at /opt/opensandbox/creds — never as container env, so user
    # commands cannot see them via `env` or /proc/<execd>/environ. execd
    # (running as root) reads them at sync time. Infra-side consumers (sync
    # Job, restore init, purge Job) keep env injection: those containers run
    # no user code.
    creds_volume = {
        "name": "objsync-creds",
        "secret": {
            "secretName": creds_secret,
            "defaultMode": 0o400,
            "items": [
                {"key": "accessKeyId", "path": "accessKeyId"},
                {"key": "secretAccessKey", "path": "secretAccessKey"},
                # sessionToken is optional: static AK/SK Secrets do not carry
                # one, and a required projection would fail the whole mount.
                {"key": "sessionToken", "path": "sessionToken", "optional": True},
            ],
        },
    }
    if not any(v.get("name") == "objsync-creds" for v in pod_spec.get("volumes", [])):
        pod_spec.setdefault("volumes", []).append(creds_volume)
    mounts = main.setdefault("volumeMounts", [])
    if not any(m.get("mountPath") == "/opt/opensandbox/creds" for m in mounts):
        mounts.append(
            {"name": "objsync-creds", "mountPath": "/opt/opensandbox/creds", "readOnly": True}
        )

    logger.info(
        "s3-workspace pause enabled for sandbox %s: target=%s roots=%s",
        sandbox_id,
        target,
        roots,
    )


def _pause_s3_config(app_config: Optional["AppConfig"]) -> Optional["PauseS3Config"]:
    pause_cfg = getattr(app_config, "pause", None) if app_config else None
    return getattr(pause_cfg, "s3", None)


def _cfg(config: Optional["PauseS3Config"], key: str) -> Any:
    if config is None:
        return None
    return getattr(config, key, None)


def _workspace_volume_name(root: str) -> str:
    """Cross-repo CONTRACT: kubernetes/internal/controller/batchsandbox_workspacerestore.go
    ::workspaceVolumeName derives the identical name ("ws-" + sha256 hex
    [:10]). Both sides pin the vector "/workspace" -> ws-c52ddf6553.
    """
    digest = hashlib.sha256(root.encode()).hexdigest()[:10]
    return f"ws-{digest}"
