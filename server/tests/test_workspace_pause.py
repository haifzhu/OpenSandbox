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

"""Tests for the s3-workspace pause policy injector.

The module is loaded directly from its file path so the test does not pull the
full services import chain (psycopg & co); the injector itself is stdlib-only
by design.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
import sys
from types import SimpleNamespace

import pytest

_MODULE_PATH = (
    pathlib.Path(__file__).resolve().parent.parent
    / "opensandbox_server"
    / "services"
    / "k8s"
    / "workspace_pause.py"
)


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "workspace_pause_under_test", _MODULE_PATH
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def wp():
    return _load_module()


def _config(**overrides) -> SimpleNamespace:
    values = dict(
        endpoint="http://minio.svc:9000",
        region="us-east-1",
        bucket="sbx",
        base_prefix="lingma/prod",
        force_path_style=True,
        credentials_secret="oss-creds",
        allowed_buckets=[],
        allowed_prefixes=[],
        default_paths=["/workspace"],
        execd_port=44772,
        max_attempts=3,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def _app(cfg: SimpleNamespace) -> SimpleNamespace:
    return SimpleNamespace(pause=SimpleNamespace(s3=cfg))


def _policy(**overrides) -> SimpleNamespace:
    values = dict(
        provider="S3Workspace",
        paths=["/workspace", "/root/.lingma"],
        bucket=None,
        prefix=None,
        credentials_secret_name=None,
        exclude_patterns=["node_modules"],
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def test_apply_stamps_contract_and_volumes(wp):
    pod_spec = {"containers": [{"name": "sandbox", "env": []}], "volumes": []}
    annotations: dict = {}

    wp.apply_workspace_pause(
        pod_spec, annotations, _policy(), _app(_config()), "ns1", "sbx-1"
    )

    assert (
        annotations[wp.ANNOTATION_S3_PAUSE_TARGET]
        == "s3://sbx/lingma/prod/sandboxes/ns1/sbx-1"
    )
    assert json.loads(annotations[wp.ANNOTATION_S3_PAUSE_PATHS]) == [
        "/workspace",
        "/root/.lingma",
    ]
    assert annotations[wp.ANNOTATION_CHECKPOINT_PROVIDER] == "s3-workspace"
    assert annotations[wp.ANNOTATION_S3_PAUSE_CREDS_SECRET] == "oss-creds"
    assert annotations[wp.ANNOTATION_EXECD_PORT] == "44772"
    # Volume layout from the first incarnation, contract-named, plus the
    # root-only credentials volume.
    assert len(pod_spec["volumes"]) == 3
    mounts = {m["mountPath"]: m["name"] for m in pod_spec["containers"][0]["volumeMounts"]}
    assert mounts["/workspace"] == "ws-c52ddf6553"
    assert mounts["/root/.lingma"] == "ws-27b691f987"
    assert mounts["/opt/opensandbox/creds"] == "objsync-creds"
    # Credentials as root-only files, NOT env.
    env_names = {e.get("name") for e in pod_spec["containers"][0].get("env", [])}
    assert not ({"OBJSYNC_ACCESS_KEY_ID", "OBJSYNC_SECRET_ACCESS_KEY"} & env_names)
    creds_vols = [v for v in pod_spec["volumes"] if v.get("name") == "objsync-creds"]
    assert len(creds_vols) == 1
    assert creds_vols[0]["secret"]["defaultMode"] == 0o400
    # No items list: the whole Secret is projected as same-named files, so a
    # static AK/SK Secret (no sessionToken) cannot fail the mount. Listing
    # items would need KeyToPath.optional, which the control plane's typed
    # Python client silently drops (kubernetes==35 has no such field).
    assert "items" not in creds_vols[0]["secret"]
    mounts = {m["mountPath"]: m for m in pod_spec["containers"][0]["volumeMounts"]}
    assert mounts["/opt/opensandbox/creds"]["readOnly"] is True


def test_volume_name_contract_parity(wp):
    # Must match kubernetes/internal/controller/batchsandbox_workspacerestore.go
    vectors = {"/workspace": "ws-c52ddf6553", "/root/.lingma": "ws-27b691f987"}
    for root, want in vectors.items():
        assert wp._workspace_volume_name(root) == want


def test_bucket_override_requires_nonempty_allowlist(wp):
    policy = _policy(bucket="other-bucket")
    with pytest.raises(wp.WorkspacePauseError, match="allowed_buckets"):
        wp.apply_workspace_pause(
            {"containers": [{}]},
            {},
            policy,
            _app(_config(allowed_buckets=[])),
            "ns1",
            "sbx-1",
        )

    # Matching allowlist passes.
    annotations: dict = {}
    wp.apply_workspace_pause(
        {"containers": [{}], "volumes": []},
        annotations,
        _policy(bucket="other-bucket"),
        _app(_config(allowed_buckets=["other-bucket"])),
        "ns1",
        "sbx-1",
    )
    assert annotations[wp.ANNOTATION_S3_PAUSE_TARGET].startswith(
        "s3://other-bucket/"
    )


def test_prefix_override_requires_matching_allowlist(wp):
    with pytest.raises(wp.WorkspacePauseError, match="allowed_prefixes"):
        wp.apply_workspace_pause(
            {"containers": [{}]},
            {},
            _policy(prefix="tenant-b"),
            _app(_config(allowed_prefixes=["tenant-a"])),
            "ns1",
            "sbx-1",
        )


def test_path_conflicts_with_explicit_volume_rejected(wp):
    with pytest.raises(wp.WorkspacePauseError, match="conflicts"):
        wp.apply_workspace_pause(
            {"containers": [{}]},
            {},
            _policy(),
            _app(_config()),
            "ns1",
            "sbx-1",
            request_volume_mount_paths=["/workspace"],
        )


def test_invalid_paths_rejected(wp):
    for bad in ["workspace", "/a//b", "/trailing/", "/"]:
        with pytest.raises(wp.WorkspacePauseError):
            wp.apply_workspace_pause(
                {"containers": [{}]},
                {},
                _policy(paths=[bad]),
                _app(_config()),
                "ns1",
                "sbx-1",
            )


def test_missing_bucket_or_secret_rejected(wp):
    with pytest.raises(wp.WorkspacePauseError, match="bucket"):
        wp.apply_workspace_pause(
            {"containers": [{}]}, {}, _policy(), _app(_config(bucket="")), "ns", "id"
        )
    with pytest.raises(wp.WorkspacePauseError, match="credentialsSecretName"):
        wp.apply_workspace_pause(
            {"containers": [{}]},
            {},
            _policy(),
            _app(_config(credentials_secret="")),
            "ns",
            "id",
        )
