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

"""The PausePolicy request contract.

The target path is derived from ``sandboxKey`` when the caller supplies one, so
that field is part of the isolation boundary, not just a convenience: it has to
be a single path-safe segment, and it must be rejected outright for the rootfs
backend where it means nothing.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from opensandbox_server.api.schema import PausePolicy


def test_sandbox_key_accepted_for_s3_workspace():
    policy = PausePolicy(provider="S3Workspace", paths=["/home/lingma"], sandboxKey="daemon-abc")
    assert policy.sandbox_key == "daemon-abc"


def test_sandbox_key_accepts_a_uuid():
    # What a caller-side daemon id looks like in practice.
    key = "3f2a1b4c-0000-1111-2222-333344445555"
    assert PausePolicy(provider="S3Workspace", sandboxKey=key).sandbox_key == key


@pytest.mark.parametrize(
    "key",
    [
        "../escape",  # path traversal out of the sandbox's own prefix
        "a/b",  # a nested segment would escape the single-segment contract
        "/absolute",
        ".hidden",  # the pattern wants an alphanumeric first character
        "has space",
        "",
        "x" * 129,
    ],
)
def test_sandbox_key_rejects_unsafe_segments(key):
    with pytest.raises(ValidationError):
        PausePolicy(provider="S3Workspace", sandboxKey=key)


def test_sandbox_key_rejected_for_rootfs():
    with pytest.raises(ValidationError):
        PausePolicy(provider="Rootfs", sandboxKey="daemon-abc")


def test_sandbox_key_is_optional():
    assert PausePolicy(provider="S3Workspace").sandbox_key is None
