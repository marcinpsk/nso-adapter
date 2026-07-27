# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Unit tests for api.deps.verify_token — the sole bearer gate for write/apply/push.

Exercises the real dependency function (no HTTP stack) with a minimal request stub. The
compare is constant-time (hmac.compare_digest); these lock in the accept/reject behavior so
that hardening can never silently regress into accepting a wrong token.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.security import HTTPAuthorizationCredentials

from nso_adapter.api.deps import verify_token
from nso_adapter.api.errors import ApiError


def _req(token: str) -> SimpleNamespace:
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(adapter_token=token)))


def _cred(value: str) -> HTTPAuthorizationCredentials:
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=value)


async def test_verify_token_accepts_matching():
    assert await verify_token(_req("s3cr3t-token"), _cred("s3cr3t-token")) == "s3cr3t-token"


async def test_verify_token_rejects_wrong_token_sharing_a_prefix():
    with pytest.raises(ApiError):
        await verify_token(_req("s3cr3t-token"), _cred("s3cr3t-toke!"))


async def test_verify_token_rejects_missing_credentials():
    with pytest.raises(ApiError):
        await verify_token(_req("s3cr3t-token"), None)
