# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Claim-first classification at the real RESTCONF sender boundary."""

import inspect
import json

import httpx
import pytest

from nso_adapter.nso.apply import NsoApplyError, apply_device_intent
from nso_adapter.nso.refusal_protocol import refusal, render_refusals, row_ref
from nso_adapter.nso.refusals import DefiniteRefusal, ProtocolFault
from tests._secret_discipline import assert_chain_free_of
from tests.nso.test_apply_send import _client_with

CANARY = "placeholder-secret-canary"
SITE = refusal("vlan", row_ref("vlan", "vlan.vlan", (("vlan", (101,)),)), "write-blocked")


def envelope(records=(SITE,)):
    return json.dumps(
        {
            "ietf-restconf:errors": {
                "error": [
                    {
                        "error-type": "application",
                        "error-tag": "operation-failed",
                        "error-message": render_refusals(records),
                    }
                ]
            }
        }
    ).encode()


class ErrorTransport(httpx.AsyncBaseTransport):
    def __init__(self, status, body, *, verification=False):
        self.status = status
        self.body = body
        self.verification = verification
        self.requests = []

    async def handle_async_request(self, request):
        self.requests.append(request)
        if self.verification and "dry-run=" not in str(request.url):
            return httpx.Response(204, request=request)
        return httpx.Response(self.status, content=self.body, request=request)


@pytest.mark.parametrize("dry_run", [False, True, "cli"])
@pytest.mark.parametrize("strict", [False, True])
async def test_sender_classifies_refusal_in_commit_and_both_dry_runs(dry_run, strict):
    transport = ErrorTransport(500, envelope())
    with pytest.raises(NsoApplyError) as caught:
        await apply_device_intent(
            _client_with(transport),
            "placeholder-device",
            {"vlan": {"vlan": [{"vlan-id": 101}]}},
            device_id=1,
            dry_run=dry_run,
            strict=strict,
        )
    assert isinstance(getattr(caught.value, "classification", None), DefiniteRefusal)
    assert caught.value.sender_phase == ("pre_commit_dry_run" if dry_run else "commit")
    assert not caught.value.commit_acknowledged
    assert len(transport.requests) == 1


@pytest.mark.parametrize(
    "body",
    [
        b'{"error-message":"device-intent: refused ' + CANARY.encode(),
        b'{"error-message":"device-intent: refu\\u0073ed ',
        b"opaque " + CANARY.encode(),
    ],
)
@pytest.mark.parametrize("dry_run", [False, True, "cli"])
@pytest.mark.parametrize("strict", [False, True])
async def test_sender_malformed_500_is_protocol_fault(body, dry_run, strict):
    with pytest.raises(NsoApplyError) as caught:
        await apply_device_intent(
            _client_with(ErrorTransport(500, body)),
            "placeholder-device",
            {},
            device_id=1,
            dry_run=dry_run,
            strict=strict,
        )
    assert isinstance(getattr(caught.value, "classification", None), ProtocolFault)
    assert_chain_free_of(caught.value, [CANARY])


async def _post_commit_error(status, body, classification):
    transport = ErrorTransport(status, body, verification=True)
    with pytest.raises(NsoApplyError) as caught:
        await apply_device_intent(_client_with(transport), "placeholder-device", {}, device_id=1)
    assert isinstance(getattr(caught.value, "classification", None), classification)
    assert caught.value.sender_phase == "post_commit_verification"
    assert caught.value.commit_acknowledged
    assert_chain_free_of(caught.value, [CANARY])
    assert len(transport.requests) == 2


async def test_post_commit_malformed_claim_is_protocol_fault():
    await _post_commit_error(502, b"device-intent: refused " + CANARY.encode(), ProtocolFault)


async def test_post_commit_malformed_500_is_protocol_fault():
    await _post_commit_error(500, b"opaque " + CANARY.encode(), ProtocolFault)


@pytest.mark.parametrize("status", [502, 503])
@pytest.mark.parametrize("body", [b"{", b"opaque"])
async def test_post_commit_malformed_unclaimed_error_is_unavailable(status, body):
    checkpoints = []

    async def checkpoint(phase, classification, acknowledged):
        checkpoints.append((phase, classification, acknowledged))

    transport = ErrorTransport(status, body, verification=True)
    kwargs = {"checkpoint": checkpoint} if "checkpoint" in inspect.signature(apply_device_intent).parameters else {}
    await apply_device_intent(_client_with(transport), "placeholder-device", {}, device_id=1, **kwargs)
    assert checkpoints and checkpoints[0] == ("commit", None, True), (
        "commit acknowledgement must reach the worker before inconclusive verification"
    )
    assert checkpoints[-1][0] == "post_commit_verification"


@pytest.mark.parametrize("status", [200, 201, 204, 400, 503])
@pytest.mark.parametrize("dry_run", [False, True, "cli"])
async def test_claimed_refusal_under_another_status_fails_closed(status, dry_run):
    transport = ErrorTransport(status, envelope())
    with pytest.raises(NsoApplyError) as caught:
        await apply_device_intent(_client_with(transport), "placeholder-device", {}, device_id=1, dry_run=dry_run)
    assert isinstance(getattr(caught.value, "classification", None), ProtocolFault)
    assert not caught.value.commit_acknowledged
