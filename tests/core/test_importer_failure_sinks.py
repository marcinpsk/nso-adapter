# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The surface fan-out must classify a failure, never repeat the server's text.

``sync.surface_refresh_failed`` logged ``repr(exc)`` for ANY surface. The surfaces that
raise today are adapter-authored, so the sink was safe for them, but a surface that lets an
``httpx`` failure out puts the request URL and the server's reason phrase into the record.
Both fan-outs are covered: ``_run_surfaces`` (the plain one) and ``_apply_projected`` (the
projected one).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import httpx
import pytest

from nso_adapter.store.models import Device
from tests.conftest import seed_device, session

#: A URL and reason phrase a real NSO would put in the httpx message.
_URL = "https://nso.invalid/restconf/data/placeholder-mount/placeholder-path"
_REASON = "Placeholder Reason Phrase"
_LEAKS = [_URL, "placeholder-mount", "placeholder-path", _REASON]


@asynccontextmanager
async def _device_session(device_id: int):
    async with session() as db:
        device = await db.get(Device, device_id)
        assert device is not None
        yield db, device
        return


def _httpx_failure() -> httpx.HTTPStatusError:
    """The REAL httpx-authored error, message built by httpx itself, not by hand."""
    request = httpx.Request("GET", _URL)
    response = httpx.Response(403, request=request, headers={"x-reason": _REASON}, text=_REASON)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        return exc
    raise AssertionError("raise_for_status did not raise on 403")


def _assert_classified(record: dict) -> None:
    detail = record["error"]
    for leaked in _LEAKS:
        assert leaked not in detail, f"the record repeats {leaked!r} from the server"
    assert "HTTPStatusError" in detail, "the record must still name the failure type"
    assert "403" in detail, "the operator must still be able to tell an auth refusal from an outage"


@pytest.mark.anyio
async def test_the_plain_fanout_classifies_an_httpx_failure_and_repeats_no_server_text(adapter_client):
    """``_run_surfaces`` is the plain fan-out: one surface raising must not take down the rest."""
    from structlog.testing import capture_logs

    from nso_adapter.core.importer import _run_surfaces

    device_id = await seed_device(nso_device_name="placeholder-sink-dev")

    async def _raises(db, device, nso_client, *, refresh_source):
        raise _httpx_failure()

    async with _device_session(device_id) as (db, device):
        with capture_logs() as logs:
            failed = await _run_surfaces(db, device, AsyncMock(), [("vlan", _raises)], "poll")

    assert failed == ["vlan"], "the surface must still be reported as failed"
    records = [r for r in logs if r["event"] == "sync.surface_refresh_failed"]
    assert records, "the failure was not reported at all"
    _assert_classified(records[0])


@pytest.mark.anyio
async def test_the_projected_fanout_classifies_an_httpx_failure_and_repeats_no_server_text(adapter_client):
    """``_apply_projected`` is the second fan-out, and it carried the same sink."""
    from structlog.testing import capture_logs

    from nso_adapter.core.importer import _apply_projected, _ProjectedRead, _projection_layout

    device_id = await seed_device(nso_device_name="placeholder-sink-dev-2")

    async def _raises(db, device, nso_client, *, refresh_source):
        raise _httpx_failure()

    surfaces = [("placeholder-spec-less-surface", _raises)]
    layout = _projection_layout(surfaces)
    assert layout.spec_by_name["placeholder-spec-less-surface"] is None, "the spec-less branch is under test"

    async with _device_session(device_id) as (db, device):
        projection = _ProjectedRead(
            device=device.nso_device_name,
            sections={},
            supplier_outcome=None,
            section_failures={},
        )
        with capture_logs() as logs:
            failed = await _apply_projected(db, device, AsyncMock(), surfaces, "poll", layout, projection)

    assert failed == ["placeholder-spec-less-surface"], "the surface must still be reported as failed"
    records = [r for r in logs if r["event"] == "sync.surface_refresh_failed"]
    assert records, "the failure was not reported at all"
    _assert_classified(records[0])
