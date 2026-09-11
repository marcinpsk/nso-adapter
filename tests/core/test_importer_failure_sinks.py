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

import ast
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest

from nso_adapter.store.models import Device
from tests.conftest import seed_device, session

#: A URL and reason phrase a real NSO would put in the httpx message.
_URL = "https://nso.invalid/restconf/data/placeholder-mount/placeholder-path"
_REASON = "Placeholder Reason Phrase"
_LEAKS = [_URL, "placeholder-mount", "placeholder-path", _REASON]
_IMPORTER = Path(__file__).resolve().parents[2] / "nso_adapter" / "core" / "importer.py"


def _raw_log_exception_renderers(source: str) -> list[int]:
    """Return log-field lines that render an exception with ``str`` or ``repr``."""
    violations: list[int] = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if not isinstance(node.func.value, ast.Name) or node.func.value.id != "logger":
            continue
        for keyword in node.keywords:
            if keyword.arg != "error":
                continue
            if any(
                isinstance(part, ast.Call) and isinstance(part.func, ast.Name) and part.func.id in {"str", "repr"}
                for part in ast.walk(keyword.value)
            ):
                violations.append(keyword.value.lineno)
    return violations


def test_raw_exception_log_guard_detects_str_and_repr() -> None:
    source = 'logger.warning("event", error=str(exc) or repr(exc))\n'
    assert _raw_log_exception_renderers(source) == [1]


def test_importer_never_logs_raw_exception_text() -> None:
    assert _raw_log_exception_renderers(_IMPORTER.read_text(encoding="utf-8")) == []


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
    response = httpx.Response(
        403,
        request=request,
        extensions={"reason_phrase": _REASON.encode("ascii")},
        text="placeholder body",
    )
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


# ── the action's own contract failures keep their own codes ──────────────────


def _atomic_output(device_name: str, sections: dict) -> dict:
    """One certified device-state-read output: atomic, right device, terminal sections."""
    return {"network-state-export:output": {"atomic": True, "device-name": device_name, **sections}}


async def test_a_missing_action_section_is_named_missing_not_malformed(adapter_client):
    """A requested family the action did not answer is an action contract failure.

    The certification deliberately lets a missing section through (`client.py:128`) because
    what it means is the caller's to decide, and the single-family escalation already decides
    `action_section_missing`. Splitting the multi-family output called it `section_malformed`,
    which says the server sent something unusable rather than nothing at all.
    """
    from nso_adapter.core.importer import _fetch_projection
    from nso_adapter.nso.read_outcome import ReadFailureCode, ReadOperation
    from tests.nso.test_nso_client_methods import MockTransport, _make_client

    device_id = await seed_device(nso_device_name="split-missing-section")
    client = _make_client()
    served = _atomic_output("split-missing-section", {"static-route": {"status": "ok", "route": []}})
    transport = MockTransport(200, served)
    client._client = lambda timeout=None: httpx.AsyncClient(transport=transport, base_url="http://nso:8080")

    async with _device_session(device_id) as (_db, device):
        sections, outcome, failures = await _fetch_projection(
            client, device, ["static-route", "interface-ip"], atomic=True
        )

    assert outcome is None, "the supplier answered; only one family is unserved"
    assert sections["static-route"] == {"status": "ok", "route": []}
    assert sections["interface-ip"] is None
    failure = failures["interface-ip"]
    assert failure.code is ReadFailureCode.action_section_missing
    assert failure.operation is ReadOperation.device_state_read
    assert failure.family == "interface-ip"


async def test_a_non_terminal_action_section_never_reaches_the_split(adapter_client):
    """The client refuses a non-terminal status, so the split cannot see a not-ready one.

    `_certify_device_state_output` (client.py:132-137) raises NsoReadContractError unless every
    requested-and-present section carries ok/unsupported/error, and that raise happens inside
    the supplier's own try, so the whole read degrades to read_error with the family rows kept.
    """
    from nso_adapter.core.importer import _fetch_projection
    from nso_adapter.nso.read_outcome import ReadOperation, Unavailable, UnavailableReason
    from tests.nso.test_nso_client_methods import MockTransport, _make_client

    device_id = await seed_device(nso_device_name="split-not-ready")
    client = _make_client()
    served = _atomic_output("split-not-ready", {"static-route": {"status": "not-ready"}})
    transport = MockTransport(200, served)
    client._client = lambda timeout=None: httpx.AsyncClient(transport=transport, base_url="http://nso:8080")

    async with _device_session(device_id) as (_db, device):
        sections, outcome, _failures = await _fetch_projection(client, device, ["static-route"], atomic=True)

    assert sections == {}, "nothing may be materialized from an uncertified answer"
    assert isinstance(outcome, Unavailable)
    assert outcome.reason is UnavailableReason.read_error
    assert outcome.failure.error_type == "NsoReadContractError"
    assert outcome.failure.operation is ReadOperation.device_state_read
