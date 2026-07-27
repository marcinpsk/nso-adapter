# SPDX-License-Identifier: Apache-2.0
"""NSO device actions — sync-from, compare-config, check-sync, connect.

Each function is a thin wrapper around the RESTCONF POST calls documented in
docs/nso-adapter.md §3.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import StrEnum

import httpx
import structlog

from nso_adapter.nso.client import NsoClient, _url_key

logger = structlog.get_logger(__name__)

_DEVICE_BASE = "/restconf/data/tailf-ncs:devices/device={name}"


class ProbeStatus(StrEnum):
    """Why a reachability probe passed or failed."""

    ok = "ok"
    unreachable = "unreachable"
    timeout = "timeout"
    error = "error"


@dataclass(frozen=True)
class ReachabilityProbe:
    """Structured probe outcome with tuple-unpacking compatibility for older callers."""

    status: ProbeStatus
    detail: str
    elapsed: float

    @property
    def reachable(self) -> bool:
        return self.status == ProbeStatus.ok

    def __iter__(self):
        yield self.reachable
        yield self.detail
        yield self.elapsed


def _device_base(client: NsoClient, device_name: str) -> str:
    """Device action URL base with the device name percent-encoded (RFC 8040 list key)."""
    return f"{client._base}{_DEVICE_BASE.format(name=_url_key(device_name))}"


async def sync_from(client: NsoClient, device_name: str) -> dict:
    """POST sync-from — refresh NSO CDB from the live device."""
    url = f"{_device_base(client, device_name)}/sync-from"
    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.post(url)
        resp.raise_for_status()
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json().get("tailf-ncs:output", {})


async def compare_config(client: NsoClient, device_name: str) -> dict:
    """POST compare-config — return diff between CDB and live device."""
    url = f"{_device_base(client, device_name)}/compare-config"
    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.post(url)
        resp.raise_for_status()
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json().get("tailf-ncs:output", {})


async def check_sync(client: NsoClient, device_name: str) -> bool:
    """POST check-sync — return True if device is in-sync with NSO CDB.

    Raises on an HTTP error rather than swallowing it into a false 'not in sync'
    (indistinguishable from real drift).
    """
    url = f"{_device_base(client, device_name)}/check-sync"
    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.post(url)
        resp.raise_for_status()
        if resp.status_code == 204 or not resp.content:
            return False
        return resp.json().get("tailf-ncs:output", {}).get("result", "") == "in-sync"


async def connect(client: NsoClient, device_name: str, timeout: float | None = None) -> dict:
    """POST connect — connectivity test to the device.

    *timeout* overrides the default action timeout — pass a short value for a reachability
    probe so an unreachable device cannot block on the full 2-minute action timeout.
    """
    url = f"{_device_base(client, device_name)}/connect"
    async with client._client(timeout=timeout if timeout is not None else client._action_timeout) as c:
        resp = await c.post(url)
        resp.raise_for_status()
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json().get("tailf-ncs:output", {})


async def probe_reachable(client: NsoClient, device_name: str, timeout: float | None = None) -> ReachabilityProbe:
    """Test device manageability via NSO ``connect`` — reachability as NSO sees it.

    Returns a structured outcome which can still be unpacked as
    ``(reachable, detail, elapsed_seconds)``. The device is reachable only when the
    connect action returns a truthy ``result`` (``"connected"``). An HTTP 200 with
    ``{"result": false, "info": ...}`` is a genuine unreachable verdict from NSO. Timeouts
    and NSO/API errors remain separate outcomes because neither proves the device address is
    down. *detail* carries NSO's ``info`` or the exception representation for logging and UI.
    """
    start = time.perf_counter()
    try:
        out = await connect(client, device_name, timeout=timeout)
    except httpx.TimeoutException as exc:
        return ReachabilityProbe(ProbeStatus.timeout, repr(exc), time.perf_counter() - start)
    except (httpx.HTTPError, ValueError) as exc:
        # An NSO/API/decoding failure does not prove that the device address is unreachable.
        return ReachabilityProbe(ProbeStatus.error, repr(exc), time.perf_counter() - start)
    elapsed = time.perf_counter() - start
    result = out.get("result")
    reachable = result in ("connected", True, "true")
    detail = "" if reachable else str(out.get("info") or result or "")
    status = ProbeStatus.ok if reachable else ProbeStatus.unreachable
    return ReachabilityProbe(status, detail, elapsed)


async def capability_probe(client: NsoClient, device_name: str) -> dict:
    """POST the route-policy capability-probe action.

    Returns the reconciler's representable-half verdict for the device's NED:
    ``{ned-id, sw-version, element:[{scope,name,status,detail}, ...]}``.
    """
    url = f"{client._base}/restconf/data/route-policy-reconciler:route-policy-capability/probe"
    body = {"route-policy-reconciler:input": {"device": device_name}}
    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.post(url, json=body)
        resp.raise_for_status()
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json().get("route-policy-reconciler:output", {})
