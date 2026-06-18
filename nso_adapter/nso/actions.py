# SPDX-License-Identifier: Apache-2.0
"""NSO device actions — sync-from, compare-config, check-sync, connect.

Each function is a thin wrapper around the RESTCONF POST calls documented in
docs/nso-adapter.md §3.
"""

from __future__ import annotations

import time

import httpx
import structlog

from nso_adapter.nso.client import NsoClient

logger = structlog.get_logger(__name__)

_DEVICE_BASE = "/restconf/data/tailf-ncs:devices/device={name}"


async def sync_from(client: NsoClient, device_name: str) -> dict:
    """POST sync-from — refresh NSO CDB from the live device."""
    url = f"{client._base}{_DEVICE_BASE.format(name=device_name)}/sync-from"
    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.post(url)
        resp.raise_for_status()
        return resp.json().get("tailf-ncs:output", {})


async def compare_config(client: NsoClient, device_name: str) -> dict:
    """POST compare-config — return diff between CDB and live device."""
    url = f"{client._base}{_DEVICE_BASE.format(name=device_name)}/compare-config"
    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.post(url)
        resp.raise_for_status()
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json().get("tailf-ncs:output", {})


async def check_sync(client: NsoClient, device_name: str) -> bool:
    """POST check-sync — return True if device is in-sync with NSO CDB."""
    url = f"{client._base}{_DEVICE_BASE.format(name=device_name)}/check-sync"
    async with client._client(timeout=client._action_timeout) as c:
        try:
            resp = await c.post(url)
            resp.raise_for_status()
            result = resp.json().get("tailf-ncs:output", {}).get("result", "")
            return result == "in-sync"
        except Exception:
            return False


async def connect(client: NsoClient, device_name: str, timeout: float | None = None) -> dict:
    """POST connect — connectivity test to the device.

    *timeout* overrides the default action timeout — pass a short value for a reachability
    probe so an unreachable device cannot block on the full 2-minute action timeout.
    """
    url = f"{client._base}{_DEVICE_BASE.format(name=device_name)}/connect"
    async with client._client(timeout=timeout if timeout is not None else client._action_timeout) as c:
        resp = await c.post(url)
        resp.raise_for_status()
        return resp.json().get("tailf-ncs:output", {})


async def probe_reachable(client: NsoClient, device_name: str, timeout: float | None = None) -> tuple[bool, str, float]:
    """Test device manageability via NSO ``connect`` — reachability as NSO sees it.

    Returns ``(reachable, detail, elapsed_seconds)``. The device is reachable only when the
    connect action returns a truthy ``result`` (``"connected"``). NSO reports an unreachable
    device EITHER by raising an RPC/HTTP error OR by returning HTTP 200 with
    ``{"result": false, "info": ...}`` (the same shape ``fetch-host-keys`` uses for a failed
    SSH negotiation), so BOTH must be treated as unreachable — never raise-only. *detail*
    carries NSO's ``info`` / the error repr for logging.
    """
    start = time.perf_counter()
    try:
        out = await connect(client, device_name, timeout=timeout)
    except httpx.HTTPError as exc:
        return False, repr(exc), time.perf_counter() - start
    elapsed = time.perf_counter() - start
    result = out.get("result")
    reachable = result in ("connected", True, "true")
    detail = "" if reachable else str(out.get("info") or result or "")
    return reachable, detail, elapsed


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
