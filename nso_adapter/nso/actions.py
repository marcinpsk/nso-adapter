# SPDX-License-Identifier: Apache-2.0
"""NSO device actions — sync-from, compare-config, check-sync, connect.

Each function is a thin wrapper around the RESTCONF POST calls documented in
docs/nso-adapter.md §3.
"""

from __future__ import annotations

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


async def connect(client: NsoClient, device_name: str) -> dict:
    """POST connect — connectivity test to the device."""
    url = f"{client._base}{_DEVICE_BASE.format(name=device_name)}/connect"
    async with client._client(timeout=client._action_timeout) as c:
        resp = await c.post(url)
        resp.raise_for_status()
        return resp.json().get("tailf-ncs:output", {})
