# SPDX-License-Identifier: Apache-2.0
"""NetBox async client."""
from __future__ import annotations

import httpx
import structlog

logger = structlog.get_logger(__name__)


class NetboxClient:
    def __init__(self, url: str, token: str, timeout: float = 30.0) -> None:
        self._base = url.rstrip("/")
        self._token = token
        self._timeout = timeout

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            headers={"Authorization": f"Token {self._token}", "Content-Type": "application/json"},
            timeout=self._timeout,
        )

    async def get_interface(self, netbox_device_id: int, interface_name: str) -> dict | None:
        """Return a NetBox interface object or None."""
        url = f"{self._base}/api/dcim/interfaces/"
        params = {"device_id": netbox_device_id, "name": interface_name}
        async with self._client() as c:
            resp = await c.get(url, params=params)
            resp.raise_for_status()
            results = resp.json().get("results", [])
            return results[0] if results else None

    async def patch_interface(self, interface_id: int, payload: dict) -> dict:
        url = f"{self._base}/api/dcim/interfaces/{interface_id}/"
        async with self._client() as c:
            resp = await c.patch(url, json=payload)
            resp.raise_for_status()
            return resp.json()

    async def create_interface(self, payload: dict) -> dict:
        url = f"{self._base}/api/dcim/interfaces/"
        async with self._client() as c:
            resp = await c.post(url, json=payload)
            resp.raise_for_status()
            return resp.json()
