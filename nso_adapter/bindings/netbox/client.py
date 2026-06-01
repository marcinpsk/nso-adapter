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
        # Persistent pooled client (keep-alive). Built lazily so construction
        # stays sync; reused across all calls instead of one connection per call.
        self._http: httpx.AsyncClient | None = None

    def _client(self) -> httpx.AsyncClient:
        """Return the shared pooled AsyncClient, creating it on first use."""
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                headers={
                    "Authorization": f"Token {self._token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=self._timeout,
            )
        return self._http

    async def aclose(self) -> None:
        """Close the pooled client (call on adapter shutdown)."""
        if self._http is not None and not self._http.is_closed:
            await self._http.aclose()

    async def get_interface(self, netbox_device_id: int, interface_name: str) -> dict | None:
        """Return a NetBox interface object or None."""
        url = f"{self._base}/api/dcim/interfaces/"
        params = {"device_id": netbox_device_id, "name": interface_name}
        resp = await self._client().get(url, params=params)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        return results[0] if results else None

    async def list_interfaces(self, netbox_device_id: int) -> list[dict]:
        """Return ALL interfaces for a device in one paginated sweep.

        Used by sync to build a name→object map once, instead of a GET per
        interface. Follows ``next`` links until exhausted.
        """
        url: str | None = f"{self._base}/api/dcim/interfaces/"
        params: dict | None = {"device_id": netbox_device_id, "limit": 500}
        out: list[dict] = []
        client = self._client()
        while url:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            body = resp.json()
            out.extend(body.get("results", []))
            url = body.get("next")
            params = None  # the `next` URL already carries query params
        return out

    async def patch_interface(self, interface_id: int, payload: dict) -> dict:
        url = f"{self._base}/api/dcim/interfaces/{interface_id}/"
        resp = await self._client().patch(url, json=payload)
        resp.raise_for_status()
        return resp.json()

    async def create_interface(self, payload: dict) -> dict:
        url = f"{self._base}/api/dcim/interfaces/"
        resp = await self._client().post(url, json=payload)
        resp.raise_for_status()
        return resp.json()

    async def bulk_create_interfaces(self, payloads: list[dict]) -> list[dict]:
        """Bulk-create interfaces via a list POST body.

        NetBox bulk create is all-or-nothing: one invalid row → HTTP 400 and the
        whole batch rolls back, with a positional error list in the body. On 400
        we drop the offending rows (by position) and retry the remainder ONCE, so
        a single rejected name can't strand an otherwise-valid batch. Returns the
        list of created objects (each has ``id`` and ``name``).
        """
        if not payloads:
            return []
        url = f"{self._base}/api/dcim/interfaces/"
        resp = await self._client().post(url, json=payloads)
        if resp.status_code == 400:
            errors = resp.json()
            # errors is a positional list aligned to payloads; {} == row ok.
            bad = {i for i, e in enumerate(errors) if e} if isinstance(errors, list) else set(range(len(payloads)))
            good = [p for i, p in enumerate(payloads) if i not in bad]
            for i in bad:
                logger.warning(
                    "netbox.bulk_create.row_rejected",
                    name=payloads[i].get("name"),
                    error=errors[i] if isinstance(errors, list) else "unknown",
                )
            if not good:
                return []
            resp = await self._client().post(url, json=good)
        resp.raise_for_status()
        return resp.json()

    async def bulk_patch_interfaces(self, payloads: list[dict]) -> list[dict]:
        """Bulk-update interfaces via a list PATCH body (each item needs ``id``).

        Same all-or-nothing semantics as bulk create; on 400 drop offending rows
        and retry the remainder once.
        """
        if not payloads:
            return []
        url = f"{self._base}/api/dcim/interfaces/"
        resp = await self._client().patch(url, json=payloads)
        if resp.status_code == 400:
            errors = resp.json()
            bad = {i for i, e in enumerate(errors) if e} if isinstance(errors, list) else set(range(len(payloads)))
            good = [p for i, p in enumerate(payloads) if i not in bad]
            for i in bad:
                logger.warning(
                    "netbox.bulk_patch.row_rejected",
                    id=payloads[i].get("id"),
                    error=errors[i] if isinstance(errors, list) else "unknown",
                )
            if not good:
                return []
            resp = await self._client().patch(url, json=good)
        resp.raise_for_status()
        return resp.json()
