# SPDX-License-Identifier: Apache-2.0
"""NetBox async client."""

from __future__ import annotations

import httpx
import structlog

logger = structlog.get_logger(__name__)

# Max rows per bulk request. A single multi-thousand-row body makes NetBox drop
# the connection, so requests are split into serial batches. PATCH is much more
# expensive server-side than POST (per-row change-logging + cable/LAG recompute),
# so it uses a smaller batch.
_BULK_CREATE_CHUNK = 100
_BULK_PATCH_CHUNK = 50
# Bulk writes can be slow on a busy / DEBUG NetBox; give them headroom beyond the
# default per-call timeout.
_BULK_TIMEOUT = 120.0


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
        """Bulk-create interfaces, chunked into batches of ``_BULK_CREATE_CHUNK``.

        A single huge list body overwhelms NetBox (the server drops the
        connection mid-request), so requests are split into batches sent
        **serially**. Each batch is all-or-nothing per NetBox; on a 400 the
        positional error list is used to drop offending rows and retry the
        remainder once, so one rejected name can't strand its batch. Returns the
        concatenated list of created objects (each has ``id`` and ``name``).
        """
        return await self._bulk_chunked("POST", payloads, chunk=_BULK_CREATE_CHUNK, label="bulk_create")

    async def bulk_patch_interfaces(self, payloads: list[dict]) -> list[dict]:
        """Bulk-update interfaces (each item needs ``id``), chunked + serial.

        Same batching and all-or-nothing/row-drop semantics as bulk create.
        """
        return await self._bulk_chunked("PATCH", payloads, chunk=_BULK_PATCH_CHUNK, label="bulk_patch")

    async def _bulk_chunked(self, method: str, payloads: list[dict], *, chunk: int, label: str) -> list[dict]:
        """Send *payloads* in serial batches, isolating failures per batch.

        A batch that errors (timeout, 5xx, unexpected 400 body) is logged and
        skipped — it must NOT abandon the remaining batches. Returns whatever was
        successfully written.
        """
        out: list[dict] = []
        for start in range(0, len(payloads), chunk):
            batch = payloads[start : start + chunk]
            try:
                out.extend(await self._bulk_one(method, batch, label=label))
            except Exception as exc:
                logger.warning(
                    f"netbox.{label}.batch_failed",
                    batch_start=start,
                    batch_size=len(batch),
                    error=str(exc) or type(exc).__name__,
                )
        return out

    async def _bulk_one(self, method: str, batch: list[dict], *, label: str) -> list[dict]:
        """Send one batch; on a 400 with positional errors, drop bad rows and retry once."""
        if not batch:
            return []
        url = f"{self._base}/api/dcim/interfaces/"
        resp = await self._client().request(method, url, json=batch, timeout=_BULK_TIMEOUT)
        if resp.status_code == 400:
            errors = resp.json()
            # errors is a positional list aligned to batch; {} == row ok.
            bad = {i for i, e in enumerate(errors) if e} if isinstance(errors, list) else set(range(len(batch)))
            for i in bad:
                logger.warning(
                    f"netbox.{label}.row_rejected",
                    name=batch[i].get("name"),
                    id=batch[i].get("id"),
                    error=errors[i] if isinstance(errors, list) else "unknown",
                )
            good = [p for i, p in enumerate(batch) if i not in bad]
            if not good:
                return []
            resp = await self._client().request(method, url, json=good, timeout=_BULK_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
