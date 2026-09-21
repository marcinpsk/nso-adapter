# SPDX-License-Identifier: Apache-2.0
"""NetBox async client."""

from __future__ import annotations

import httpx
import structlog

from nso_adapter.nso.client import failure_detail

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


def rejection_detail(body: object) -> str:
    """Classify a NetBox rejection body for a log record.

    NetBox repeats the submitted value in its validation messages, and a bulk row carries
    an interface name. The response keys can also come from NetBox, so only counts travel.
    """
    if isinstance(body, dict):
        return f"fields: {len(body)}" if body else "fields: none"
    if isinstance(body, list):
        return f"errors: {len(body)}"
    return "unparsed"


def _row_fields(position: int, payload: dict) -> dict[str, int]:
    """Row context for a rejected bulk row: its position, plus its interface id on a PATCH."""
    fields = {"payload_index": position}
    interface_id = payload.get("id")
    if isinstance(interface_id, int):
        fields["netbox_interface_id"] = interface_id
    return fields


class NetboxClient:
    def __init__(self, url: str, token: str, timeout: float = 30.0, verify: str | bool = True) -> None:
        self._base = url.rstrip("/")
        self._token = token
        self._timeout = timeout
        # TLS verification: True (system CAs), False (skip), or a CA-bundle path so a
        # private-CA NetBox endpoint can be pinned (mirrors the NSO/SSE clients).
        self._verify = verify
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
                    # Marks every write as adapter-origin (an import/apply, never an
                    # operator intent edit). The netbox-nso-plugin's Decision-G
                    # post_save signal skips intent promotion/push when it sees this,
                    # so imports aren't mis-read as operator accepts.
                    "X-NSO-Adapter-Import": "1",
                },
                timeout=self._timeout,
                verify=self._verify,
            )
        return self._http

    async def aclose(self) -> None:
        """Close the pooled client (call on adapter shutdown)."""
        if self._http is not None and not self._http.is_closed:
            await self._http.aclose()

    async def notify_sync_complete(self, netbox_device_id: int) -> None:
        """Tell the netbox-nso-plugin a device sync finished so it refreshes its cache.

        The plugin reconciles adapter state into its NSO*State display tables off the
        request path (a background job) instead of doing it — slowly, with
        write-on-read — every time an operator opens the NSO tab. Best-effort: the
        caller swallows errors, since the plugin also reconciles on the next cycle.
        """
        url = f"{self._base}/api/plugins/nso/sync-complete/"
        resp = await self._client().post(url, json={"netbox_device_id": netbox_device_id})
        resp.raise_for_status()

    async def notify_provision_complete(self, provision_job_id: int) -> None:
        """Tell the netbox-nso-plugin a device-provision job finished so it advances the onboard row.

        The plugin advances the gated NSODeviceManagement row off the dashboard-poll path — to
        ready (→ its signal maps/scopes/syncs) or provision_failed. Best-effort: the caller swallows
        errors, and the plugin's device-tab self-heal + hourly sweep still catch a missed callback.
        """
        url = f"{self._base}/api/plugins/nso/provision-complete/"
        resp = await self._client().post(url, json={"provision_job_id": provision_job_id})
        resp.raise_for_status()

    async def device_exists(self, netbox_device_id: int) -> bool:
        """Return whether the NetBox device still exists.

        A 404 maps to ``False`` (device removed from NetBox), distinct from a transport
        error, which still raises.
        """
        resp = await self._client().get(f"{self._base}/api/dcim/devices/{netbox_device_id}/")
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        return True

    async def create_journal_entry(self, netbox_device_id: int, comments: str, kind: str = "info") -> None:
        """POST a JournalEntry onto a NetBox device — an operator-visible audit trail."""
        url = f"{self._base}/api/extras/journal-entries/"
        resp = await self._client().post(
            url,
            json={
                "assigned_object_type": "dcim.device",
                "assigned_object_id": netbox_device_id,
                "kind": kind,
                "comments": comments,
            },
        )
        resp.raise_for_status()

    async def get_interface(self, netbox_device_id: int, interface_name: str) -> dict | None:
        """Return a NetBox interface object or None."""
        url = f"{self._base}/api/dcim/interfaces/"
        params: dict[str, str | int] = {"device_id": netbox_device_id, "name": interface_name}
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

    async def bulk_create_interfaces(self, payloads: list[dict], *, netbox_device_id: int) -> list[dict]:
        """Bulk-create interfaces, chunked into batches of ``_BULK_CREATE_CHUNK``.

        A single huge list body overwhelms NetBox (the server drops the
        connection mid-request), so requests are split into batches sent
        **serially**. Each batch is all-or-nothing per NetBox; on a 400 the
        positional error list is used to drop offending rows and retry the
        remainder once, so one rejected name can't strand its batch. Returns the
        concatenated list of created objects (each has ``id`` and ``name``).
        """
        return await self._bulk_chunked(
            "POST",
            payloads,
            netbox_device_id=netbox_device_id,
            chunk=_BULK_CREATE_CHUNK,
            label="bulk_create",
        )

    async def bulk_patch_interfaces(self, payloads: list[dict], *, netbox_device_id: int) -> list[dict]:
        """Bulk-update interfaces (each item needs ``id``), chunked + serial.

        Same batching and all-or-nothing/row-drop semantics as bulk create.
        """
        return await self._bulk_chunked(
            "PATCH",
            payloads,
            netbox_device_id=netbox_device_id,
            chunk=_BULK_PATCH_CHUNK,
            label="bulk_patch",
        )

    async def _bulk_chunked(
        self,
        method: str,
        payloads: list[dict],
        *,
        netbox_device_id: int,
        chunk: int,
        label: str,
    ) -> list[dict]:
        """Send *payloads* in serial batches, isolating failures per batch.

        A batch that errors (timeout, 5xx, unexpected 400 body) is logged and
        skipped — it must NOT abandon the remaining batches. Returns whatever was
        successfully written.
        """
        out: list[dict] = []
        for start in range(0, len(payloads), chunk):
            rows = list(enumerate(payloads[start : start + chunk], start))
            try:
                out.extend(await self._bulk_one(method, rows, netbox_device_id=netbox_device_id, label=label))
            except Exception as exc:
                logger.warning(
                    f"netbox.{label}.batch_failed",
                    netbox_device_id=netbox_device_id,
                    batch_start=start,
                    batch_size=len(rows),
                    error=failure_detail(exc),
                )
        return out

    async def _bulk_one(
        self,
        method: str,
        rows: list[tuple[int, dict]],
        *,
        netbox_device_id: int,
        label: str,
    ) -> list[dict]:
        """Send one batch; on a 400, isolate the offending row(s) and write the rest.

        NetBox returns a *positional* error list for bulk writes ({} == row ok), so
        the flagged rows are dropped and the good remainder retried. But some 400s
        come back as a non-positional body (a dict / single error) — e.g. when a row
        fails model-level validation, such as a pre-existing out-of-range MTU on the
        target interface that makes NetBox reject ANY PATCH to it (even one that only
        touches description/enabled). A non-positional body hides *which* row is bad,
        so the batch is bisected recursively until the culprit is a single row, which
        is logged and dropped. This keeps one poison row from stranding its innocent
        batch-mates (the device-27 'stuck 23' bug).
        """
        if not rows:
            return []
        url = f"{self._base}/api/dcim/interfaces/"
        resp = await self._client().request(
            method,
            url,
            json=[payload for _, payload in rows],
            timeout=_BULK_TIMEOUT,
        )
        if resp.status_code != 400:
            resp.raise_for_status()
            return resp.json()

        # 400 — try to parse a positional error list aligned to the batch.
        try:
            errors = resp.json()
        except Exception:
            errors = None

        if isinstance(errors, list) and len(errors) == len(rows):
            bad = {i for i, e in enumerate(errors) if e}
            if bad:
                for i in sorted(bad):
                    position, payload = rows[i]
                    logger.warning(
                        f"netbox.{label}.row_rejected",
                        netbox_device_id=netbox_device_id,
                        **_row_fields(position, payload),
                        error=rejection_detail(errors[i]),
                    )
                good = [row for i, row in enumerate(rows) if i not in bad]
                return (
                    await self._bulk_one(method, good, netbox_device_id=netbox_device_id, label=label) if good else []
                )
            # Positional list but nothing flagged though status==400 — fall through
            # to bisection rather than re-sending the identical batch (infinite loop).

        # Non-positional 400 (or unflagged): can't tell which row is bad from the body.
        if len(rows) == 1:
            position, payload = rows[0]
            logger.warning(
                f"netbox.{label}.row_rejected",
                netbox_device_id=netbox_device_id,
                **_row_fields(position, payload),
                error=rejection_detail(errors),
            )
            return []
        # Bisect to isolate the culprit; each half is retried independently.
        mid = len(rows) // 2
        left = await self._bulk_one(method, rows[:mid], netbox_device_id=netbox_device_id, label=label)
        right = await self._bulk_one(method, rows[mid:], netbox_device_id=netbox_device_id, label=label)
        return left + right
