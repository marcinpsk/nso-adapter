# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Full-replace removal helper shared by the per-service intent PUT endpoints.

A merge-PATCH apply never drops a list entry you omit, and a node-level RESTCONF
DELETE 404s on empty-string list keys. So when an intent PUT (full-replace store)
deletes rows, the device keeps the orphaned config. This helper re-asserts the FULL
remaining desired state via a PUT-replace of the keyed service instance
(``apply_callable(..., replace=True)``), so FASTMAP reverts the removed entries.
"""

from __future__ import annotations

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)


async def replace_on_removal(db: AsyncSession, device, removed, store_model, apply_callable) -> bool:
    """If *removed* is truthy, PUT-replace the service with the remaining accepted rows.

    *device* is the store Device row (needs ``nso_instance`` + ``nso_device_name``).
    *store_model* is the intent table; *apply_callable* is the matching
    ``apply_*`` coroutine taking ``(client, device_name, rows, replace=True)``.
    Best-effort: logs and returns False on failure so the request still succeeds.
    """
    if not removed:
        return False
    from nso_adapter.core.importer import get_nso_client

    remaining = (
        await db.execute(
            select(store_model).where(
                store_model.device_id == device.id, store_model.accepted_at.is_not(None)
            )
        )
    ).scalars().all()
    try:
        client = get_nso_client(device.nso_instance)
        await apply_callable(client, device.nso_device_name, remaining, replace=True)
        return True
    except Exception as exc:  # noqa: BLE001 — never fail the request on removal propagation
        logger.error(
            "removal.replace_failed",
            device_id=device.id,
            model=store_model.__name__,
            error=repr(exc),
        )
        return False
