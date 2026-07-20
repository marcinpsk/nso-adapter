# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""The shared read-mirror refresh engine (READSEM S1).

One executor replaces the ~18 copy-pasted ``refresh_<family>_for_device`` bodies. Each family
declares a :class:`FamilySpec` (getter + empty-policy + extractor + materializer); the engine
does the invariant part uniformly — skip unmapped devices, read once, classify the result into
the :data:`~nso_adapter.nso.read_outcome.ReadOutcome` vocabulary, and drive the mirror action:

* **Present** → materialize the extracted rows (a present-but-empty read replaces → clears).
* **AbsentAuthoritative** → materialize an empty row set (clear; the device genuinely has none).
* **Unavailable** → keep the last-known rows and return ``False`` (a degraded surface).

The family's *writes* stay family-owned in its materializer (bgp's multi-table flush,
vlan/switchport diff-by-key, etc.); only the empty/error *semantics* are centralized here. The
returned ``bool`` preserves the legacy ``refresh_*_for_device`` contract (``True`` = read
succeeded or nothing-to-read; ``False`` = read failed, rows untouched) so existing callers,
``_run_surfaces``, and monkeypatching tests keep working unchanged.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.nso.client import NsoClient
from nso_adapter.nso.read_outcome import (
    AbsentAuthoritative,
    EmptyPolicy,
    Present,
    Unavailable,
    UnavailableReason,
    classify_read,
)
from nso_adapter.store.models import Device

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class FamilySpec:
    """Declarative description of one read-mirror family (the policy-table row).

    * ``name`` — the surface name used in logs and degraded-surface lists (e.g. ``"static_route"``).
    * ``empty_policy`` — how a ``None`` read is interpreted (see :class:`EmptyPolicy`).
    * ``getter`` — ``(client, device_name) -> Awaitable[dict | None]``; the existing
      ``NsoClient.get_*`` coroutine (so mocks that patch the getter keep working).
    * ``extract`` — ``data -> list[dict]``; pull the family's row list out of the read entry.
    * ``materialize`` — ``(db, device, rows, refresh_source) -> Awaitable[None]``; the
      family-owned full-replace/upsert that also commits.
    """

    name: str
    empty_policy: EmptyPolicy
    getter: Callable[[NsoClient, str], Awaitable[dict | None]]
    extract: Callable[[dict], list]
    materialize: Callable[[AsyncSession, Device, list, str], Awaitable[None]]


async def run_family_refresh(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    spec: FamilySpec,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Refresh one family's mirror for one device via the shared engine.

    Behaviour-equivalent to the hand-written ``refresh_<family>_for_device`` it replaces:
    returns ``True`` on a successful read (including an authoritative clear or an intentional
    skip) and ``False`` when the read was unavailable and the last-known rows were left intact.
    """
    if not device.nso_device_name:
        logger.debug(f"{spec.name}.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return True

    outcome = await classify_read(
        lambda: spec.getter(nso_client, device.nso_device_name),
        spec.empty_policy,
    )

    if isinstance(outcome, Present):
        rows = spec.extract(outcome.data)
        await spec.materialize(db, device, rows, refresh_source)
        logger.info(
            f"{spec.name}.refresh.done",
            device_id=device.id,
            device_name=device.nso_device_name,
            row_count=len(rows),
            freshness=outcome.freshness.value,
            refresh_source=refresh_source,
        )
        return True

    if isinstance(outcome, AbsentAuthoritative):
        await spec.materialize(db, device, [], refresh_source)
        logger.info(
            f"{spec.name}.refresh.cleared",
            device_id=device.id,
            device_name=device.nso_device_name,
            refresh_source=refresh_source,
        )
        return True

    # Unavailable — keep the last-known rows in every case.
    assert isinstance(outcome, Unavailable)
    if outcome.reason is UnavailableReason.not_authoritative:
        # A keep-on-None inventory family (present policy) got a 404: the export isn't serving
        # this device for this family (unsupported NED / unknown / not-ready). That's an EXPECTED
        # absence, not a read failure — keep the rows and report success (NOT a degraded surface),
        # so it never flips the device to `partial` on every poll.
        logger.info(
            f"{spec.name}.refresh.not_authoritative",
            device_id=device.id,
            device_name=device.nso_device_name,
            refresh_source=refresh_source,
        )
        return True

    logger.warning(
        f"{spec.name}.refresh.unavailable",
        device_id=device.id,
        device_name=device.nso_device_name,
        reason=outcome.reason.value,
        detail=outcome.detail,
    )
    return False
