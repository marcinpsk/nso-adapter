# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Reconcile the topology interfaces the attribute sync's cfg.port feed never sees.

The dedicated creation owner for the LAG parents, logical channels/SAPs and
loopback/system interfaces.

Without these in NetBox, Nokia SR OS bound_port correlation (IS-IS and
interface-IP) resolves nothing: IS-IS interfaces are logical router-interfaces
(``LAG99:10``) bound to a physical/LAG port (``lag-99:10``), and the plugin
keys correlation on that ``bound_port`` string — so the NetBox interface must be
named ``lag-99:10``, not the logical name.

This step unions four DB-resident sources (each refreshed by its own poll job)
and feeds the union to ``bulk_ensure_interfaces``:

  1. cfg.port physical ports        — DbInterface.name (attribute sync)
  2. bound_ports                    — DeviceIsisInterface/InterfaceIpAddress.bound_port
  3. LAG parents (members-or-ref'd) — LagInterface (+ LagMember)
  4. loopback/system/dotted units   — IS-IS/IP interface_name with bound_port=None

See docs/nokia-lag-channel-modeling-plan.md §6 (decisions LOCKED 2026-06-02).
The plugin reconcilers stay reference-only — they never create interfaces.
"""

from __future__ import annotations

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.bindings.netbox.mapper import bulk_ensure_interfaces
from nso_adapter.store.models import (
    DbInterface,
    Device,
    DeviceIsisInterface,
    InterfaceIpAddress,
    LagInterface,
    LagMember,
)

logger = structlog.get_logger(__name__)


async def ensure_topology_interfaces(
    db: AsyncSession,
    device: Device,
    nb_client,  # type: ignore[annotation-unchecked]
) -> dict[str, int]:
    """Ensure NetBox holds the LAG/channel/loopback interfaces bound_port needs.

    Reads the adapter's read-mirror tables (populated by the IS-IS, interface-IP
    and lag-topology poll jobs) plus the attribute-sync DbInterface rows, unions
    them per the locked decisions, and bulk-ensures the result in NetBox.

    Returns the name→netbox_interface_id map (empty when there is no NetBox
    binding or nothing to ensure). Idempotent — bulk_ensure skips existing rows.
    """
    if nb_client is None or not device.netbox_device_id:
        return {}

    # 1. cfg.port — physical ports + any port-level LAG (e.g. lag-99). These are
    #    already ensured by the attribute sync; we include them so channel/SAP
    #    bases (lag-99, 1/1/c22/1) are present when their units get parented.
    cfg_ports = set(
        (await db.execute(select(DbInterface.name).where(DbInterface.device_id == device.id))).scalars().all()
    )

    # 2. bound_ports from IS-IS + interface-IP — channels (lag-99:10), SAPs
    #    (1/1/c22/1:4090) and LAG refs (lag-67). The canonical NetBox name.
    isis_bp = (
        (
            await db.execute(
                select(DeviceIsisInterface.bound_port).where(
                    DeviceIsisInterface.device_id == device.id,
                    DeviceIsisInterface.bound_port.is_not(None),
                )
            )
        )
        .scalars()
        .all()
    )
    ip_bp = (
        (
            await db.execute(
                select(InterfaceIpAddress.bound_port).where(
                    InterfaceIpAddress.device_id == device.id,
                    InterfaceIpAddress.bound_port.is_not(None),
                )
            )
        )
        .scalars()
        .all()
    )
    bound_ports = {bp for bp in (*isis_bp, *ip_bp) if bp}

    # 3. LAG parents from lag-topology — decision 2: create only when the LAG has
    #    ≥1 member OR is referenced by some interface's bound_port. Skip pure-empty
    #    unreferenced LAGs.
    lag_rows = (await db.execute(select(LagInterface).where(LagInterface.device_id == device.id))).scalars().all()
    lag_ids = [lag.id for lag in lag_rows]
    member_lag_ids: set[int] = set()
    if lag_ids:
        member_lag_ids = set(
            (await db.execute(select(LagMember.lag_interface_id).where(LagMember.lag_interface_id.in_(lag_ids))))
            .scalars()
            .all()
        )
    lag_names = {lag.name for lag in lag_rows if lag.id in member_lag_ids or lag.name in bound_ports}

    # 4. Name-only loopback/system/dotted-unit interfaces (bp=None) from IS-IS/IP.
    #    Decision 4: create system/lo0/Management-lo0/ae108.0/lag103.0 by name so
    #    their IS-IS NET / router-id rows correlate. Decision 3: SKIP the bp=None
    #    `lagXX:0`-style unbound shells (colon form) — no port/no IP on the device,
    #    so creating one would assert a non-existent binding.
    isis_unbound = (
        (
            await db.execute(
                select(DeviceIsisInterface.interface_name).where(
                    DeviceIsisInterface.device_id == device.id,
                    DeviceIsisInterface.bound_port.is_(None),
                )
            )
        )
        .scalars()
        .all()
    )
    ip_unbound = (
        (
            await db.execute(
                select(InterfaceIpAddress.interface_name).where(
                    InterfaceIpAddress.device_id == device.id,
                    InterfaceIpAddress.bound_port.is_(None),
                )
            )
        )
        .scalars()
        .all()
    )
    name_only: set[str] = set()
    skipped_unbound: list[str] = []
    for name in {n for n in (*isis_unbound, *ip_unbound) if n}:
        if ":" in name:
            skipped_unbound.append(name)
            continue
        name_only.add(name)

    names = cfg_ports | bound_ports | lag_names | name_only
    if not names:
        return {}

    ensured = await bulk_ensure_interfaces(nb_client, device.netbox_device_id, sorted(names))
    logger.info(
        "topology_interfaces.ensured",
        device_id=device.id,
        nso_device_name=device.nso_device_name,
        cfg_ports=len(cfg_ports),
        bound_ports=len(bound_ports),
        lag_parents=len(lag_names),
        name_only=len(name_only),
        skipped_unbound=len(skipped_unbound),
        total=len(names),
    )
    if skipped_unbound:
        logger.debug(
            "topology_interfaces.skipped_unbound",
            device_id=device.id,
            names=sorted(skipped_unbound),
        )
    return ensured
