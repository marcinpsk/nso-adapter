# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""IS-IS interface refresh — reads NSO oper-data and upserts the DB.

Entry points:
- refresh_isis_interfaces_for_device() — called on-demand by scheduler
- handle_isis_interface_change()       — placeholder for future SSE hook
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.isis_canon import isis_level
from nso_adapter.nso.client import NsoClient
from nso_adapter.store.models import Device, DeviceIsisInterface, DeviceIsisProcess

logger = structlog.get_logger(__name__)

# Cross-vendor scalar leaves mirrored 1:1 from the network-state-export
# isis-interface process/interface nodes onto the typed DB columns. Hyphenated
# export key → snake_case column.
_PROC_SCALAR_KEYS = (
    "spf-initial-wait",
    "spf-max-wait",
    "lsp-initial-wait",
    "lsp-max-wait",
    "lsp-lifetime",
    "lsp-refresh-interval",
    "lsp-mtu",
    "overload-on-startup",
    "overload-timeout",
    "te-enabled",
    "suppress-attached-bit",
    "ignore-attached-bit",
    "fast-reroute",
    "microloop-avoidance",
    "distance",
    "maximum-paths",
    "reference-bandwidth",
)
_IFACE_SCALAR_KEYS = (
    "frr-enabled",
    "frr-protection",
    "csnp-interval",
    "retransmit-interval",
    "lsp-interval",
    "mesh-group",
)


def _scalar_cols(src: dict, keys: tuple[str, ...]) -> dict:
    """Pick present keys from *src*, returning a {snake_case: value} dict."""
    out: dict = {}
    for k in keys:
        if k in src and src[k] is not None:
            out[k.replace("-", "_")] = src[k]
    return out


def _settings_dict(src: dict) -> dict | None:
    """Collapse the export ``setting`` list ([{key,value}]) into a flat dict."""
    raw = src.get("setting")
    if not raw:
        return None
    out = {s["key"]: s.get("value") for s in raw if isinstance(s, dict) and s.get("key")}
    return out or None


async def _upsert_isis_data(
    db: AsyncSession,
    device: Device,
    processes: list[dict],
    interfaces: list[dict],
    refresh_source: str,
) -> None:
    """Full-replace: delete existing IS-IS rows for *device*, then insert fresh ones."""
    await db.execute(delete(DeviceIsisProcess).where(DeviceIsisProcess.device_id == device.id))
    await db.execute(delete(DeviceIsisInterface).where(DeviceIsisInterface.device_id == device.id))

    now = datetime.now(UTC).replace(tzinfo=None)

    # First-wins in-refresh dedup: a duplicate identity tuple in the export would otherwise
    # IntegrityError on commit and roll back the whole full-replace (uq_deviceisisprocess_identity
    # is device_id+process_tag; the interface constraint adds af).
    seen_procs: set[str] = set()
    seen_ifaces: set[tuple[str, str]] = set()

    for proc in processes:
        proc_tag = proc.get("process-tag", "")
        if proc_tag in seen_procs:
            continue
        seen_procs.add(proc_tag)
        db.add(
            DeviceIsisProcess(
                device_id=device.id,
                process_tag=proc_tag,
                net=proc.get("net"),
                is_type=isis_level(proc.get("is-type")),
                metric_style=proc.get("metric-style"),
                overload_bit=proc.get("overload-bit"),
                area_auth_type=proc.get("area-auth-type"),
                area_auth_present=proc.get("area-auth-present"),
                area_auth_key=proc.get("area-auth-key"),
                domain_auth_type=proc.get("domain-auth-type"),
                domain_auth_present=proc.get("domain-auth-present"),
                domain_auth_key=proc.get("domain-auth-key"),
                settings=_settings_dict(proc),
                levels=proc.get("level") or None,
                segment_routing=proc.get("segment-routing") or None,
                flex_algos=proc.get("flex-algo") or None,
                srv6_locators=proc.get("srv6-locator") or None,
                last_refreshed_at=now,
                refresh_source=refresh_source,
                **_scalar_cols(proc, _PROC_SCALAR_KEYS),
            )
        )

    for iface in interfaces:
        iface_name = iface.get("interface-name", "")
        af = iface.get("af", "")
        if not iface_name or not af:
            continue
        if (iface_name, af) in seen_ifaces:
            continue
        seen_ifaces.add((iface_name, af))
        db.add(
            DeviceIsisInterface(
                device_id=device.id,
                interface_name=iface_name,
                af=af,
                process_tag=iface.get("process-tag", ""),
                circuit_type=isis_level(iface.get("circuit-type")),
                network_type=iface.get("network-type"),
                metric=iface.get("metric"),
                passive=bool(iface.get("passive", False)),
                bound_port=iface.get("bound-port") or None,
                hello_auth_type=iface.get("hello-auth-type") or None,
                hello_auth_present=iface.get("hello-auth-present"),
                bfd_enabled=iface.get("bfd-enabled"),
                settings=_settings_dict(iface),
                levels=iface.get("level") or None,
                prefix_sids=iface.get("prefix-sid") or None,
                last_refreshed_at=now,
                refresh_source=refresh_source,
                **_scalar_cols(iface, _IFACE_SCALAR_KEYS),
            )
        )

    await db.commit()


async def refresh_isis_interfaces_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "poll",
) -> bool:
    """Read IS-IS oper-data for *device* from NSO and upsert DB rows.

    Returns True when the read succeeded (or there was nothing to read); False when the
    NSO read failed and the last-known rows were left untouched (a degraded surface).
    """
    if not device.nso_device_name:
        logger.debug("isis.refresh.skipped", device_id=device.id, reason="no_nso_device_name")
        return True

    try:
        entry = await nso_client.get_isis_interfaces(device.nso_device_name)
    except Exception as exc:
        logger.warning("isis.refresh.nso_error", device_id=device.id, error=repr(exc))
        return False

    processes = entry.get("process", []) if entry else []
    interfaces = entry.get("interface", []) if entry else []
    await _upsert_isis_data(db, device, processes, interfaces, refresh_source)
    logger.info(
        "isis.refresh.done",
        device_id=device.id,
        device_name=device.nso_device_name,
        process_count=len(processes),
        interface_count=len(interfaces),
        refresh_source=refresh_source,
    )
    return True


async def handle_isis_interface_change(
    db: AsyncSession,
    nso_device_name: str,
    nso_client: NsoClient,
) -> None:
    """Handle an SSE notification that IS-IS config changed for *nso_device_name*.

    Finds the Device row, then delegates to refresh_isis_interfaces_for_device.
    """
    result = await db.execute(select(Device).where(Device.nso_device_name == nso_device_name))
    device = result.scalar_one_or_none()
    if device is None:
        logger.debug("isis.sse.unknown_device", nso_device_name=nso_device_name)
        return

    await refresh_isis_interfaces_for_device(db, device, nso_client, refresh_source="sse")
