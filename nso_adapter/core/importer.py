# SPDX-License-Identifier: Apache-2.0
"""Device sync importer — NSO → adapter DB → NetBox.

Sync flow (docs/nso-adapter.md §7):
  1. sync-from on NSO (refresh CDB from live device)
  2. Read managed attributes per interface
  3. Compute per-attribute sync_state vs stored netbox_value
  4. NetBox binding writes NSO value onto dcim.Interface
  5. Persist interface_attr_state, update device.last_sync_*
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import NamedTuple

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.config import get_config
from nso_adapter.core.refresh_engine import (
    _action_semaphore,
    classify_envelope_family_read,
    run_family_refresh_from_outcome,
    run_family_refresh_from_section,
)
from nso_adapter.core.sync_state import compute_sync_state
from nso_adapter.domain.models import Interface, InterfaceAttr
from nso_adapter.nso import actions as nso_actions
from nso_adapter.nso.client import NsoClient, NsoExportUnavailableError
from nso_adapter.nso.read_outcome import (  # noqa: F401 — Present used below
    EmptyPolicy,
    Present,
    Unavailable,
    UnavailableReason,
    classify_envelope_section,
)
from nso_adapter.nso.shape import as_list
from nso_adapter.store.models import (
    DbInterface,
    Device,
    InterfaceAttrState,
    InterfaceIntent,
    LastSyncStatus,
    ManagedScope,
    MappingStatus,
    SyncState,
)

logger = structlog.get_logger(__name__)

_nso_clients: dict[str, NsoClient] = {}
_netbox_client = None  # set at startup via set_netbox_client


def _utcnow() -> datetime:
    """Naive-UTC now — the timestamp columns are timezone-naive (datetime.utcnow() is deprecated)."""
    return datetime.now(UTC).replace(tzinfo=None)


def _attrs_to_interface_list(data: dict | None) -> list[Interface]:
    """Convert NSO package interface-attributes oper-data to domain Interface objects.

    Skips malformed entries (missing ``interface-name``) with a warning log.
    Returns an empty list if *data* is None or has no ``interface`` key.
    """
    if data is None:
        return []
    result = []
    for entry in as_list(data.get("interface")):
        name = entry.get("interface-name")
        if not name:
            logger.warning("interface-attrs: skipping malformed entry (no interface-name)", entry=entry)
            continue
        result.append(
            Interface(
                name=name,
                nso=InterfaceAttr(
                    description=entry.get("description"),
                    enabled=entry.get("enabled"),
                ),
                netbox=InterfaceAttr(description=None, enabled=None),
                # M27R: pass through the logical-interface modeling fields (empty for
                # physical ports / Cisco / Junos).
                parent_binding=entry.get("parent-binding") or None,
                kind=entry.get("kind") or None,
                encap_tag=entry.get("encap-tag") or None,
                vrf=entry.get("vrf") or None,
                service=entry.get("service") or None,
            )
        )
    return result


async def _load_intent_by_attr(db: AsyncSession, interface_id: int) -> dict[str, object]:
    """Return {attribute: intent_value} for an interface from InterfaceIntent.

    InterfaceIntent is the single source of truth for deployed intent (written by
    PUT /intent, apply and the scheduler). The importer reads it here to decide
    Phase 1 vs Phase 2 — there is no separate attr_state.intent_value cache.
    """
    result = await db.execute(select(InterfaceIntent).where(InterfaceIntent.interface_id == interface_id))
    return {row.attribute: row.intent_value for row in result.scalars().all()}


def _attr_str(attr: str, value: object) -> str | None:
    """Normalise an attribute value to the canonical string used for comparison.

    Empty descriptions ("" or None) collapse to None so a blank on either side
    compares equal; ``enabled`` stays "True"/"False".
    """
    if attr == "description":
        return str(value) if value else None
    return str(value) if value is not None else None


def register_nso_client(instance_name: str, client: NsoClient) -> None:
    _nso_clients[instance_name] = client


def get_nso_client(instance_name: str) -> NsoClient:
    if instance_name not in _nso_clients:
        raise RuntimeError(f"NSO client for {instance_name!r} not registered")
    return _nso_clients[instance_name]


def set_netbox_client(client) -> None:  # type: ignore[annotation-unchecked]
    global _netbox_client
    _netbox_client = client


def get_netbox_client():
    return _netbox_client


async def _run_surfaces(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    surfaces: list[tuple[str, object]],
    refresh_source: str,
) -> list[str]:
    """Run a list of ``(name, refresh_fn)`` for one device, isolating per-surface failures.

    Returns the names of surfaces that FAILED — either the refresher raised, or it signalled
    a swallowed NSO read failure with ``return False`` (its last-known rows are now stale).
    Callers record these so the device reports ``partial`` rather than a misleading
    ``succeeded``. One failing surface (or a NED that does not serve it) must not abort the
    others. The caller commits.
    """
    failed: list[str] = []
    for name, fn in surfaces:
        try:
            ok = await fn(db, device, nso_client, refresh_source=refresh_source)
            if ok is False:
                failed.append(name)
        except Exception as exc:
            logger.warning(
                "sync.surface_refresh_failed",
                device_id=device.id,
                surface=name,
                error=repr(exc),
            )
            failed.append(name)
    return failed


def _projectable_spec(name: str):
    """Resolve a surface name to its FamilySpec (None for non-spec composites like redistribution).

    Lazy imports mirror the surface-list builders below (import cost only when used).
    """
    from nso_adapter.core.bfd import BFD_SPEC
    from nso_adapter.core.bgp import BGP_SPEC
    from nso_adapter.core.interface_ip import INTERFACE_IP_SPEC
    from nso_adapter.core.interface_mtu import INTERFACE_MTU_SPEC
    from nso_adapter.core.isis import ISIS_SPEC
    from nso_adapter.core.l2_service import L2_SERVICE_SPEC
    from nso_adapter.core.lag_config import LAG_CONFIG_SPEC
    from nso_adapter.core.lag_topology import LAG_TOPOLOGY_SPEC
    from nso_adapter.core.logging_config import LOGGING_CONFIG_SPEC
    from nso_adapter.core.ospf import OSPF_SPEC
    from nso_adapter.core.route_policy import ROUTE_POLICY_SPEC
    from nso_adapter.core.snmp import SNMP_SPEC
    from nso_adapter.core.static_route import STATIC_ROUTE_SPEC
    from nso_adapter.core.subinterface import SUBINTERFACE_SPEC
    from nso_adapter.core.svi import SVI_SPEC
    from nso_adapter.core.vlan import SWITCHPORT_SPEC, VLAN_DATABASE_SPEC

    return {
        "static_route": STATIC_ROUTE_SPEC,
        "isis": ISIS_SPEC,
        "bgp": BGP_SPEC,
        "ospf": OSPF_SPEC,
        "route_policy": ROUTE_POLICY_SPEC,
        "snmp": SNMP_SPEC,
        "logging": LOGGING_CONFIG_SPEC,
        "bfd": BFD_SPEC,
        "interface_ip": INTERFACE_IP_SPEC,
        "vlan": VLAN_DATABASE_SPEC,
        "svi": SVI_SPEC,
        "subinterface": SUBINTERFACE_SPEC,
        "interface_mtu": INTERFACE_MTU_SPEC,
        "lag_topology": LAG_TOPOLOGY_SPEC,
        "lag_config": LAG_CONFIG_SPEC,
        "l2_service": L2_SERVICE_SPEC,
        "switchport": SWITCHPORT_SPEC,
    }.get(name)


async def _fetch_projection(nso_client, device, wire_names: list[str], *, atomic: bool = False):
    """Grain-b supplier: ONE record-served doc GET + ONE heal action for the not-ready set.

    Returns ``(sections, supplier_outcome)``: on supplier failure sections is empty and
    the outcome (export_down / read_error) fans out to every family via
    ``run_family_refresh_from_outcome`` — NEVER a fabricated section (codex S3-R1 F8).
    A confirmed device absence yields ``{wire: None}`` (per-family EmptyPolicy applies).
    """
    if atomic:
        # READSEM grain c: ONE txid-bracketed build for every requested family. Output
        # sections are terminal (ok|unsupported|error); an action error (bracket
        # exhaustion, unknown device) keeps every family. 360s: the action may rebuild
        # everything up to 3x under commit churn (3 x rc1 75.6s outruns the 180s default).
        try:
            async with _action_semaphore():
                output = await nso_client.run_device_state_read(device.nso_device_name, wire_names, timeout=360.0)
        except Exception as exc:  # noqa: BLE001 — action error keeps every family
            return {}, Unavailable(UnavailableReason.read_error, detail=repr(exc))
        # Codex S3-R3 F5: a non-mapping output or atomic!=True must NEVER be materialized
        # as an atomic read — fan out read_error (keep) instead.
        if not isinstance(output, dict) or output.get("atomic") is not True:
            return {}, Unavailable(UnavailableReason.read_error, detail="action output malformed or not atomic")
        return {w: _section_or_error(output.get(w)) for w in wire_names}, None
    try:
        doc = await nso_client.get_device_state_doc(device.nso_device_name)
    except NsoExportUnavailableError as exc:
        return {}, Unavailable(UnavailableReason.export_down, detail=repr(exc))
    except Exception as exc:  # noqa: BLE001 — any supplier failure keeps every family
        return {}, Unavailable(UnavailableReason.read_error, detail=repr(exc))
    if doc is None:
        return {w: None for w in wire_names}, None
    # Codex S3-R3 F5: only the confirmed whole-doc 404 above may mean device absence. A
    # present-but-null/scalar section inside a 200 doc is MALFORMED - an error section
    # (keep), never None (which would authoritatively clear pop families).
    sections = {w: _section_or_error(doc.get(w)) for w in wire_names}
    not_ready = [w for w, sec in sections.items() if sec.get("status") == "not-ready"]
    if not_ready:
        try:
            async with _action_semaphore():
                output = await nso_client.run_device_state_read(device.nso_device_name, not_ready)
            if not isinstance(output, dict):
                raise TypeError(f"action output is {type(output).__name__}, not a mapping")
            for w in not_ready:
                sections[w] = _section_or_error(output.get(w))
        except Exception as exc:  # noqa: BLE001 — heal failure degrades only the not-ready set
            for w in not_ready:
                sections[w] = {"status": "error", "error-reason": f"heal action failed: {exc!r}"}
    return sections, None


def _section_or_error(section) -> dict:
    """Coerce a projected section to a dict; anything else becomes an error section."""
    if isinstance(section, dict):
        return section
    return {"status": "error", "error-reason": f"malformed section ({type(section).__name__})"}


async def _run_surfaces_projected(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    surfaces: list[tuple[str, object]],
    refresh_source: str,
    *,
    atomic: bool = False,
) -> list[str]:
    """READSEM grains b/c: feed every spec-backed surface from ONE projected read.

    Same contract as :func:`_run_surfaces` (failed-name list, per-surface isolation);
    non-spec surfaces (redistribution) still run their own warm section reads.
    """
    from contextlib import AsyncExitStack

    from nso_adapter.core.redistribution import _REDIST_COMPONENTS, refresh_redistribution_from_outcomes
    from nso_adapter.core.refresh_engine import _family_lock

    spec_by_name = {name: _projectable_spec(name) for name, _ in surfaces}
    wire_names = [spec.wire_name for spec in spec_by_name.values() if spec is not None]
    if "redistribution" in spec_by_name:
        # Codex S3-R3 F2: redistribution's three components classify from the SAME fetched
        # snapshot — force their wires into the fetch even if those family surfaces are
        # individually disabled (a missing section would read as device absence and WIPE
        # the partition).
        wire_names = sorted({*wire_names, *(w for _p, w, _b in _REDIST_COMPONENTS)})
    # Codex S3-R3 F3: hold EVERY spec surface's family lock across fetch+apply -
    # otherwise a grain-a/SSE refresh can materialize a NEWER read between our fetch and
    # our apply, and the delayed projection overwrites it with older data. Sorted
    # acquisition (single order, no nesting elsewhere) keeps it deadlock-free; the apply
    # calls below pass own_lock=False.
    async with AsyncExitStack() as lock_stack:
        for name in sorted(n for n, spec in spec_by_name.items() if spec is not None):
            await lock_stack.enter_async_context(_family_lock(device.id, name))
        sections, supplier_outcome = await _fetch_projection(nso_client, device, wire_names, atomic=atomic)

        failed: list[str] = []
        for name, fn in surfaces:
            spec = spec_by_name[name]
            try:
                if spec is None and name == "redistribution":
                    if supplier_outcome is not None:
                        component_outcomes = {proto: supplier_outcome for proto, _w, _b in _REDIST_COMPONENTS}
                    else:
                        component_outcomes = {
                            proto: classify_envelope_section(
                                sections[w] if sections[w] is None else _section_or_error(sections[w]),
                                EmptyPolicy.pop,
                            )
                            for proto, w, _b in _REDIST_COMPONENTS
                        }
                    ok = await refresh_redistribution_from_outcomes(
                        db, device, component_outcomes, refresh_source=refresh_source
                    )
                elif spec is None:
                    ok = await fn(db, device, nso_client, refresh_source=refresh_source)
                elif supplier_outcome is not None:
                    ok = await run_family_refresh_from_outcome(
                        db, device, spec, supplier_outcome, refresh_source=refresh_source, own_lock=False
                    )
                else:
                    section = sections[spec.wire_name]
                    if section is None:
                        ok = await run_family_refresh_from_outcome(
                            db,
                            device,
                            spec,
                            classify_envelope_section(None, spec.empty_policy),
                            refresh_source=refresh_source,
                            own_lock=False,
                        )
                    else:
                        ok = await run_family_refresh_from_section(
                            db, device, spec, section, refresh_source=refresh_source, own_lock=False
                        )
                if not ok:
                    failed.append(name)
            except Exception as exc:  # noqa: BLE001 — one surface must not take down the rest
                logger.warning("sync.surface_refresh_failed", device_id=device.id, surface=name, error=repr(exc))
                failed.append(name)
        return failed


def _routing_surfaces(cfg) -> list[tuple[str, object]]:
    """Build the routing/extra surface list refreshed by ``sync_device`` (Sync Now + 15-min poll).

    Includes ``interface_ip`` (A3): folding it into the sync fan-out is a cheap +1 read that
    keeps the operator-visible IP mirror fresh on every sync, instead of only on its 60-min
    poll or an SSE event. The heavier L2/lag families stay off this hot path — they refresh
    on their own poll jobs and via the comprehensive on-demand ``refresh_all_surfaces``.
    """
    surfaces: list[tuple[str, object]] = []
    if cfg.enable_static_routing_sync:
        from nso_adapter.core.static_route import refresh_static_routes_for_device

        surfaces.append(("static_route", refresh_static_routes_for_device))
    if cfg.enable_isis_sync:
        from nso_adapter.core.isis import refresh_isis_interfaces_for_device

        surfaces.append(("isis", refresh_isis_interfaces_for_device))
    if cfg.enable_bgp_sync:
        from nso_adapter.core.bgp import refresh_bgp_config_for_device

        surfaces.append(("bgp", refresh_bgp_config_for_device))
    if cfg.enable_ospf_sync:
        from nso_adapter.core.ospf import refresh_ospf_for_device

        surfaces.append(("ospf", refresh_ospf_for_device))
    if cfg.enable_redistribution_sync:
        from nso_adapter.core.redistribution import refresh_redistribution_for_device

        surfaces.append(("redistribution", refresh_redistribution_for_device))
    if cfg.enable_route_policy_sync:
        from nso_adapter.core.route_policy import refresh_route_policy_for_device

        surfaces.append(("route_policy", refresh_route_policy_for_device))
    if cfg.enable_snmp_sync:
        from nso_adapter.core.snmp import refresh_snmp_config_for_device

        surfaces.append(("snmp", refresh_snmp_config_for_device))
    if cfg.enable_logging_sync:
        from nso_adapter.core.logging_config import refresh_logging_config_for_device

        surfaces.append(("logging", refresh_logging_config_for_device))
    if cfg.enable_bfd_sync:
        from nso_adapter.core.bfd import refresh_bfd_interfaces_for_device

        surfaces.append(("bfd", refresh_bfd_interfaces_for_device))
    if cfg.enable_interface_ip_sync:
        from nso_adapter.core.interface_ip import refresh_interface_ips_for_device

        surfaces.append(("interface_ip", refresh_interface_ips_for_device))
    return surfaces


def _config_surfaces(cfg) -> list[tuple[str, object]]:
    """Build the L2 / interface config surface list (VLAN / SVI / subinterface / MTU)."""
    surfaces: list[tuple[str, object]] = []
    if cfg.enable_vlan_sync:
        from nso_adapter.core.vlan import refresh_vlan_database_for_device

        surfaces.append(("vlan", refresh_vlan_database_for_device))
    if cfg.enable_svi_sync:
        from nso_adapter.core.svi import refresh_svi_for_device

        surfaces.append(("svi", refresh_svi_for_device))
    if cfg.enable_subinterface_sync:
        from nso_adapter.core.subinterface import refresh_subinterface_for_device

        surfaces.append(("subinterface", refresh_subinterface_for_device))
    if cfg.enable_interface_mtu_sync:
        from nso_adapter.core.interface_mtu import refresh_interface_mtu_for_device

        surfaces.append(("interface_mtu", refresh_interface_mtu_for_device))
    return surfaces


def _extra_mirror_surfaces(cfg) -> list[tuple[str, object]]:
    """Build the device-mirror surface list neither in the routing fan-out nor the config set.

    ``lag_topology`` / ``lag_config`` carry no dedicated enable flag (their poll job is
    gated on interval only), so they are always included in the comprehensive refresh.
    """
    surfaces: list[tuple[str, object]] = []
    from nso_adapter.core.lag_topology import refresh_lag_topology_for_device

    surfaces.append(("lag_topology", refresh_lag_topology_for_device))
    from nso_adapter.core.lag_config import refresh_lag_config_for_device

    surfaces.append(("lag_config", refresh_lag_config_for_device))
    if cfg.enable_l2_service_sync:
        from nso_adapter.core.l2_service import refresh_l2_services_for_device

        surfaces.append(("l2_service", refresh_l2_services_for_device))
    if cfg.enable_switchport_sync:
        from nso_adapter.core.vlan import refresh_switchport_for_device

        surfaces.append(("switchport", refresh_switchport_for_device))
    return surfaces


async def refresh_routing_surfaces_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "sync",
    atomic: bool = False,
) -> list[str]:
    """Fan-out for ``sync_device``: refresh the routing/extra surfaces (incl. interface_ip).

    A device "sync" historically only refreshed interface attributes; the routing surfaces
    were updated solely by their independent poll jobs, so "Sync Now" never moved them (and
    interface_ip was omitted entirely). Runs each enabled surface's per-device refresh on
    demand, gated by the scheduler enable flags. Returns the FAILED surface names (see
    :func:`_run_surfaces`); the caller records them so the device reports ``partial``.
    """
    return await _run_surfaces_projected(
        db, device, nso_client, _routing_surfaces(get_config().scheduler), refresh_source, atomic=atomic
    )


async def refresh_config_surfaces_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "apply",
    atomic: bool = False,
) -> list[str]:
    """Best-effort refresh of the L2 / interface config surfaces (VLAN / SVI / subinterface / MTU).

    These back the plugin's ``accepted → deploying`` overlay rows that settle to ``in_sync`` only
    once the applied object is *present* in the adapter read-mirror. Re-reading them right after a
    device Apply lets the row settle instead of waiting for that surface's next poll. Returns the
    FAILED surface names (callers may ignore the list — apply just wants the best-effort refresh).
    """
    return await _run_surfaces_projected(
        db, device, nso_client, _config_surfaces(get_config().scheduler), refresh_source, atomic=atomic
    )


async def refresh_all_surfaces_for_device(
    db: AsyncSession,
    device: Device,
    nso_client: NsoClient,
    *,
    refresh_source: str = "refresh",
    atomic: bool = False,
) -> list[str]:
    """Comprehensive on-demand refresh of EVERY enabled read-mirror family for one device.

    The single "refresh this device's whole mirror now" primitive, used at the moments that
    matter (onboarding, an explicit refresh) where reading all 18 families is worth it — as
    opposed to the lean per-15-min ``sync_device`` path (routing + interface_ip only). Each
    surface is enable-gated and isolated; returns the FAILED surface names so the caller can
    report ``partial``. The caller commits.
    """
    cfg = get_config().scheduler
    surfaces = _routing_surfaces(cfg) + _config_surfaces(cfg) + _extra_mirror_surfaces(cfg)
    return await _run_surfaces_projected(db, device, nso_client, surfaces, refresh_source, atomic=atomic)


class _WriteCtx(NamedTuple):
    """NetBox write context threaded through the per-interface reconcile.

    ``attr_patches`` / ``pending_by_id`` are accumulators mutated in place: the
    merged PATCH rows, and the (attr_state, value) updates applied only once the
    bulk PATCH confirms each id was written.
    """

    nb_client: object
    device: Device
    nb_id_by_name: dict[str, int]
    attr_patches: dict[int, dict]
    pending_by_id: dict[int, list[tuple]]


async def _resolve_ned_id(db: AsyncSession, device: Device, client: NsoClient) -> None:
    """Resolve (and refresh) the device's NED ID from NSO; mark unmatched + raise if unresolvable.

    Re-reads NSO on every sync so a NED change on the device is picked up — ``ned_id`` keys the
    capability matrix, so a stale value silently mis-keys every verdict. A transient read that
    returns nothing does NOT clobber a previously-known ned_id (only an *unset*-and-unresolvable
    ned_id marks the device unmatched); the read is a small ``fields=device-type`` GET.

    "Returns nothing" includes RAISING. ``get_device_ned_id`` calls ``raise_for_status()``, and
    this is the FIRST NSO call in :func:`sync_device` — before ``sync_from`` — so an NSO restart
    or load spike answering 502/503 (or a 404 for a device renamed in NSO) would otherwise fail
    the whole sync for every device whose NED was already known, staling the entire fleet's
    mirrors. Before the per-sync refresh was added, such a device never made this call at all.
    """
    try:
        learned = await client.get_device_ned_id(device.nso_device_name)
    except Exception as exc:  # noqa: BLE001 — a read failure must not fail an otherwise-fine sync
        if device.ned_id:
            logger.warning(
                "importer.ned_id.read_failed",
                device=device.nso_device_name,
                kept=device.ned_id,
                error=repr(exc),
            )
            return  # keep the last-known value and sync on
        learned = ""  # nothing to fall back on → the unmatched path below
    if learned:
        if device.ned_id != learned:
            logger.info("importer.ned_id.changed", device=device.nso_device_name, old=device.ned_id, new=learned)
            device.ned_id = learned
            # Persist the corrected NED now, so a device whose later sync steps fail (e.g. an
            # unsupported NED with no reader) still self-heals its ned_id on any sync attempt.
            await db.commit()
        return
    if device.ned_id:
        return  # keep the last-known value — a transient empty read must not wipe it
    device.mapping_status = MappingStatus.unmatched_device
    device.last_sync_at = _utcnow()
    device.last_sync_status = LastSyncStatus.failed
    await db.commit()
    raise ValueError(f"NSO device {device.nso_device_name!r} not found or has no NED ID")


async def _ensure_netbox_interfaces(nb_client, device: Device, device_id: int, interfaces) -> dict[str, int]:
    """Phase 1: bulk-ensure every NSO interface exists in NetBox; return name→nb_id (best-effort)."""
    if not (nb_client and device.netbox_device_id):
        return {}
    from nso_adapter.bindings.netbox.mapper import bulk_ensure_interfaces

    try:
        return await bulk_ensure_interfaces(
            nb_client,
            device.netbox_device_id,
            # M27R: pass parent_binding/kind so Nokia logical interfaces are created
            # by their faithful name, parented to the bound port/LAG.
            [{"name": i.name, "parent_binding": i.parent_binding, "kind": i.kind} for i in interfaces],
        )
    except Exception as exc:
        logger.warning("netbox.bulk_ensure_failed", device_id=device_id, error=str(exc))
        return {}


async def _upsert_db_interface(db: AsyncSession, device_id: int, iface, existing_ifaces) -> tuple[DbInterface, bool]:
    """Upsert the DbInterface row + keep the M27R logical-modeling fields fresh. Returns (row, created)."""
    db_iface = existing_ifaces.get(iface.name)
    created = False
    if db_iface is None:
        db_iface = DbInterface(device_id=device_id, name=iface.name)
        db.add(db_iface)
        await db.flush()  # get id before upserting attr states
        created = True
    existing_ifaces[iface.name] = db_iface
    # M27R: NULL/empty for physical ports and for Cisco/Junos.
    db_iface.parent_binding = iface.parent_binding
    db_iface.kind = iface.kind
    db_iface.encap_tag = iface.encap_tag
    db_iface.vrf = iface.vrf
    db_iface.service = iface.service
    return db_iface, created


def _reconcile_attr(db, db_iface, attr, iface, intent_by_attr, existing_attrs, ctx: _WriteCtx) -> bool:
    """Compute one attribute's sync_state, queue a NetBox write if it changed, update the state row.

    Returns True if a Phase-1 ``changed`` was detected (drives changes_detected).
    """
    nso_val = iface.nso.description if attr == "description" else iface.nso.enabled
    nso_str = str(nso_val) if nso_val is not None else None

    attr_state = existing_attrs.get(attr)
    if attr_state is None:
        attr_state = InterfaceAttrState(interface_id=db_iface.id, attribute=attr)
        db.add(attr_state)

    intent_val = intent_by_attr.get(attr)
    prev_netbox_val = attr_state.netbox_value
    status = compute_sync_state(nso_str, prev_netbox_val, intent_val)
    changed = status == SyncState.changed

    # Queue a NetBox write only when the value differs from what we last successfully
    # wrote (netbox_value, updated after the Phase 2 flush confirms it) — without this
    # every sync re-patches every interface, overwhelming NetBox.
    if ctx.nb_client and ctx.device.netbox_device_id:
        nb_id = ctx.nb_id_by_name.get(iface.name)
        if nb_id is not None:
            if db_iface.netbox_interface_id is None:
                db_iface.netbox_interface_id = nb_id
            if prev_netbox_val != nso_str:
                field_payload: dict = {}
                if attr == "description":
                    field_payload["description"] = iface.nso.description or ""
                elif iface.nso.enabled is not None:
                    field_payload["enabled"] = iface.nso.enabled
                else:
                    return changed  # NSO package didn't report enabled; skip write + state update
                ctx.attr_patches.setdefault(nb_id, {"id": nb_id}).update(field_payload)
                ctx.pending_by_id.setdefault(nb_id, []).append((attr_state, nso_str))

    attr_state.nso_value = nso_str
    if intent_val is not None:
        # Phase 2: intent deployed — use in_sync/drifted; never downgrade to "imported".
        attr_state.sync_state = status
    else:
        # Phase 1: "imported" when values match (netbox_value lags one flush — self-heals).
        attr_state.sync_state = SyncState.imported if attr_state.netbox_value == nso_str else status
    attr_state.last_checked_at = _utcnow()
    return changed


async def _reconcile_interface(db, device_id, iface, scope_attrs, existing_ifaces, ctx: _WriteCtx) -> tuple[bool, int]:
    """Upsert one interface + reconcile each in-scope attr. Returns (created, changes_detected)."""
    db_iface, created = await _upsert_db_interface(db, device_id, iface, existing_ifaces)

    # InterfaceIntent is the single source of truth for deployed intent (Phase 1 vs 2).
    attr_result = await db.execute(select(InterfaceAttrState).where(InterfaceAttrState.interface_id == db_iface.id))
    existing_attrs = {row.attribute: row for row in attr_result.scalars().all()}
    intent_by_attr = await _load_intent_by_attr(db, db_iface.id)

    changes = 0
    for attr in ("description", "enabled"):
        if attr not in scope_attrs:
            continue
        if _reconcile_attr(db, db_iface, attr, iface, intent_by_attr, existing_attrs, ctx):
            changes += 1
    return created, changes


async def _flush_netbox_patches(nb_client, attr_patches, pending_by_id) -> int:
    """Phase 2: push the batched PATCHes; mark netbox_value only for confirmed ids. Returns count."""
    if not (nb_client and attr_patches):
        return 0
    written = await nb_client.bulk_patch_interfaces(list(attr_patches.values()))
    count = 0
    for obj in written:
        for attr_state, nso_str in pending_by_id.get(obj["id"], []):
            attr_state.netbox_value = nso_str
            count += 1
    return count


def _sync_from_succeeded(output) -> bool:
    """Whether a sync-from action output signals it actually pulled the running config.

    NSO returns ``{"result": true}`` on a real pull. Mirrors ``NsoClient.sync_from``'s check
    (absent/falsy result → not pulled), tolerating a string rendering. Used so ``sync_device``
    does not report a live reread when the CDB was never refreshed (device unreachable).
    """
    result = (output or {}).get("result")
    if isinstance(result, str):
        return result.strip().lower() == "true"
    return bool(result)


async def _record_attrs_read(db, device, outcome, refresh_source: str):
    """Best-effort phase-1 outcome record for the importer-owned attrs read (R1-F7).

    With the engine's session-recovery discipline (codex S3-R3 F4): a store write that
    dies at the DB level dooms sync_device's batched transaction — recover it or the
    interface reconcile and every later surface inherit PendingRollbackError.
    """
    from nso_adapter.core.refresh_engine import _recover_session
    from nso_adapter.store import outcome_store

    device_id = device.id  # snapshot before the store call can poison the session
    try:
        return await outcome_store.record_read_outcome(
            db, device_id, "interface_attributes", outcome, refresh_source=refresh_source
        )
    except Exception as exc:  # noqa: BLE001 — telemetry write; the sync is the story
        logger.warning("interface_attributes.outcome.read_record_failed", device_id=device_id, error=repr(exc))
        await _recover_session(db, device, "interface_attributes", device_id)
        return None


async def _record_attrs_result(db, device, attempt_id, *, available: bool, row_count: int | None) -> None:
    """Best-effort phase-2 terminalization for the attrs read (same recovery discipline)."""
    from nso_adapter.core.refresh_engine import _recover_session
    from nso_adapter.store import outcome_store

    if attempt_id is None:
        return
    device_id = device.id
    try:
        await outcome_store.record_result(
            db,
            attempt_id,
            result="replaced" if available else "kept",
            succeeded=available,
            row_count=row_count if available else None,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("interface_attributes.outcome.result_record_failed", attempt_id=attempt_id, error=repr(exc))
        await _recover_session(db, device, "interface_attributes", device_id)


async def sync_device(device_id: int, db: AsyncSession, *, atomic: bool = False) -> dict:
    """Full sync: NSO → DB → NetBox. Returns job result summary dict.

    ``atomic=True`` is READSEM grain c (operator Sync-Now): the surface fan-out reads ONE
    txid-bracketed ``device-state-read`` build instead of the record-served projection.
    """
    device = await db.get(Device, device_id)
    if not device:
        raise ValueError(f"Device {device_id} not found")

    client = get_nso_client(device.nso_instance)
    await _resolve_ned_id(db, device, client)

    # Step 1: sync-from — refresh CDB from live device. Capture the action result: a 200 does
    # NOT mean it pulled (a device-unreachable sync-from returns result:false), and every mirror
    # read below is only as fresh as this made the CDB (A3b).
    sync_from_ok = _sync_from_succeeded(await nso_actions.sync_from(client, device.nso_device_name))

    # Step 2: read canonical interface attributes from NSO package oper-data, through the
    # read-outcome vocabulary. interface-attributes is a present-policy inventory family
    # (get_interface_attributes → confirm_404=False), so a 404/None means the export is down /
    # the NED is unsupported / the device is not-ready — NOT "this device has zero interfaces".
    # Only an authoritative Present read may drive the interface reconcile and flip
    # mapping_status; an Unavailable read leaves the prior mapping intact and reports the surface
    # degraded, so a transient export blip never demotes a mapped device to unmatched_interfaces.
    attrs_outcome = await classify_envelope_family_read(
        device,
        client,
        wire_name="interface-attributes",
        empty_policy=EmptyPolicy.present,
        family_name="interface_attributes",
    )
    attrs_available = isinstance(attrs_outcome, Present)
    # READSEM S3 B5 (codex R1-F7): attrs starts recording outcomes. Phase 1 here; phase 2
    # after the interface reconcile below (best-effort — never breaks the sync).
    attrs_attempt_id = await _record_attrs_read(db, device, attrs_outcome, "sync")

    nb_client = get_netbox_client()
    interfaces_created = 0
    changes_detected = 0
    interfaces_written = 0
    if attrs_available:
        interfaces = _attrs_to_interface_list(attrs_outcome.data)

        scope_result = await db.execute(select(ManagedScope).where(ManagedScope.device_id == device_id))
        scope_attrs = [s.attribute for s in scope_result.scalars().all()]

        result_rows = await db.execute(select(DbInterface).where(DbInterface.device_id == device_id))
        existing_ifaces: dict[str, DbInterface] = {row.name: row for row in result_rows.scalars().all()}

        # Phase 1: bulk interface inventory reconcile (plan Layer A).
        nb_id_by_name = await _ensure_netbox_interfaces(nb_client, device, device_id, interfaces)

        ctx = _WriteCtx(nb_client, device, nb_id_by_name, {}, {})
        for iface in interfaces:
            created, changes = await _reconcile_interface(db, device_id, iface, scope_attrs, existing_ifaces, ctx)
            interfaces_created += int(created)
            changes_detected += changes

        # Phase 2 flush: push queued attribute updates, batched + isolated.
        interfaces_written = await _flush_netbox_patches(nb_client, ctx.attr_patches, ctx.pending_by_id)

        # The interface sync itself is done; its mapping is accurate regardless of what the
        # routing surfaces do next. An authoritative present-empty read (zero interfaces) is a
        # genuine unmatched_interfaces.
        device.mapping_status = MappingStatus.mapped if interfaces else MappingStatus.unmatched_interfaces
    else:
        # Unavailable read: keep the prior mapping_status untouched; the surface is degraded and
        # is added to the fan-out's degraded list below.
        logger.warning(
            "sync.interface_attributes_unavailable",
            device_id=device_id,
            reason=attrs_outcome.reason.value,
        )
    # Commit the interface work + timestamp now, but defer the final last_sync_status until after
    # the fan-out so a silently-failed surface read cannot hide under a premature 'succeeded'.
    device.last_sync_at = _utcnow()
    await db.commit()
    await _record_attrs_result(db, device, attrs_attempt_id, available=attrs_available, row_count=interfaces_written)

    # Fan out to the routing/extra surfaces so one sync refreshes everything the device
    # exposes (IS-IS/BGP/OSPF/route-policy/...), not just interface attributes. Done
    # before the plugin notify so its reconcile sees the fresh surface state in one pass.
    degraded = await refresh_routing_surfaces_for_device(db, device, client, refresh_source="sync", atomic=atomic)
    if not attrs_available:
        degraded = [*degraded, "interface_attributes"]

    # A3b: a sync-from that did not actually pull (result:false / unreachable) means every surface
    # was just re-read from STALE CDB — this sync is not a live device reread, so report it degraded
    # rather than claim 'succeeded' with fresh data it does not have.
    if not sync_from_ok:
        degraded = [*degraded, "sync_from"]

    # Record the outcome only AFTER the fan-out. A surface whose NSO read failed leaves a
    # stale mirror, so the device reports 'partial' (naming the offending surfaces) rather
    # than a misleading 'succeeded'; a clean sync clears any prior degraded marker.
    if degraded:
        device.last_sync_status = LastSyncStatus.partial
        device.degraded_surfaces = sorted(degraded)
    else:
        device.last_sync_status = LastSyncStatus.succeeded
        device.degraded_surfaces = None
    await db.commit()

    # Notify the netbox-nso-plugin so it refreshes its NSO*State display cache off
    # the request path. Best-effort — a callback failure must not fail the sync.
    if nb_client and device.netbox_device_id:
        try:
            await nb_client.notify_sync_complete(device.netbox_device_id)
        except Exception as exc:
            logger.warning(
                "netbox.sync_complete_notify_failed", device_id=device_id, error=str(exc) or type(exc).__name__
            )

    summary = {
        "interfaces_written": interfaces_written,
        "interfaces_created": interfaces_created,
        "changes_detected": changes_detected,
    }
    logger.info("sync.done", device_id=device_id, **summary)
    return summary


async def detect_drift(device_id: int, db: AsyncSession) -> dict:
    """Re-read NSO config and recompute sync_state WITHOUT writing to NetBox."""
    device = await db.get(Device, device_id)
    if not device:
        raise ValueError(f"Device {device_id} not found")

    client = get_nso_client(device.nso_instance)

    # compare-config re-reads from NSO CDB vs live device
    await nso_actions.compare_config(client, device.nso_device_name)

    # Route the attrs read through the vocabulary (present-policy family): a 404/None or read
    # error is Unavailable, not "zero interfaces", so drift is computed only from an authoritative
    # Present read. An Unavailable read leaves the stored sync_state untouched (drift is read-only).
    attrs_outcome = await classify_envelope_family_read(
        device,
        client,
        wire_name="interface-attributes",
        empty_policy=EmptyPolicy.present,
        family_name="interface_attributes",
    )
    if isinstance(attrs_outcome, Present):
        interfaces = _attrs_to_interface_list(attrs_outcome.data)
    else:
        logger.warning(
            "drift.interface_attributes_unavailable",
            device_id=device_id,
            reason=attrs_outcome.reason.value,
        )
        interfaces = []

    scope_result2 = await db.execute(select(ManagedScope).where(ManagedScope.device_id == device_id))
    scope_attrs = [s.attribute for s in scope_result2.scalars().all()]
    changes_detected = 0

    # Compare against the CURRENT NetBox value, not the cached netbox_value: the cache
    # only ever holds a value the adapter itself wrote, so a description/enable set
    # straight into NetBox is otherwise invisible to drift detection. detect_drift is
    # read-only by contract — we never persist netbox_value here, so sync_device's
    # change-detection cache stays intact and cannot clobber the operator's edit.
    nb_client = get_netbox_client()
    netbox_attrs: dict[str, dict] = {}
    if nb_client and device.netbox_device_id:
        try:
            for nb_iface in await nb_client.list_interfaces(device.netbox_device_id):
                netbox_attrs[nb_iface["name"]] = nb_iface
        except Exception as exc:
            logger.warning("netbox.drift_read_failed", device_id=device_id, error=str(exc) or type(exc).__name__)

    for iface in interfaces:
        result_rows = await db.execute(
            select(DbInterface).where(DbInterface.device_id == device_id, DbInterface.name == iface.name)
        )
        db_iface = result_rows.scalar_one_or_none()
        if db_iface is None:
            continue

        nb_iface = netbox_attrs.get(iface.name)
        intent_by_attr = await _load_intent_by_attr(db, db_iface.id)
        for attr in ("description", "enabled"):
            if attr not in scope_attrs:
                continue
            nso_val = iface.nso.description if attr == "description" else iface.nso.enabled
            nso_str = _attr_str(attr, nso_val)

            attr_result = await db.execute(
                select(InterfaceAttrState).where(
                    InterfaceAttrState.interface_id == db_iface.id,
                    InterfaceAttrState.attribute == attr,
                )
            )
            attr_state = attr_result.scalar_one_or_none()
            if attr_state is None:
                continue

            # Live NetBox value when we could read it; fall back to the cache otherwise.
            if nb_iface is not None:
                netbox_str = _attr_str(attr, nb_iface.get(attr))
            else:
                netbox_str = attr_state.netbox_value

            status = compute_sync_state(nso_str, netbox_str, intent_by_attr.get(attr))
            if status in (SyncState.changed, SyncState.drifted):
                changes_detected += 1
            attr_state.nso_value = nso_str
            attr_state.sync_state = status
            attr_state.last_checked_at = _utcnow()

    device.last_sync_at = _utcnow()
    await db.commit()

    # Refresh the netbox-nso-plugin display cache so Detect Drift results are
    # visible immediately (mirrors sync_device). Without this, detect-drift updates
    # only the adapter's view and the plugin keeps showing stale statuses until the
    # next full sync reconciles. Best-effort — a callback failure must not fail drift.
    if nb_client and device.netbox_device_id:
        try:
            await nb_client.notify_sync_complete(device.netbox_device_id)
        except Exception as exc:
            logger.warning("netbox.drift_notify_failed", device_id=device_id, error=str(exc) or type(exc).__name__)

    return {"changes_detected": changes_detected}


async def discover_devices(db: AsyncSession) -> None:
    """Pull device list from all configured NSO instances and upsert into DB."""
    cfg = get_config()
    for inst in cfg.nso_instances:
        client = get_nso_client(inst.name)
        try:
            device_list = await client.list_devices()
        except Exception as exc:
            logger.error("discover.error", instance=inst.name, error=str(exc))
            continue
        for dev_data in device_list:
            name = dev_data.get("name")
            if not name:
                continue
            result = await db.execute(
                select(Device).where(
                    Device.nso_instance == inst.name,
                    Device.nso_device_name == name,
                )
            )
            if not result.scalar_one_or_none():
                db.add(Device(nso_instance=inst.name, nso_device_name=name))
    await db.commit()
