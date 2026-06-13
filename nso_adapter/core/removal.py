# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Async removal propagation for the per-service intent PUT endpoints.

A merge-PATCH apply never drops a list entry you omit, and a node-level RESTCONF
DELETE 404s on empty-string list keys. So when an intent PUT (full-replace store)
deletes rows, the device keeps the orphaned config until the FULL remaining
desired state is re-asserted via a PUT-replace of the keyed service instance
(``apply_callable(..., replace=True)``), which lets FASTMAP revert the removed
entries.

That PUT-replace is a synchronous device commit and can take well over the
plugin's HTTP client timeout (~30s). So it does NOT run inline in the intent
PUT anymore — :func:`replace_on_removal` enqueues a ``removal`` job and returns
immediately; the worker runs :func:`run_removal` in the background. The job is
idempotent (it re-reads the current accepted rows and PUT-replaces), so it is
safe to requeue after a restart.
"""

from __future__ import annotations

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)


# scope → (intent store model name, apply function name) for the "simple" services
# whose apply takes a single ``(client, device_name, rows, replace=True)`` signature.
_SIMPLE_TARGETS: dict[str, tuple[str, str]] = {
    "route_policy": ("RoutePolicyObjectIntent", "apply_route_policy_config"),
    "bfd": ("BfdIntent", "apply_bfd_config"),
    "svi": ("SviIntent", "apply_svi_config"),
    "subinterface": ("SubinterfaceIntent", "apply_subinterface_config"),
    "static_route": ("StaticRouteIntent", "apply_static_routes"),
    "interface_mtu": ("InterfaceMtuIntent", "apply_mtu_config"),
    "vlan": ("VlanIntent", "apply_vlan_config"),
    "logging": ("LoggingHostIntent", "apply_logging_config"),
    "l2_sap": ("L2SapIntent", "apply_l2_saps"),
}

# Reverse map: intent store model name → removal scope, so the legacy
# replace_on_removal(store_model, apply_callable) callers need no change.
_SCOPE_BY_MODEL: dict[str, str] = {model: scope for scope, (model, _) in _SIMPLE_TARGETS.items()}

# OSPF and BGP have multi-row applies, so they get bespoke handlers below.
VALID_REMOVAL_SCOPES: set[str] = set(_SIMPLE_TARGETS) | {"ospf", "bgp"}


async def _replace_simple(db: AsyncSession, device, client, scope: str) -> None:
    """PUT-replace a single-model service with its remaining accepted rows."""
    from nso_adapter.nso import apply as nso_apply
    from nso_adapter.store import models as store_models

    model_name, apply_name = _SIMPLE_TARGETS[scope]
    model = getattr(store_models, model_name)
    apply_fn = getattr(nso_apply, apply_name)
    rows = (
        (await db.execute(select(model).where(model.device_id == device.id, model.accepted_at.is_not(None))))
        .scalars()
        .all()
    )
    await apply_fn(client, device.nso_device_name, rows, replace=True)


async def _replace_ospf(db: AsyncSession, device, client) -> None:
    from nso_adapter.nso.apply import apply_ospf_config
    from nso_adapter.store.models import OspfInstanceIntent, OspfInterfaceIntent, RedistributionIntent

    insts = (
        (await db.execute(select(OspfInstanceIntent).where(OspfInstanceIntent.device_id == device.id))).scalars().all()
    )
    ifaces = (
        (await db.execute(select(OspfInterfaceIntent).where(OspfInterfaceIntent.device_id == device.id)))
        .scalars()
        .all()
    )
    redist = (
        (
            await db.execute(
                select(RedistributionIntent).where(
                    RedistributionIntent.device_id == device.id,
                    RedistributionIntent.dest_protocol == "ospf",
                )
            )
        )
        .scalars()
        .all()
    )
    await apply_ospf_config(client, device.nso_device_name, insts, ifaces, redist, replace=True)


async def _replace_bgp(db: AsyncSession, device, client) -> None:
    from nso_adapter.core.bgp_load import attach_bgp_relationships
    from nso_adapter.nso.apply import apply_bgp_config
    from nso_adapter.store.models import BgpRouterIntent, RedistributionIntent

    routers = (
        (
            await db.execute(
                select(BgpRouterIntent).where(
                    BgpRouterIntent.device_id == device.id, BgpRouterIntent.accepted_at.is_not(None)
                )
            )
        )
        .scalars()
        .all()
    )
    await attach_bgp_relationships(db, routers)
    redist = (
        (
            await db.execute(
                select(RedistributionIntent).where(
                    RedistributionIntent.device_id == device.id,
                    RedistributionIntent.dest_protocol == "bgp",
                )
            )
        )
        .scalars()
        .all()
    )
    await apply_bgp_config(client, device.nso_device_name, routers, redist, replace=True)


async def _dispatch_scope(db: AsyncSession, device, client, scope: str) -> None:
    if scope == "ospf":
        await _replace_ospf(db, device, client)
    elif scope == "bgp":
        await _replace_bgp(db, device, client)
    elif scope in _SIMPLE_TARGETS:
        await _replace_simple(db, device, client, scope)
    else:
        raise ValueError(f"Unknown removal scope {scope!r}")


async def enqueue_removal(db: AsyncSession, device_id: int, scope: str):
    """Queue an async ``removal`` job that PUT-replaces *scope*'s service.

    Non-blocking: the intent PUT returns immediately and the worker runs the
    (potentially slow) device commit in the background via :func:`run_removal`.
    """
    from nso_adapter.store.models import Job, JobStatus, JobType

    if scope not in VALID_REMOVAL_SCOPES:
        raise ValueError(f"Unknown removal scope {scope!r}")
    job = Job(
        job_type=JobType.removal,
        device_id=device_id,
        status=JobStatus.queued,
        context={"scope": scope},
    )
    db.add(job)
    await db.flush()
    logger.info("removal.enqueued", device_id=device_id, scope=scope, job_id=job.id)
    return job


async def run_removal(job_id: int, device_id: int) -> None:
    """Execute a queued ``removal`` job: PUT-replace the scope's reconciler service.

    Idempotent — reads the CURRENT accepted rows at run time, so a requeue after a
    restart re-asserts whatever the present desired state is.
    """
    from nso_adapter.core.importer import get_nso_client
    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Device, Job, JobStatus

    async for db in get_session():
        job = await db.get(Job, job_id)
        if not job:
            return
        job.status = JobStatus.running
        await db.commit()
        scope = (job.context or {}).get("scope")
        try:
            device = await db.get(Device, device_id)
            if not device:
                raise ValueError(f"Device {device_id} not found")
            client = get_nso_client(device.nso_instance)
            await _dispatch_scope(db, device, client, scope)
            job.status = JobStatus.succeeded
            job.result = {"scope": scope}
        except Exception as exc:  # noqa: BLE001 — record on the job, never crash the worker
            logger.error("removal.failed", job_id=job_id, device_id=device_id, scope=scope, error=repr(exc))
            job.status = JobStatus.failed
            job.error = {"code": "removal_failed", "message": repr(exc), "detail": {"scope": scope}}
        finally:
            await db.commit()


async def replace_on_removal(db: AsyncSession, device, removed, store_model, apply_callable=None) -> bool:
    """Enqueue an async removal job for *store_model*'s scope if *removed* is truthy.

    Back-compat shim: the per-service intent PUTs still call this with their
    ``(store_model, apply_callable)``; the scope is derived from ``store_model`` and
    the device commit now runs in a background ``removal`` job rather than inline.
    *apply_callable* is retained for signature compatibility but superseded by the
    scope registry. Returns True if a removal job was queued.

    These callers invoke this AFTER committing their row deletes, so the enqueued
    job is committed here. (OSPF/BGP call :func:`enqueue_removal` directly, before
    their own commit, to persist the deletes and the job atomically.)
    """
    if not removed:
        return False
    scope = _SCOPE_BY_MODEL.get(store_model.__name__)
    if scope is None:
        logger.error("removal.unknown_model", model=store_model.__name__)
        return False
    await enqueue_removal(db, device.id, scope)
    await db.commit()
    return True
