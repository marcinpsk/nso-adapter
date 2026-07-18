# SPDX-License-Identifier: Apache-2.0
"""Adapter config API — global mgmt-IP failover tuning (the plugin's settings singleton).

The plugin's ``NSOFailoverSettings`` singleton pushes the tuning here on save; the base-tick
reads it live each run, so changes apply on the next tick without rescheduling APScheduler.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.deps import get_db, verify_token
from nso_adapter.config import get_config
from nso_adapter.core.failover import (
    EffectiveFailoverConfig,
    get_effective_failover_config,
    upsert_failover_config,
)

router = APIRouter(prefix="/api/v1/config", tags=["config"])


def _config_out(eff: EffectiveFailoverConfig, deployment_enabled: bool) -> dict:
    """Serialize the effective config with the canonical (un-prefixed) field names.

    ``deployment_enabled`` is the deployment-level master switch
    (``scheduler.enable_failover`` from the adapter's static config). It gates the whole
    feature — both the failover probe loop's job registration AND onboarding's OOB
    bootstrap — so when it is False the runtime ``enabled`` toggle has no effect. The
    plugin surfaces this so enabling failover there isn't silently a no-op.
    """
    return {
        "enabled": eff.enabled,
        "deployment_enabled": deployment_enabled,
        "primary_probe_interval": eff.failover_primary_probe_interval,
        "oob_probe_interval": eff.failover_oob_probe_interval,
        "failure_threshold": eff.failover_failure_threshold,
        "success_threshold": eff.failover_success_threshold,
        "probe_timeout": eff.failover_probe_timeout,
        "active_probe_timeout": eff.failover_active_probe_timeout,
        "probe_concurrency": eff.probe_concurrency,
        "max_flips_per_tick": eff.max_flips_per_tick,
        "sync_from_after_switch": eff.failover_sync_from_after_switch,
    }


class FailoverConfigUpdate(BaseModel):
    """Partial update of the failover tuning singleton — every field optional (None = leave as-is).

    The plugin pushes the full set on save; the bounds keep an operator typo from wedging the loop.
    """

    enabled: bool | None = None
    primary_probe_interval: int | None = Field(None, ge=1)  # minutes
    oob_probe_interval: int | None = Field(None, ge=1)  # minutes
    failure_threshold: int | None = Field(None, ge=1)
    success_threshold: int | None = Field(None, ge=1)
    probe_timeout: float | None = Field(None, gt=0, le=120)  # seconds
    active_probe_timeout: float | None = Field(None, gt=0, le=120)  # seconds
    # Ceiling kept ≤ the DB pool headroom (store/db.py sizes the pool at 30): each concurrent
    # probe holds a session for the full unreachable-probe timeout, so a higher value would
    # starve the pool that API/sync traffic shares.
    probe_concurrency: int | None = Field(None, ge=1, le=16)
    max_flips_per_tick: int | None = Field(None, ge=1, le=256)
    sync_from_after_switch: bool | None = None


@router.get("/failover", dependencies=[Depends(verify_token)])
async def get_failover_config(db: AsyncSession = Depends(get_db)):
    """Return the live failover config (the stored singleton, or the static fallback)."""
    sched = get_config().scheduler
    eff = await get_effective_failover_config(db, sched)
    return _config_out(eff, sched.enable_failover)


@router.put("/failover", dependencies=[Depends(verify_token)])
async def put_failover_config(body: FailoverConfigUpdate, db: AsyncSession = Depends(get_db)):
    """Upsert the failover tuning singleton; the next base-tick reads the new values."""
    await upsert_failover_config(db, **body.model_dump(exclude_unset=True))
    await db.commit()
    sched = get_config().scheduler
    eff = await get_effective_failover_config(db, sched)
    return _config_out(eff, sched.enable_failover)
