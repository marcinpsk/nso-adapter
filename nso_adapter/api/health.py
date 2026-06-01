# SPDX-License-Identifier: Apache-2.0
"""Health check endpoint."""

from __future__ import annotations

from fastapi import APIRouter, Request

from nso_adapter.config import get_config
from nso_adapter.core.importer import get_nso_client

router = APIRouter(tags=["health"])

_VERSION = "0.1.0"


@router.get("/healthz")
async def healthz(request: Request):
    cfg = get_config()
    instances = []
    for inst in cfg.nso_instances:
        reachable = False
        try:
            client = get_nso_client(inst.name)
            await client.list_devices()
            reachable = True
        except Exception:
            pass
        instances.append({"name": inst.name, "reachable": reachable})

    return {"status": "ok", "version": _VERSION, "nso_instances": instances}
