# SPDX-License-Identifier: Apache-2.0
"""NSO Adapter — FastAPI application entry point."""
from __future__ import annotations

import asyncio
import inspect
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from nso_adapter.api.actions import router as actions_router
from nso_adapter.api.bgp import router as bgp_router
from nso_adapter.api.devices import router as devices_router
from nso_adapter.api.errors import ApiError, api_error_handler
from nso_adapter.api.health import router as health_router
from nso_adapter.api.intent import router as intent_router
from nso_adapter.api.interface_ip import router as interface_ip_router
from nso_adapter.api.interfaces import router as interfaces_router
from nso_adapter.api.isis import router as isis_router
from nso_adapter.api.jobs import router as jobs_router
from nso_adapter.api.lag_topology import router as lag_topology_router
from nso_adapter.api.nso_instances import router as nso_instances_router
from nso_adapter.api.ospf import router as ospf_router
from nso_adapter.api.redistribution import router as redistribution_router
from nso_adapter.api.route_policy import router as route_policy_router
from nso_adapter.api.scope import router as scope_router
from nso_adapter.api.snmp import router as snmp_router
from nso_adapter.api.static_route import router as static_route_router
from nso_adapter.config import get_config, get_env_settings
from nso_adapter.core.importer import register_nso_client, set_netbox_client
from nso_adapter.core.interface_ip import handle_interface_ip_change
from nso_adapter.core.lag_topology import handle_netconf_config_change
from nso_adapter.core.scheduler import start_scheduler, stop_scheduler
from nso_adapter.core.snmp import handle_snmp_config_change
from nso_adapter.notifications.persistent_subscriber import persistent_subscriber
from nso_adapter.notifications.sse_subscriber import SSESubscriber
from nso_adapter.nso.client import NsoClient
from nso_adapter.secrets import make_provider
from nso_adapter.store.db import get_engine, init_db
from nso_adapter.store.models import Base

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = get_config()
    env = get_env_settings()

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(
            __import__("logging").getLevelName(cfg.log_level)
        ),
    )

    # Init secrets provider — all further secret access goes through this
    provider = make_provider(cfg, env)
    app.state.secrets = provider
    app.state.adapter_token = provider.get(cfg.api.adapter_token_ref)

    # Init DB
    init_db(cfg.database_url)
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)  # pragma: no cover
    logger.info("db.ready", url=cfg.database_url)

    # Clean up any jobs that were left in running/queued state from a previous
    # process (e.g. adapter container restarted mid-job).  Must run before the
    # scheduler starts so get_active_job() doesn't block on stale entries.
    from sqlalchemy import update as sa_update

    from nso_adapter.store.db import get_session
    from nso_adapter.store.models import Job, JobStatus

    async for db in get_session():
        result = await db.execute(
            sa_update(Job)
            .where(Job.status.in_([JobStatus.running, JobStatus.queued]))
            .values(
                status=JobStatus.failed,
                error={
                    "code": "orphaned",
                    "message": "Adapter restarted while job was running",
                    "detail": {},
                },
            )
        )
        await db.commit()  # pragma: no cover
        if result.rowcount:  # pragma: no cover
            logger.warning("jobs.orphaned_cleanup", count=result.rowcount)  # pragma: no cover

    # Build NSO clients — resolve username + password per instance
    from nso_adapter.store.db import get_session

    nso_clients: dict[str, NsoClient] = {}
    for inst in cfg.nso_instances:
        username = provider.get(inst.username_ref)
        password = provider.get(inst.password_ref)
        client = NsoClient(inst, username, password)
        nso_clients[inst.name] = client
        register_nso_client(inst.name, client)
        logger.info("nso.client.registered", instance=inst.name)
    app.state.nso_clients = nso_clients

    # Build NetBox client and register with importer
    from nso_adapter.bindings.netbox.client import NetboxClient
    netbox_token = provider.get(cfg.netbox.api_token_ref)
    netbox_client = NetboxClient(
        url=cfg.netbox.base_url,
        token=netbox_token,
    )
    app.state.netbox_client = netbox_client
    set_netbox_client(netbox_client)

    sse_tasks: list[asyncio.Task] = []
    sse_stop = asyncio.Event()
    app.state.sse_stop = sse_stop
    app.state.sse_tasks = sse_tasks

    def make_handler(clients: dict[str, NsoClient]):
        def on_event(raw: str, parsed: dict | None) -> None:
            if parsed is None:
                return

            async def _handle() -> None:
                async for db in get_session():
                    await handle_netconf_config_change(parsed, db, clients)
                    if cfg.scheduler.enable_interface_ip_sync:
                        await handle_interface_ip_change(parsed, db, clients)
                    if cfg.scheduler.enable_snmp_sync:
                        await handle_snmp_config_change(parsed, db, clients)

            asyncio.create_task(_handle())

        return on_event

    if cfg.scheduler.enable_nso_streams:
        for inst in cfg.nso_instances:
            username = provider.get(inst.username_ref)
            password = provider.get(inst.password_ref)
            subscriber = SSESubscriber(
                base_url=inst.base_url,
                auth=(username, password),
                host_header=inst.host_header,
                verify=inst.ca_cert if inst.ca_cert else True,
            )
            stream_url = f"{inst.base_url.rstrip('/')}/restconf/streams/NETCONF/json"
            task = asyncio.create_task(
                persistent_subscriber(
                    subscriber,
                    stream_url,
                    make_handler({inst.name: nso_clients[inst.name]}),
                    stop_event=sse_stop,
                )
            )
            sse_tasks.append(task)
            logger.info("sse.stream.started", instance=inst.name, url=stream_url)

    start_scheduler()
    try:
        yield
    finally:
        stop_scheduler()
        sse_stop.set()
        for task in sse_tasks:
            task.cancel()
        for task in sse_tasks:
            try:
                await asyncio.wait_for(task, timeout=5.0)
            except (asyncio.CancelledError, TimeoutError):
                pass

        # Close the pooled NetBox HTTP client. Guarded with inspect.isawaitable
        # so a test-mocked client (plain MagicMock) doesn't break teardown.
        maybe = netbox_client.aclose()
        if inspect.isawaitable(maybe):
            await maybe

        engine = get_engine()
        if engine:
            await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="NSO Adapter", version="0.1.0", lifespan=lifespan)
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(health_router)
    app.include_router(nso_instances_router)
    app.include_router(devices_router)
    app.include_router(scope_router)
    app.include_router(intent_router)
    app.include_router(actions_router)
    app.include_router(interfaces_router)
    app.include_router(lag_topology_router)
    app.include_router(interface_ip_router)
    app.include_router(snmp_router)
    app.include_router(static_route_router)
    app.include_router(isis_router)
    app.include_router(bgp_router)
    app.include_router(route_policy_router)
    app.include_router(ospf_router)
    app.include_router(redistribution_router)
    app.include_router(jobs_router)
    return app


app = create_app()

