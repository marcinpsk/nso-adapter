# SPDX-License-Identifier: Apache-2.0
"""NSO Adapter — FastAPI application entry point."""

from __future__ import annotations

import asyncio
import inspect
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from nso_adapter import __version__
from nso_adapter.api.actions import router as actions_router
from nso_adapter.api.bfd import router as bfd_router
from nso_adapter.api.bgp import router as bgp_router
from nso_adapter.api.capability import router as capability_router
from nso_adapter.api.config import router as config_router
from nso_adapter.api.devices import router as devices_router
from nso_adapter.api.errors import (
    ApiError,
    api_error,
    api_error_handler,
    framework_http_error_handler,
    projection_gone_handler,
    promotion_provenance_handler,
    unhandled_exception_response,
    validation_error_handler,
)
from nso_adapter.api.health import router as health_router
from nso_adapter.api.intent import router as intent_router
from nso_adapter.api.intent_receipts import router as intent_receipts_router
from nso_adapter.api.interface_ip import router as interface_ip_router
from nso_adapter.api.interface_mtu import router as interface_mtu_router
from nso_adapter.api.interfaces import router as interfaces_router
from nso_adapter.api.isis import router as isis_router
from nso_adapter.api.jobs import router as jobs_router
from nso_adapter.api.l2_service import router as l2_service_router
from nso_adapter.api.lag_config import router as lag_config_router
from nso_adapter.api.lag_topology import router as lag_topology_router
from nso_adapter.api.logging_config import router as logging_config_router
from nso_adapter.api.nso_instances import router as nso_instances_router
from nso_adapter.api.ospf import router as ospf_router
from nso_adapter.api.read_state import router as read_state_router
from nso_adapter.api.redistribution import router as redistribution_router
from nso_adapter.api.route_policy import router as route_policy_router
from nso_adapter.api.scope import router as scope_router
from nso_adapter.api.secrets import router as secrets_router
from nso_adapter.api.snmp import router as snmp_router
from nso_adapter.api.static_route import router as static_route_router
from nso_adapter.api.subinterface import router as subinterface_router
from nso_adapter.api.svi import router as svi_router
from nso_adapter.api.vlan import router as vlan_router
from nso_adapter.config import get_config, get_env_settings
from nso_adapter.core.generation import DeviceProjectionGone
from nso_adapter.core.importer import register_nso_client, set_netbox_client
from nso_adapter.core.receipt import PromotionProvenanceUnexecutable
from nso_adapter.core.request_flags import (
    BACKFILL_ONLY,
    DELETE_ORIGIN,
    MAX_PUSH_SEQ,
    MIN_PUSH_SEQ,
    STORE_ONLY,
    parse_request_flag,
)
from nso_adapter.core.scheduler import start_scheduler, stop_scheduler
from nso_adapter.core.worker import start_workers, stop_workers
from nso_adapter.notifications.persistent_subscriber import persistent_subscriber
from nso_adapter.notifications.sse_subscriber import SSESubscriber
from nso_adapter.nso.client import NsoClient
from nso_adapter.secrets import make_provider
from nso_adapter.store.db import get_engine, init_db, session

logger = structlog.get_logger(__name__)


def _preserve_exact_openapi_integer_bounds(app: FastAPI) -> None:
    """Restore integer bounds that FastAPI coerces to imprecise JSON floats."""
    generated_openapi = app.openapi

    def openapi():
        schema = generated_openapi()
        selected = schema["components"]["schemas"]["ActionApplyIn"]["properties"]["selected"]
        sequence = selected["additionalProperties"]
        sequence["minimum"] = MIN_PUSH_SEQ
        sequence["maximum"] = MAX_PUSH_SEQ
        return schema

    app.openapi = openapi  # type: ignore[method-assign]


def _init_secrets(app: FastAPI, cfg, env):
    """Build the secrets provider, stashing it and the resolved adapter token on ``app.state``."""
    from nso_adapter.core.snmp_verify import register_secrets_provider

    provider = make_provider(cfg, env)
    app.state.secrets = provider
    # The apply/removal WORKERS run outside the request scope, so they cannot reach
    # `request.app.state.secrets`. They need the provider to resolve an SNMP community's
    # vault_ref into the sha256 the device export keys it by (CR-A17) — same module-level
    # registry pattern as the NSO / NetBox clients in core.importer.
    register_secrets_provider(provider)
    app.state.adapter_token = provider.get(cfg.api.adapter_token_ref)
    return provider


async def _init_database(cfg) -> None:
    """Bind the engine/sessionmaker. The schema is NEVER materialised here.

    Alembic is the only schema source: the entrypoint runs `alembic upgrade head` before
    the app starts. A second materialiser in the startup path is the DuplicateTable hazard
    that `alembic stamp head` used to exist to recover from.
    """
    init_db(cfg.database_url)
    # Mint/load the store incarnation (READSEM S4 D3) — idempotent; the migration inserted
    # the row, so this takes its read path.
    from nso_adapter.store.meta import ensure_store_meta

    await ensure_store_meta()
    logger.info("db.ready", url=cfg.database_url)


def _build_nso_clients(cfg, provider) -> dict[str, NsoClient]:
    """Construct and register one NsoClient per configured instance, resolving creds via the provider."""
    nso_clients: dict[str, NsoClient] = {}
    for inst in cfg.nso_instances:
        username = provider.get(inst.username_ref)
        password = provider.get(inst.password_ref)
        client = NsoClient(inst, username, password)
        nso_clients[inst.name] = client
        register_nso_client(inst.name, client)
        logger.info("nso.client.registered", instance=inst.name)
    return nso_clients


def _build_netbox_client(app: FastAPI, cfg, provider):
    """Build the pooled NetBox client, stash it on ``app.state`` and register it with the importer."""
    from nso_adapter.bindings.netbox.client import NetboxClient

    netbox_token = provider.get(cfg.netbox.api_token_ref)
    netbox_client = NetboxClient(
        url=cfg.netbox.base_url,
        token=netbox_token,
        verify=cfg.netbox.ca_cert if cfg.netbox.ca_cert else True,
    )
    app.state.netbox_client = netbox_client
    set_netbox_client(netbox_client)
    return netbox_client


class _DeviceRefreshCoalescer:
    """Per-device coalescing latch for SSE-triggered comprehensive refreshes (S5a D).

    One change event = ONE grain-b projected refresh (a record-served doc GET feeds all
    18 surfaces) instead of the former nine per-family section refreshes. A trigger
    landing mid-refresh sets a dirty edge consumed by exactly one rerun; both success
    AND ordinary failure reach the dirty check (codex R2-F5), and cancellation never
    consumes an edge. Real refresh tasks register in the lifespan's dispatch_tasks set
    so shutdown cancellation reaches them (codex R1-F6). Single-event-loop discipline:
    latch mutations only in synchronous sections.
    """

    def __init__(self, clients: dict[str, NsoClient], dispatch_tasks: set, on_done) -> None:
        self._clients = clients
        self._tasks = dispatch_tasks
        self._on_done = on_done
        self._state: dict[int, dict] = {}

    def trigger(self, device_id: int, nso_instance: str, netbox_device_id: int | None) -> None:
        st = self._state.setdefault(device_id, {"running": False, "dirty": False})
        if st["running"]:
            st["dirty"] = True
            return
        st["running"] = True
        task = asyncio.create_task(self._run(device_id, nso_instance, netbox_device_id))
        self._tasks.add(task)
        task.add_done_callback(self._discard)

    def _discard(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if callable(self._on_done):
            self._on_done(task)

    async def _run(self, device_id: int, nso_instance: str, netbox_device_id: int | None) -> None:
        st = self._state[device_id]
        try:
            while True:
                try:
                    await self._refresh_once(device_id, nso_instance, netbox_device_id)
                except asyncio.CancelledError:
                    raise  # shutdown: no respawn, no dirty consumption
                except Exception as exc:  # noqa: BLE001 — fallthrough: the dirty check still runs
                    logger.warning("sse.coalesced_refresh_failed", device_id=device_id, error=repr(exc))
                if st["dirty"]:  # synchronous check-and-transition — no awaits between
                    st["dirty"] = False
                    continue
                break
        finally:
            st["running"] = False

    async def _refresh_once(self, device_id: int, nso_instance: str, netbox_device_id: int | None) -> None:
        from nso_adapter.core.importer import get_netbox_client, refresh_all_surfaces_for_device
        from nso_adapter.store.models import Device

        client = self._clients.get(nso_instance)
        if client is None:
            return
        async with session() as db:
            device = await db.get(Device, device_id)  # RE-FETCH by id — never a foreign session's row
            if device is None:
                return
            await refresh_all_surfaces_for_device(db, device, client, refresh_source="notification", atomic=False)
        # Notify AFTER the refresh and BEFORE the dirty check (codex R1-F5 ordering): the
        # plugin reconciles the refreshed mirror; failures are swallowed (best-effort).
        nb_client = get_netbox_client()
        if nb_client is not None and netbox_device_id:
            try:
                await nb_client.notify_sync_complete(netbox_device_id)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "sse.notify_failed", netbox_device_id=netbox_device_id, error=str(exc) or type(exc).__name__
                )


async def _dispatch_netconf_change(
    cfg, parsed: dict, db, clients: dict[str, NsoClient], coalescer: _DeviceRefreshCoalescer
) -> None:
    """S5a D: per changed device, ONE coalesced comprehensive refresh (grain b).

    Replaces the former nine per-family handler calls — cfg enable flags apply inside
    the surface builders. Device resolution is scoped to THIS handler's instance map
    (codex R1-F8: nso_device_name is not globally unique across NSO instances).
    """
    from sqlalchemy import select

    from nso_adapter.core.lag_topology import parse_changed_nso_devices
    from nso_adapter.store.models import Device

    changed = parse_changed_nso_devices(parsed)
    if not changed:
        return
    devices = (
        (
            await db.execute(
                select(Device).where(Device.nso_device_name.in_(changed), Device.nso_instance.in_(list(clients.keys())))
            )
        )
        .scalars()
        .all()
    )
    for device in devices:
        coalescer.trigger(device.id, device.nso_instance, device.netbox_device_id)


def _make_sse_event_handler(
    cfg,
    clients: dict[str, NsoClient],
    dispatch_tasks: set[asyncio.Task],
    coalescer: _DeviceRefreshCoalescer | None = None,
):
    """Build the SSE on-event callback: ignore unparseable frames, else dispatch on its own task/session.

    Each dispatch task is retained in *dispatch_tasks* (a bare create_task can be garbage-
    collected mid-flight and its exception swallowed) with a done-callback that logs failures
    and discards it; the set is cancelled at shutdown — and the coalescer registers its
    REAL refresh tasks in the same set, so shutdown reaches them too (S5a D, codex R1-F6).
    """

    def _on_done(task: asyncio.Task) -> None:
        dispatch_tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.warning("sse.dispatch_failed", error=repr(task.exception()))

    if coalescer is None:
        coalescer = _DeviceRefreshCoalescer(clients, dispatch_tasks, _on_done)

    def on_event(raw: str, parsed: dict | None) -> None:
        if parsed is None:
            return

        async def _run() -> None:
            async with session() as db:
                await _dispatch_netconf_change(cfg, parsed, db, clients, coalescer)

        task = asyncio.create_task(_run())
        dispatch_tasks.add(task)
        task.add_done_callback(_on_done)

    return on_event


def _start_sse_streams(
    cfg,
    provider,
    nso_clients: dict[str, NsoClient],
    sse_stop: asyncio.Event,
    dispatch_tasks: set[asyncio.Task],
) -> list[asyncio.Task]:
    """Start one persistent NETCONF SSE subscriber per instance (when enabled); return the spawned tasks."""
    sse_tasks: list[asyncio.Task] = []
    if not cfg.scheduler.enable_nso_streams:
        return sse_tasks
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
                _make_sse_event_handler(cfg, {inst.name: nso_clients[inst.name]}, dispatch_tasks),
                stop_event=sse_stop,
            )
        )
        sse_tasks.append(task)
        logger.info("sse.stream.started", instance=inst.name, url=stream_url)
    return sse_tasks


async def _shutdown_sse(
    sse_stop: asyncio.Event, sse_tasks: list[asyncio.Task], dispatch_tasks: set[asyncio.Task]
) -> None:
    """Signal stop, cancel every SSE subscriber + in-flight dispatch task, and drain (bounded)."""
    sse_stop.set()
    for task in (*sse_tasks, *tuple(dispatch_tasks)):
        task.cancel()
    for task in (*sse_tasks, *tuple(dispatch_tasks)):
        try:
            await asyncio.wait_for(task, timeout=5.0)
        except (asyncio.CancelledError, TimeoutError):
            pass


async def _close_netbox(netbox_client) -> None:
    """Close the pooled NetBox client, guarding with isawaitable so a mocked client doesn't break teardown."""
    maybe = netbox_client.aclose()
    if inspect.isawaitable(maybe):
        await maybe


async def _dispose_engine() -> None:
    """Dispose the SQLAlchemy engine if one was initialised."""
    engine = get_engine()
    if engine:
        await engine.dispose()


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = get_config()
    env = get_env_settings()

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(__import__("logging").getLevelName(cfg.log_level)),
    )

    provider = _init_secrets(app, cfg, env)
    await _init_database(cfg)

    nso_clients = _build_nso_clients(cfg, provider)
    app.state.nso_clients = nso_clients
    netbox_client = _build_netbox_client(app, cfg, provider)

    sse_stop = asyncio.Event()
    sse_dispatch_tasks: set[asyncio.Task] = set()
    sse_tasks = _start_sse_streams(cfg, provider, nso_clients, sse_stop, sse_dispatch_tasks)
    app.state.sse_stop = sse_stop
    app.state.sse_tasks = sse_tasks
    app.state.sse_dispatch_tasks = sse_dispatch_tasks

    # Start the durable worker pool first: it reconciles orphaned jobs from a
    # previous process (requeue idempotent / fail interrupted apply) before the
    # scheduler begins enqueuing fresh work.
    await start_workers(cfg.scheduler.worker_concurrency)

    start_scheduler()
    try:
        yield
    finally:
        stop_scheduler()
        await stop_workers()
        await _shutdown_sse(sse_stop, sse_tasks, sse_dispatch_tasks)
        await _close_netbox(netbox_client)
        await _dispose_engine()


def create_app() -> FastAPI:
    app = FastAPI(title="NSO Adapter", version=__version__, lifespan=lifespan)
    app.add_exception_handler(ApiError, api_error_handler)
    app.add_exception_handler(StarletteHTTPException, framework_http_error_handler)
    app.add_exception_handler(RequestValidationError, validation_error_handler)
    app.add_exception_handler(DeviceProjectionGone, projection_gone_handler)
    app.add_exception_handler(PromotionProvenanceUnexecutable, promotion_provenance_handler)

    @app.middleware("http")
    async def _request_mode_flags(request, call_next):
        # ?store_only=true → this request must not create device-touching jobs
        # (removal/apply); ?delete_origin=true → this intent push comes from a NetBox
        # object DELETION, so a shrink may retract from the device (unmarked shrinks
        # detach instead, #106). Both guarded at the enqueue choke points in core.
        # See core/request_flags.py for why these are request-scoped, not per-endpoint.
        # The third piece of a delivery's identity, X-Push-Seq, is NOT parsed here: it is a
        # declared parameter of every in-protocol intent PUT and nothing else consumes it,
        # so it lives on the delivery dependency where OpenAPI can see it.
        modes = {}
        for parameter in ("store_only", "delete_origin", "backfill_only"):
            raw = request.query_params.get(parameter)
            try:
                modes[parameter] = parse_request_flag(raw)
            except ValueError:
                return await api_error_handler(
                    request,
                    api_error(
                        422,
                        "validation_error",
                        f"{parameter} must be a boolean",
                        {"parameter": parameter},
                    ),
                )

        token = STORE_ONLY.set(modes["store_only"])
        del_token = DELETE_ORIGIN.set(modes["delete_origin"])
        # ?backfill_only=true → an id-backfill pass that opens a device's replacement fence and
        # writes nothing else (#1503 §4.4). Parsed here with its two siblings so the three
        # request modes have one spelling; only the static-route stream implements it.
        backfill_token = BACKFILL_ONLY.set(modes["backfill_only"])
        try:
            return await call_next(request)
        finally:
            BACKFILL_ONLY.reset(backfill_token)
            DELETE_ORIGIN.reset(del_token)
            STORE_ONLY.reset(token)

    # Registered LAST so it is the OUTERMOST user middleware — Starlette inserts each one at
    # the front of the stack — and therefore also covers the middleware above.
    @app.middleware("http")
    async def _seal_unhandled_exception(request, call_next):
        # NOT add_exception_handler(Exception, ...): that routes to ServerErrorMiddleware,
        # which RE-RAISES after responding, and the ASGI server then logs the raw traceback
        # — the one copy nobody redacts. Answering here re-raises nothing. Specific handlers
        # sit deeper (ExceptionMiddleware) and still win; this is the last resort.
        try:
            return await call_next(request)
        except Exception as exc:
            return unhandled_exception_response(request, exc)

    app.include_router(health_router)
    app.include_router(nso_instances_router)
    app.include_router(devices_router)
    app.include_router(scope_router)
    app.include_router(intent_router)
    app.include_router(intent_receipts_router)
    app.include_router(actions_router)
    app.include_router(interfaces_router)
    app.include_router(lag_topology_router)
    app.include_router(lag_config_router)
    app.include_router(vlan_router)
    app.include_router(interface_ip_router)
    app.include_router(snmp_router)
    app.include_router(logging_config_router)
    app.include_router(svi_router)
    app.include_router(subinterface_router)
    app.include_router(interface_mtu_router)
    app.include_router(static_route_router)
    app.include_router(l2_service_router)
    app.include_router(isis_router)
    app.include_router(bfd_router)
    app.include_router(bgp_router)
    app.include_router(read_state_router)
    app.include_router(route_policy_router)
    app.include_router(capability_router)
    app.include_router(secrets_router)
    app.include_router(ospf_router)
    app.include_router(redistribution_router)
    app.include_router(jobs_router)
    app.include_router(config_router)
    _preserve_exact_openapi_integer_bounds(app)
    return app


app = create_app()
