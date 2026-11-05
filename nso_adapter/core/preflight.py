# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Detached manual-Apply plans built in an owned, rollback-only transaction."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal, NamedTuple
from uuid import uuid4

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from nso_adapter.core.apply import _apply_error_summary, build_device_containers, encode_section
from nso_adapter.core.generation import (
    LAST_ENQUEUED_GENERATION_ID,
    ExecutionPolicy,
    _authorized_fragments,
    _compose_document,
    _selected_promotions,
    create_action_apply,
    create_reissue_generation,
    execution_policy,
    generation_execution_context,
    lock_projection,
)
from nso_adapter.core.projection import (
    InterfaceProofConflict,
    fragment_tables,
    projection_streams,
    rows_by_intent_identity,
    section_proof,
    section_registry,
    section_streams,
)
from nso_adapter.core.removal import _leaf_keys, evaluate_collateral_guard, guard_allowed
from nso_adapter.core.static_route_plan import hydrate_static_route_apply_plan, hydrate_static_route_removal_plan
from nso_adapter.core.static_route_reader import CertifiedSection, certified_static_route_section
from nso_adapter.nso.apply import NsoApplyError, apply_device_intent
from nso_adapter.nso.origins import AttributionFailure, OriginContext, OriginIndex, resolve_refusals
from nso_adapter.nso.refusals import DefiniteRefusal, OrdinaryRejection, ProtocolFault, Unavailable
from nso_adapter.store.execution_evidence import attributed_catalog_records, origin_records, origins_from_records
from nso_adapter.store.execution_history import bind_historical_origins
from nso_adapter.store.models import Base, DbInterface, Device, DeviceProjectionStream, GenerationMode


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


class LivePlanInputs(NamedTuple):
    section: CertifiedSection
    digest: str | None


async def read_plan_inputs(client, device) -> LivePlanInputs:
    """Read once outside the planning transaction; an inconclusive read has no digest."""
    try:
        section = await certified_static_route_section(client, device)
    except httpx.HTTPError:
        return LivePlanInputs(CertifiedSection("inconclusive", None), None)
    digest = None if section.inconclusive else _digest(section._asdict())
    return LivePlanInputs(section, digest)


@dataclass(frozen=True)
class PlannedLink:
    """Immutable JSON capture; each accessor returns an independent detached copy."""

    _json: str

    @property
    def order(self) -> int:
        return json.loads(self._json)["order"]

    @property
    def mode(self) -> GenerationMode:
        return GenerationMode(json.loads(self._json)["mode"])

    @property
    def streams(self) -> tuple[str, ...]:
        return tuple(json.loads(self._json)["streams"])

    @property
    def digest(self) -> str:
        return json.loads(self._json)["digest"]

    @property
    def document(self) -> dict:
        return json.loads(self._json)["document"]

    @property
    def containers(self) -> dict:
        return json.loads(self._json)["containers"]

    @property
    def policy(self) -> ExecutionPolicy:
        return ExecutionPolicy(**json.loads(self._json)["policy"])

    @property
    def origins(self) -> OriginIndex:
        """Rehydrate attribution evidence with local, non-persistent generation markers."""
        return origins_from_records(json.loads(self._json)["origins"])

    @property
    def allowed_removal_keys(self) -> dict:
        return json.loads(self._json)["allowed_removal_keys"]

    @property
    def guard_orphans(self) -> dict | None:
        return json.loads(self._json)["guard_orphans"]

    @property
    def guard_contributions(self) -> dict:
        return json.loads(self._json)["guard_contributions"]

    @property
    def errors(self) -> dict:
        return json.loads(self._json)["errors"]


@dataclass(frozen=True)
class PlannedChain:
    links: tuple[PlannedLink, ...]
    _json: str
    _candidate_json: str | None = None

    @property
    def candidate_identity(self) -> str | None:
        return json.loads(self._candidate_json)["identity"] if self._candidate_json is not None else None

    @property
    def candidate_selection(self) -> dict[str, int] | None:
        return json.loads(self._candidate_json)["selection"] if self._candidate_json is not None else None

    @property
    def skipped(self) -> dict[str, str]:
        return json.loads(self._json)["skipped"]

    @property
    def skipped_detail(self) -> dict:
        return json.loads(self._json)["skipped_detail"]

    @property
    def baseline_interface_proofs(self) -> dict:
        """Composed baseline records referenced by each authorized interface stream."""
        return json.loads(self._json)["baseline_interface_proofs"]


@dataclass(frozen=True)
class PreflightResult:
    """Detached admission evidence; mutable accessors return independent copies."""

    status: Literal["ready", "refused", "unavailable", "error"]
    plan: PlannedChain | None
    _json: str

    @property
    def streams(self) -> dict[str, dict]:
        return json.loads(self._json)["streams"]

    @property
    def admitted_selection(self) -> dict[str, int]:
        return json.loads(self._json)["admitted_selection"]

    @property
    def skips(self) -> dict[str, str]:
        return json.loads(self._json)["skips"]

    @property
    def attempts(self) -> list[dict]:
        return json.loads(self._json)["attempts"]

    @property
    def deltas(self) -> list[dict]:
        return json.loads(self._json)["deltas"]

    @property
    def live_input_digest(self) -> str | None:
        return json.loads(self._json)["live_input_digest"]

    @property
    def baseline_refused(self) -> list[str]:
        return json.loads(self._json)["baseline_refused"]

    @property
    def cross_stream(self) -> list[str]:
        return json.loads(self._json)["cross_stream"]

    @property
    def context_disturbed(self) -> list[str]:
        return json.loads(self._json)["context_disturbed"]

    @property
    def error(self) -> dict | None:
        return json.loads(self._json)["error"]

    @property
    def refusals(self) -> list[dict]:
        return json.loads(self._json)["refusals"]


class _DryRun(NamedTuple):
    status: Literal["ready", "refused", "unavailable", "error"]
    deltas: list[dict]
    refusals: list[dict]
    error: dict | None = None


class _PlanContextConflict(ValueError):
    def __init__(self, streams: tuple[str, ...], promoted: set[str], skipped: dict[str, str]):
        self.disturbed = sorted(set(streams) - promoted)
        self.blocks = {
            stream: {"status": "blocked", "context_disturbed": self.disturbed} for stream in set(streams) & promoted
        }
        self.skipped = skipped
        super().__init__("Manual selection changes authorized interface context")


async def _dry_run_link(client, device, link: PlannedLink) -> _DryRun:
    if link.errors:
        return _DryRun("error", [], [], {"code": "body_build_failed", "sections": link.errors})
    delta_record: dict = {"order": link.order}
    formats = (True,) if link.policy.no_networking else (True, "cli")
    for fmt in formats:
        try:
            delta = await apply_device_intent(
                client,
                device.nso_device_name,
                link.containers,
                device_id=device.id,
                dry_run=fmt,
                no_networking=link.policy.no_networking,
                strict=True,
            )
        except NsoApplyError as exc:
            classification = exc.classification
            if isinstance(classification, (Unavailable, ProtocolFault)):
                return _DryRun("unavailable", [], [])
            if not isinstance(classification, DefiniteRefusal):
                message = (
                    "NSO rejected the device document"
                    if isinstance(classification, OrdinaryRejection)
                    else _apply_error_summary(exc)
                )
                return _DryRun("error", [], [], {"code": exc.code, "message": message})
            origins = link.origins
            attributed = resolve_refusals(classification.records, origins)
            if isinstance(attributed, AttributionFailure):
                return _DryRun("error", [], [], {"code": "attribution_failed", "reason": attributed.reason})
            records = attributed_catalog_records(attributed, origins, "pre_commit_dry_run")
            return _DryRun("refused", [], records)
        if delta is None:
            return _DryRun("unavailable", [], [])
        delta_record["cli" if fmt == "cli" else "native"] = delta
    return _DryRun("ready", [] if link.policy.no_networking else [delta_record], [])


async def _dry_run_chain(client, device, planned: PlannedChain) -> _DryRun:
    """Check every ready link; a refused link requires immediate recomposition."""
    deltas: list[dict] = []
    previous = None
    for link in planned.links:
        sent = (link.containers, link.policy.no_networking)
        if sent == previous and not link.errors:
            continue  # in order this link resends the preceding body unchanged, so NSO commits nothing
        previous = sent
        outcome = await _dry_run_link(client, device, link)
        if outcome.status != "ready":
            return outcome
        deltas.extend(outcome.deltas)
    return _DryRun("ready", deltas, [])


def _refusal_streams(records: list[dict]) -> set[str]:
    return {row["stream"] for record in records for row in record["logical_rows"]}


def _refusal_blocks(records: list[dict], current: dict[str, int]) -> dict[str, dict]:
    return {
        stream: {
            "status": "blocked",
            "refusals": [record for record in records if stream in _refusal_streams([record])],
        }
        for stream in _refusal_streams(records) & current.keys()
    }


def _disturbed_streams(planned: PlannedChain) -> list[str]:
    if not planned.baseline_interface_proofs:
        return []
    promoted = {stream for link in planned.links for stream in link.streams}
    disturbed = set()
    for link in planned.links:
        proof = section_proof(link.document, "interface_config") or {}
        records = proof.get("interfaces", {})
        for stream, baseline in planned.baseline_interface_proofs.items():
            if stream not in promoted and any(records.get(key) != record for key, record in baseline.items()):
                disturbed.add(stream)
    return sorted(disturbed)


def _guard_contributions(authorized: dict[str, dict], containers: dict, orphans: dict | None) -> dict:
    """Capture lost guard keys from each stream's frozen authorized contribution."""
    contributions: dict = {}
    for label in orphans or {}:
        section, guarded = label.split("/", 1)
        entry = section_registry()[section]
        guard = next(guard for guard in entry.guard_lists if guard.label == guarded)
        planned = _leaf_keys(section, containers.get(entry.container) or {}, guard)
        for stream in section_streams(section) & authorized.keys():
            document = _compose_document({stream: authorized[stream]})
            try:
                baseline = encode_section(document, section).body
            except NsoApplyError:
                # An unbuildable contribution cannot prove ownership of an orphan.
                continue
            lost = _leaf_keys(section, baseline, guard) - planned
            if lost:
                contributions.setdefault(label, {})[stream] = [list(key) for key in sorted(lost)]
    return contributions


def _guard_blocks(planned: PlannedChain, current: dict[str, int]) -> tuple[dict[str, dict], dict]:
    blocks: dict[str, dict] = {}
    unselected: dict = {}
    for link in planned.links:
        contributions = link.guard_contributions
        for label, keys in (link.guard_orphans or {}).items():
            for key in keys:
                named = {
                    stream for stream, lost in contributions.get(label, {}).items() if key in lost and stream in current
                }
                if not named:
                    unselected.setdefault(label, []).append(key)
                for stream in named:
                    blocks.setdefault(stream, {"status": "blocked", "guard": {}})["guard"].setdefault(label, []).append(
                        key
                    )
    return blocks, unselected


def preflight_identity(result: PreflightResult) -> str:
    """Bind the ready chain and live snapshot to the manual admission inputs."""
    if result.status != "ready" or result.plan is None or result.live_input_digest is None:
        raise ValueError("Preflight identity requires a ready plan")
    captured = json.loads(result._json)
    return manual_apply_identity(
        device_id=captured["device_id"],
        nso_instance=captured["nso_instance"],
        nso_device_name=captured["nso_device_name"],
        selected=result.admitted_selection,
        skips=result.skips,
        links=[(link.mode, link.digest, link.policy) for link in result.plan.links],
        live_input_digest=result.live_input_digest,
    )


def manual_apply_identity(
    *,
    device_id: int,
    nso_instance: str,
    nso_device_name: str,
    selected: dict[str, int],
    skips: dict[str, str],
    links: list[tuple[GenerationMode, str, ExecutionPolicy]],
    live_input_digest: str,
) -> str:
    """Hash the same executable inputs for preview and promotion."""
    return _digest(
        {
            "device_id": device_id,
            "nso_instance": nso_instance,
            "nso_device_name": nso_device_name,
            "admitted_selection": selected,
            "skips": skips,
            "links": [
                {"mode": mode.value, "digest": digest, "policy": policy._asdict()} for mode, digest, policy in links
            ],
            "live_input_digest": live_input_digest,
        }
    )


class PreflightStale(ValueError):
    """The manual admission no longer describes the executable inputs."""

    def __init__(self, reason: str):
        super().__init__("Manual Apply needs a fresh preflight")
        self.reason = reason


def _device_context(device) -> tuple[str, str, str | None]:
    return device.nso_instance, device.nso_device_name, device.ned_id


async def _diagnose_unselected_refusal(engine, client, device, live, outcome: _DryRun) -> dict:
    try:
        baseline = await plan_action_apply(
            engine, device.id, {}, live.section, expected_context=_device_context(device), baseline_only=True
        )
    except PreflightStale:
        return {"status": "unavailable", "error": _INPUTS_MOVED}
    diagnosis = await _dry_run_chain(client, device, baseline)
    if diagnosis.status in {"unavailable", "error"}:
        return {"status": diagnosis.status, "outcome": diagnosis}
    if diagnosis.status == "refused":
        return {
            "status": "refused",
            "baseline_refused": sorted(_refusal_streams(diagnosis.refusals)),
            "outcome": diagnosis,
        }
    return {"status": "refused", "cross_stream": sorted(_refusal_streams(outcome.refusals)), "outcome": outcome}


def _context_blocks(planned: PlannedChain, promoted: set[str]) -> tuple[dict[str, dict], list[str]]:
    """Block the promoted interface-section streams when an unpromoted one would change context."""
    disturbed = _disturbed_streams(planned)
    if not disturbed:
        return {}, []
    blocked = promoted & {"interface_config", "ip"}
    return {stream: {"status": "blocked", "context_disturbed": disturbed} for stream in blocked}, disturbed


async def _judge_dry_run(engine, client, device, live, planned, effective) -> tuple[dict[str, dict], dict | None]:
    """Dry-run one pass: return the selected streams to drop, or the final result arguments."""
    outcome = await _dry_run_chain(client, device, planned)
    if outcome.status in {"unavailable", "error"}:
        return {}, {"status": outcome.status, "outcome": outcome}
    if outcome.status == "ready":
        return {}, {"status": "ready", "plan": planned, "outcome": outcome}
    blocks = _refusal_blocks(outcome.refusals, effective)
    if blocks:
        return blocks, None
    return {}, await _diagnose_unselected_refusal(engine, client, device, live, outcome)


#: A selection changed between two passes of one preflight; the caller retries.
_INPUTS_MOVED = {"code": "inputs_changed_during_preflight"}


def _skips_moved(earlier: dict[str, str], later: dict[str, str]) -> bool:
    return any(later.get(stream) != reason for stream, reason in earlier.items())


CandidateIngest = Callable[[AsyncSession], Awaitable[tuple[dict[str, int], dict[str, dict]]]]


def _preflight_result(
    device,
    live,
    streams,
    current,
    skips,
    attempts,
    status,
    *,
    plan=None,
    outcome=None,
    baseline_refused=(),
    cross_stream=(),
    context_disturbed=(),
    error=None,
) -> PreflightResult:
    admitted = (
        {stream: revision for stream, revision in current.items() if stream not in skips} if status == "ready" else {}
    )
    verdicts = {stream: dict(verdict) for stream, verdict in streams.items()}
    for stream in admitted:
        verdicts[stream] = {"status": "admitted"}
    return PreflightResult(
        status,
        plan if status == "ready" else None,
        _canonical(
            {
                "device_id": device.id,
                "nso_instance": device.nso_instance,
                "nso_device_name": device.nso_device_name,
                "streams": verdicts,
                "admitted_selection": admitted,
                "skips": skips,
                "attempts": attempts,
                "deltas": outcome.deltas if status == "ready" and outcome else [],
                "live_input_digest": live.digest,
                "baseline_refused": baseline_refused,
                "cross_stream": cross_stream,
                "context_disturbed": context_disturbed,
                "error": error or (outcome.error if outcome else None),
                "refusals": outcome.refusals if outcome else [],
            }
        ),
    )


async def run_preflight(
    engine: AsyncEngine, client, device, selected: dict[str, int], *, ingest: CandidateIngest | None = None
) -> PreflightResult:
    """Evaluate a stored selection or an exact candidate through the shared planner."""
    if ingest is not None:
        return await _run_candidate_preflight(engine, client, device, selected, ingest)
    return await _run_selected_preflight(engine, client, device, selected)


async def _run_candidate_preflight(engine, client, device, selected, ingest) -> PreflightResult:
    live = await read_plan_inputs(client, device)
    streams: dict[str, dict] = {stream: {"status": "not_evaluated"} for stream in selected}
    attempts: list[dict] = []
    current = dict(selected)
    skips: dict[str, str] = {}

    def finish(status, **kwargs):
        return _preflight_result(device, live, streams, current, skips, attempts, status, **kwargs)

    if live.section.inconclusive:
        return finish("unavailable")
    attempts.append({"selection": current, "dropped": []})
    try:
        planned = await plan_action_apply(
            engine,
            device.id,
            current,
            live.section,
            ingest=ingest,
            expected_context=_device_context(device),
        )
    except PreflightStale:
        return finish("unavailable", error=_INPUTS_MOVED)
    except _PlanContextConflict as conflict:
        streams.update(conflict.blocks)
        return finish("refused", context_disturbed=conflict.disturbed)
    assert planned.candidate_selection is not None
    current = planned.candidate_selection
    skips = planned.skipped
    attempts[-1]["selection"] = current
    promoted = {stream for link in planned.links for stream in link.streams}
    effective = {stream: current[stream] for stream in promoted}
    blocks, disturbed = _context_blocks(planned, promoted)
    guard_blocks, unselected_guard = _guard_blocks(planned, effective)
    blocks.update(guard_blocks)
    streams.update(blocks)
    if unselected_guard:
        return finish("refused", error={"code": "removal_blocked_collateral", "orphans": unselected_guard})
    if disturbed or blocks:
        return finish("refused", context_disturbed=disturbed)
    blocks, final = await _judge_dry_run(engine, client, device, live, planned, effective)
    if final is not None:
        return finish(**final)
    streams.update(blocks)
    records = {_canonical(record): record for block in blocks.values() for record in block.get("refusals", [])}
    return finish("refused", outcome=_DryRun("refused", [], list(records.values())))


async def _run_selected_preflight(engine: AsyncEngine, client, device, selected: dict[str, int]) -> PreflightResult:
    """Admit a manual selection without storing a promotion or committing to NSO."""
    current = dict(selected)
    streams: dict[str, dict] = {stream: {"status": "not_evaluated"} for stream in selected}
    attempts: list[dict] = []
    skips: dict[str, str] = {}
    live = await read_plan_inputs(client, device)

    def finish(
        status, *, plan=None, outcome=None, baseline_refused=(), cross_stream=(), context_disturbed=(), error=None
    ):
        return _preflight_result(
            device,
            live,
            streams,
            current,
            skips,
            attempts,
            status,
            plan=plan,
            outcome=outcome,
            baseline_refused=baseline_refused,
            cross_stream=cross_stream,
            context_disturbed=context_disturbed,
            error=error,
        )

    def drop(blocks):
        assert blocks and blocks.keys() <= current.keys(), "Preflight can only drop selected streams"
        streams.update(blocks)
        attempts[-1]["dropped"] = sorted(blocks)
        # Skipped selections stay: Apply confirms them, and some skips still change authorized metadata.
        return {stream: revision for stream, revision in current.items() if stream not in blocks}

    if live.section.inconclusive:
        return finish("unavailable")
    for _ in range(len(selected) + 1):
        assert len(attempts) <= len(selected), "Preflight exceeded its pass bound"
        attempts.append({"selection": dict(current), "dropped": []})
        if not current and streams:
            return finish("refused")
        try:
            planned = await plan_action_apply(
                engine, device.id, current, live.section, expected_context=_device_context(device)
            )
        except PreflightStale:
            return finish("unavailable", error=_INPUTS_MOVED)
        except _PlanContextConflict as conflict:
            if _skips_moved(skips, conflict.skipped):
                return finish("unavailable", error=_INPUTS_MOVED)
            skips.update(conflict.skipped)
            if not conflict.blocks:
                return finish("refused", context_disturbed=conflict.disturbed)
            current = drop(conflict.blocks)
            continue
        if _skips_moved(skips, planned.skipped):
            return finish("unavailable", error=_INPUTS_MOVED)
        skips.update(planned.skipped)
        promoted = {stream for link in planned.links for stream in link.streams}
        effective = {stream: current[stream] for stream in promoted}
        blocks, disturbed = _context_blocks(planned, promoted)
        if disturbed and not blocks:
            return finish("refused", context_disturbed=disturbed)
        guard_blocks, unselected_guard = _guard_blocks(planned, effective)
        if unselected_guard:
            streams.update(guard_blocks)
            attempts[-1]["dropped"] = sorted(guard_blocks)
            return finish("refused", error={"code": "removal_blocked_collateral", "orphans": unselected_guard})
        blocks.update(guard_blocks)
        if not blocks:
            blocks, final = await _judge_dry_run(engine, client, device, live, planned, effective)
            if final is not None:
                return finish(**final)
        current = drop(blocks)
    raise AssertionError("Preflight did not reduce its selection")


def _without_execution_ids(value: Any) -> Any:
    if isinstance(value, list):
        return [_without_execution_ids(item) for item in value]
    if not isinstance(value, dict):
        return value
    return {
        key: (0 if key == "generation_id" else None)
        if key in {"generation_id", "settlement_cohort", "job_id", "seq", "source_push_seq"}
        else _without_execution_ids(item)
        for key, item in value.items()
    }


def _baseline_proofs(fragments: dict[str, dict]) -> dict:
    baseline = _compose_document(fragments)
    if "interface_config" not in baseline:
        return {}
    proof = section_proof(baseline, "interface_config")
    if proof is None:
        raise ValueError("Authorized interface baseline requires an execution proof")
    records = proof["interfaces"]
    return {
        stream: {
            str(row["interface_id"]): records[str(row["interface_id"])]
            for rows in fragment_tables(fragments[stream]).values()
            for row in rows
        }
        for stream in ("interface_config", "ip")
        if stream in fragments
    }


async def _capture_link(
    db: AsyncSession, device, generation, order: int, snapshot: CertifiedSection, authorized: dict[str, dict]
) -> PlannedLink:
    policy = execution_policy(generation)
    context = policy.context
    body = await build_device_containers(
        None,
        device,
        generation.document,
        retain_static_routes=policy.retain_static_routes,
        origin_context=await generation_execution_context(db, generation.job_id)
        if generation.job_id is not None
        else OriginContext.from_generation(generation),
        static_route_snapshot=snapshot,
    )
    await bind_historical_origins(db, device.id, body.origins, generation)
    route_keys: set | frozenset | None = None
    scope = context.get("scope")
    if scope == "static_route" and not context.get("force"):
        route_keys = hydrate_static_route_removal_plan(generation.document).authorized
    elif scope is None and "static_route" in generation.document:
        route_keys = hydrate_static_route_apply_plan(generation.document).allowed
    allowed = guard_allowed(generation, scope=scope, context=context, route_keys=route_keys)
    orphans = evaluate_collateral_guard(
        snapshot.instance, body.containers, allowed=allowed, context=context, no_networking=policy.no_networking
    )
    return PlannedLink(
        _canonical(
            {
                "order": order,
                "mode": generation.mode.value,
                "streams": sorted(generation.stream_revisions),
                "digest": generation.digest,
                "document": generation.document,
                "containers": body.containers,
                "policy": policy._asdict(),
                "origins": _without_execution_ids(origin_records(body.origins)),
                "allowed_removal_keys": allowed,
                "guard_orphans": orphans,
                "guard_contributions": _guard_contributions(authorized, body.containers, orphans),
                "errors": {
                    section: {"code": error.code, "message": _apply_error_summary(error), "detail": error.detail}
                    for section, error in body.errors.items()
                },
            }
        )
    )


async def plan_action_apply(
    engine: AsyncEngine,
    device_id: int,
    selected: dict[str, int],
    snapshot: CertifiedSection,
    *,
    expected_context: tuple[str, str, str | None],
    baseline_only: bool = False,
    ingest: CandidateIngest | None = None,
) -> PlannedChain:
    """Plan the real Apply chain without persistence, even if a planner commits its session.

    The caller reads live inputs before this call. This function owns the connection and
    outer transaction. Session commits only release savepoints. The outer transaction
    always rolls back before the detached plan is returned or an exception escapes.
    """
    if snapshot.inconclusive:
        raise ValueError("Planning requires a certified service instance")
    async with engine.connect() as connection:
        outer = await connection.begin()
        token = LAST_ENQUEUED_GENERATION_ID.set(None)
        try:
            async with AsyncSession(
                bind=connection, join_transaction_mode="create_savepoint", expire_on_commit=False
            ) as db:
                await lock_projection(db, device_id)
                device = await db.get(Device, device_id)
                if device is None or _device_context(device) != expected_context:
                    raise PreflightStale("inputs_changed")
                candidate = None
                if ingest is not None:
                    selected, candidate = await _ingest_candidate_plan(db, device, ingest, snapshot)
                return await _plan_in_session(db, device_id, selected, snapshot, baseline_only, candidate)
        finally:
            LAST_ENQUEUED_GENERATION_ID.reset(token)
            await outer.rollback()


async def _ingest_candidate_plan(db, device, ingest, snapshot):
    selected, bodies = await ingest(db)
    revisions = {
        stream: revision
        for stream, revision in (
            await db.execute(
                select(DeviceProjectionStream.stream, DeviceProjectionStream.authorized_revision).where(
                    DeviceProjectionStream.device_id == device.id
                )
            )
        ).all()
    }
    candidate = _canonical(
        {
            "selection": selected,
            "identity": _digest(
                {
                    "bodies": bodies,
                    "authorized_revisions": {
                        stream: revisions.get(stream, 0) for stream in sorted(projection_streams())
                    },
                    "nso_instance": device.nso_instance,
                    "nso_device_name": device.nso_device_name,
                    "ned_id": device.ned_id,
                    "live_input_digest": _digest(snapshot._asdict()),
                }
            ),
        }
    )
    return selected, candidate


async def _plan_in_session(db, device_id, selected, snapshot, baseline_only, candidate) -> PlannedChain:
    authorized = await _authorized_fragments(db, device_id)
    baseline = _baseline_proofs(authorized)
    skipped: dict[str, str]
    if baseline_only:
        if selected:
            raise ValueError("Baseline planning cannot promote streams")
        generations = [await create_reissue_generation(db, device_id, mode=GenerationMode.networked)]
        skipped, skipped_detail = {}, {}
    else:
        planning_error = None
        try:
            result = await create_action_apply(db, device_id, selected, uuid4())
        except InterfaceProofConflict as conflict:
            promoted, skipped, _ = await _selected_promotions(db, device_id, selected)
            planning_error = _PlanContextConflict(conflict.streams, set(promoted), skipped)
        if planning_error is not None:
            raise planning_error
        generations = result.generations
        skipped, skipped_detail = result.skipped, result.skipped_detail
    device = await db.get(Device, device_id)
    links = tuple(
        [
            await _capture_link(db, device, generation, order, snapshot, authorized)
            for order, generation in enumerate(generations)
        ]
    )
    return PlannedChain(
        links,
        _canonical(
            {
                "skipped": skipped,
                "skipped_detail": _without_execution_ids(skipped_detail),
                "baseline_interface_proofs": baseline,
            }
        ),
        candidate,
    )


def normalized_plan_digest(planned: PlannedChain) -> str:
    """Hash every link's body, policy, frozen execution inputs and removal authority.

    Metadata normalization excludes id, device_id, row_id, generation_id, job_id,
    settlement_cohort, seq, source_push_seq, frozen_row_identity, live_stamp_join,
    baseline_attempt_id, frozen_revision, row_ids, tombstone_ids, pending_clear_ids and
    tombstone_id_watermark, content_identity and carrier_content_identity. The last
    two hashes include surrogate ids; normalized document values bind their content.
    Projection foreign keys use schema-defined logical row identities. Interface proof
    keys and attribute_eligibility keys use interface names. Carrier id lists use their
    lengths. The raw generation digest is excluded because it hashes database identities.
    Wire containers are hashed verbatim, including wire id keys.
    """
    links = []
    for link in planned.links:
        payload = json.loads(link._json)
        payload.pop("digest")
        containers = payload.pop("containers")
        document = _normalize_document(payload.pop("document"))
        links.append({**_normalize_metadata(payload), "document": document, "containers": containers})
    summary = json.loads(planned._json)
    summary["baseline_interface_proofs"] = {
        stream: {record["name"]: _normalize_metadata(record) for record in records.values()}
        for stream, records in summary["baseline_interface_proofs"].items()
    }
    return _digest({"links": links, **_normalize_metadata(summary)})


_DATABASE_KEYS = frozenset(
    {
        "id",
        "device_id",
        "row_id",
        "generation_id",
        "job_id",
        "settlement_cohort",
        "seq",
        "source_push_seq",
        "frozen_row_identity",
        "live_stamp_join",
        "baseline_attempt_id",
        "frozen_revision",
        "row_ids",
        "tombstone_ids",
        "pending_clear_ids",
        "tombstone_id_watermark",
        "content_identity",
        "carrier_content_identity",
    }
)


def _normalize_metadata(value: Any) -> Any:
    if isinstance(value, list):
        return [_normalize_metadata(item) for item in value]
    if not isinstance(value, dict):
        return value
    normalized = {key: _normalize_metadata(item) for key, item in value.items() if key not in _DATABASE_KEYS}
    for key in ("row_ids", "tombstone_ids", "pending_clear_ids"):
        if key in value:
            normalized[key] = len(value[key])
    if isinstance(value.get("interfaces"), dict):
        names = {key: record["name"] for key, record in value["interfaces"].items()}
        normalized["interfaces"] = {
            names[key]: _normalize_metadata(record) for key, record in value["interfaces"].items()
        }
        if "attribute_eligibility" in value:
            normalized["attribute_eligibility"] = {
                f"{names[key.split('/', 1)[0]]}/{key.split('/', 1)[1]}": verdict
                for key, verdict in value["attribute_eligibility"].items()
            }
    return normalized


def _normalize_document(document: dict) -> dict:
    normalized = {}
    for section, fragment in document.items():
        tables = fragment_tables(fragment)
        proof = (fragment.get("_execution") or {}).get("proof") or {}
        interfaces = {int(key): record["name"] for key, record in proof.get("interfaces", {}).items()}
        identities = {
            table: {row["id"]: identity for identity, row in rows_by_intent_identity(tables, table).items()}
            for table in tables
        }
        normalized[section] = {
            **_normalize_metadata({key: value for key, value in fragment.items() if key not in tables}),
            **{
                table: sorted([_normalize_row(table, row, identities, interfaces) for row in rows], key=_canonical)
                for table, rows in tables.items()
            },
        }
    return normalized


def _normalize_row(table: str, row: dict, identities: dict, interfaces: dict) -> dict:
    normalized = _normalize_metadata(row)
    for column in Base.metadata.tables[table].columns:
        for foreign in column.foreign_keys:
            target = foreign.column.table.name
            if column.name in _DATABASE_KEYS or target == Device.__tablename__ or column.name not in row:
                continue
            identity = row[column.name]
            if identity is None:
                continue
            if target == DbInterface.__tablename__:
                normalized[column.name] = interfaces[identity]
            else:
                normalized[column.name] = identities[target][identity]
    return normalized
