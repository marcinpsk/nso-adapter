# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Detached manual-Apply plans built in an owned, rollback-only transaction."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, NamedTuple
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from nso_adapter.core.apply import _apply_error_summary, build_device_containers
from nso_adapter.core.generation import (
    LAST_ENQUEUED_GENERATION_ID,
    ExecutionPolicy,
    _authorized_fragments,
    _compose_document,
    create_action_apply,
    execution_policy,
    generation_execution_context,
    lock_projection,
)
from nso_adapter.core.projection import fragment_tables, rows_by_intent_identity, section_proof
from nso_adapter.core.removal import evaluate_collateral_guard, guard_allowed
from nso_adapter.core.static_route_plan import hydrate_static_route_apply_plan, hydrate_static_route_removal_plan
from nso_adapter.core.static_route_reader import CertifiedSection, certified_static_route_section
from nso_adapter.nso.origins import OriginIndex
from nso_adapter.store.execution_evidence import origin_records, origins_from_records
from nso_adapter.store.execution_history import bind_historical_origins
from nso_adapter.store.models import Base, DbInterface, Device, GenerationMode


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


class LivePlanInputs(NamedTuple):
    section: CertifiedSection
    digest: str | None


async def read_plan_inputs(client, device) -> LivePlanInputs:
    """Read once outside the planning transaction; an inconclusive read has no digest."""
    section = await certified_static_route_section(client, device)
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
    def errors(self) -> dict:
        return json.loads(self._json)["errors"]


@dataclass(frozen=True)
class PlannedChain:
    links: tuple[PlannedLink, ...]
    _json: str

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


async def _capture_link(db: AsyncSession, device, generation, order: int, snapshot: CertifiedSection) -> PlannedLink:
    policy = execution_policy(generation)
    context = policy.context
    body = await build_device_containers(
        None,
        device,
        generation.document,
        retain_static_routes=policy.retain_static_routes,
        origin_context=await generation_execution_context(db, generation.job_id),
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
                "errors": {
                    section: {"code": error.code, "message": _apply_error_summary(error), "detail": error.detail}
                    for section, error in body.errors.items()
                },
            }
        )
    )


async def plan_action_apply(
    engine: AsyncEngine, device_id: int, selected: dict[str, int], snapshot: CertifiedSection
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
                baseline = _baseline_proofs(await _authorized_fragments(db, device_id))
                result = await create_action_apply(db, device_id, selected, uuid4())
                device = await db.get(Device, device_id)
                links = tuple(
                    [
                        await _capture_link(db, device, generation, order, snapshot)
                        for order, generation in enumerate(result.generations)
                    ]
                )
                return PlannedChain(
                    links,
                    _canonical(
                        {
                            "skipped": result.skipped,
                            "skipped_detail": _without_execution_ids(result.skipped_detail),
                            "baseline_interface_proofs": baseline,
                        }
                    ),
                )
        finally:
            LAST_ENQUEUED_GENERATION_ID.reset(token)
            await outer.rollback()


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
