# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Candidate ingestion bound to the same handlers and models as live deliveries."""

from collections.abc import Callable
from dataclasses import dataclass, replace
from inspect import signature
from typing import Any

from fastapi.routing import iter_route_contexts
from pydantic import BaseModel, TypeAdapter, ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.errors import api_error
from nso_adapter.api.timestamps import UtcInstant
from nso_adapter.core.intent_protocol import INTENT_PUT_ENDPOINTS, OUT_OF_PROTOCOL_APPLY_POSTS
from nso_adapter.core.receipt import IntentDelivery, PushIdentity, digest_body, latest_receipt
from nso_adapter.core.request_flags import BACKFILL_ONLY, DELETE_ORIGIN, MAX_PUSH_SEQ, STORE_ONLY
from nso_adapter.store.models import DeploymentGeneration, GenerationMode, Job


def candidate_ingester(ingest: Callable):
    """Bind a route to shared ingestion that needs no independent device claim."""

    def decorate(endpoint):
        endpoint.candidate_ingester = ingest
        return endpoint

    return decorate


@dataclass(frozen=True)
class CandidateHandler:
    endpoint: Callable
    body_name: str
    adapter: TypeAdapter
    receipt: bool


def candidate_registry(routes) -> dict[str, CandidateHandler]:
    """Refuse incomplete wiring instead of omitting a registered ingestion route."""
    paths = {path: (entry.stream, True, "PUT") for path, entry in INTENT_PUT_ENDPOINTS.items()}
    paths.update({path: (stream, False, "POST") for path, stream in OUT_OF_PROTOCOL_APPLY_POSTS.items()})
    registry = {}
    for route in iter_route_contexts(routes):
        if route.path not in paths:
            continue
        stream, receipt, method = paths[route.path]
        if method not in (route.methods or set()):
            continue
        params = route.dependant.body_params
        if len(params) != 1:
            raise RuntimeError("Candidate ingestion requires one route body")
        body = params[0]
        expected = {"device_id", "db", body.name} | ({"delivery"} if receipt else set())
        endpoint = getattr(route.endpoint, "candidate_ingester", route.endpoint)
        if not callable(endpoint):
            raise RuntimeError("Candidate ingestion requires a route handler")
        if set(signature(endpoint).parameters) != expected or stream in registry:
            raise RuntimeError("Candidate handler wiring is ambiguous")
        registry[stream] = CandidateHandler(endpoint, body.name, TypeAdapter(body.field_info.annotation), receipt)
    if registry.keys() != {entry[0] for entry in paths.values()}:
        raise RuntimeError("Candidate registry is missing an ingestion route")
    return registry


def _require_timestamps(value: Any) -> None:
    if isinstance(value, BaseModel):
        for name in type(value).model_fields:
            item = getattr(value, name)
            if name == "accepted_at" and item is None:
                raise ValueError("Candidate timestamps must be explicit")
            _require_timestamps(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _require_timestamps(item)


@dataclass(frozen=True)
class CandidateIngestion:
    device_id: int
    handlers: dict[str, CandidateHandler]
    bodies: dict[str, Any]
    normalized: dict[str, dict]
    wire_digests: dict[str, str]

    async def __call__(self, db: AsyncSession) -> tuple[dict[str, int], dict[str, dict]]:
        return await self._ingest(db, store_only=True)

    async def delivery_effect(self, db: AsyncSession) -> dict:
        """Observe every generation and job created by each normal delivery."""
        effects = []
        for stream, body in self.bodies.items():
            # Watermarks, not id lists: retained history must not grow the query (asyncpg caps bind parameters).
            generation_floor = await self._max_id(db, DeploymentGeneration)
            job_floor = await self._max_id(db, Job)
            candidate = replace(self, bodies={stream: body}, normalized={stream: self.normalized[stream]})
            await candidate._ingest(db, store_only=False)
            await db.flush()
            created = select(DeploymentGeneration.mode, DeploymentGeneration.removal_context).where(
                DeploymentGeneration.device_id == self.device_id, DeploymentGeneration.id > generation_floor
            )
            for generation in (await db.execute(created)).all():
                if generation.mode == GenerationMode.detach:
                    kind = "detach"
                elif generation.removal_context is not None:
                    kind = "removal"
                else:
                    kind = "auto_apply" if generation.mode == GenerationMode.networked else generation.mode.value
                effects.append({"stream": stream, "kind": kind})
            new_jobs = select(Job.id).where(Job.device_id == self.device_id, Job.id > job_floor)
            effects.extend({"stream": stream, "kind": "job"} for _job_id in (await db.scalars(new_jobs)).all())
        return (
            {"promotes": True, "effects": sorted(effects, key=lambda effect: (effect["stream"], effect["kind"]))}
            if effects
            else {"promotes": False}
        )

    async def _max_id(self, db: AsyncSession, model) -> int:
        return await db.scalar(select(func.coalesce(func.max(model.id), 0)).where(model.device_id == self.device_id))

    async def _ingest(self, db: AsyncSession, *, store_only: bool) -> tuple[dict[str, int], dict[str, dict]]:
        selected = {}
        deletion = DELETE_ORIGIN.set(False)
        backfill = BACKFILL_ONLY.set(False)
        try:
            for stream, body in self.bodies.items():
                handler = self.handlers[stream]
                kwargs = {handler.body_name: body, "device_id": self.device_id, "db": db}
                if handler.receipt:
                    receipt = await latest_receipt(db, self.device_id, stream)
                    seq = receipt.push_seq + 1 if receipt is not None else 1
                    if seq > MAX_PUSH_SEQ:
                        raise api_error(422, "validation_error", "Candidate push sequence is exhausted")
                    kwargs["delivery"] = IntentDelivery(
                        stream, PushIdentity(seq, self.wire_digests[stream], store_only, False, False)
                    )
                token = STORE_ONLY.set(store_only and handler.receipt)
                try:
                    response = await handler.endpoint(**kwargs)
                finally:
                    STORE_ONLY.reset(token)
                selected[stream] = seq if handler.receipt else response["selection_revision"]
        finally:
            BACKFILL_ONLY.reset(backfill)
            DELETE_ORIGIN.reset(deletion)
        return selected, self.normalized


def validate_candidates(
    registry: dict[str, CandidateHandler], device_id: int, candidates: dict[str, dict]
) -> CandidateIngestion:
    if not candidates or candidates.keys() - registry.keys():
        raise api_error(422, "validation_error", "candidates must name known ingestion streams")
    bodies, normalized = {}, {}
    invalid = False
    try:
        for stream, raw in sorted(candidates.items()):
            handler = registry[stream]
            body = handler.adapter.validate_python(raw)
            _require_timestamps(body)
            if stream == "route_policy":
                objects = body["objects"]
                if not isinstance(objects, list) or not all(isinstance(obj, dict) for obj in objects):
                    raise ValueError("route_policy objects must be a list of JSON objects")
                body = {**body, "objects": [dict(obj) for obj in objects]}
                for obj in body["objects"]:
                    obj["accepted_at"] = TypeAdapter(UtcInstant).validate_python(obj["accepted_at"])
            bodies[stream] = body
            normalized[stream] = handler.adapter.dump_python(body, mode="json")
    except (ValidationError, ValueError, KeyError, TypeError):
        invalid = True
    if invalid:
        raise api_error(422, "validation_error", "Candidate body is invalid or lacks an explicit accepted_at")
    return CandidateIngestion(
        device_id, registry, bodies, normalized, {stream: digest_body(raw) for stream, raw in candidates.items()}
    )
