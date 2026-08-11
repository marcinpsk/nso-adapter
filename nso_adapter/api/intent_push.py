# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The intent-PUT delivery seam every in-protocol endpoint shares (#1522 §G2).

Two pieces, defined once so the sixteen endpoints cannot each spell them differently:

* :func:`get_intent_delivery` — the FastAPI dependency that resolves WHICH section this
  request lands in (from the matched route, via :mod:`core.intent_protocol`) and WHAT
  identifies the delivery (the ``X-Push-Seq``, the raw-body digest and the request mode);
* :func:`begin_delivery` — the ordered projection write and admission call, with the two
  refusals mapped onto the wire and the replay turned into a response.

Both run inside the endpoint's mutation transaction. ``begin_delivery`` takes the device's
projection lock before admission. That ordering is the guarantee: two concurrent deliveries
of one sequence cannot both read "no receipt", and a refused or replayed delivery leaves
nothing behind because the same transaction is rolled back.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Header, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.errors import api_error, push_conflict_error
from nso_adapter.core.intent_protocol import intent_endpoint
from nso_adapter.core.receipt import IntentDelivery, PushIdentity, PushSequenceConflict, admit_push, digest_body
from nso_adapter.core.request_flags import (
    BACKFILL_ONLY,
    DELETE_ORIGIN,
    MAX_PUSH_SEQ,
    MIN_PUSH_SEQ,
    STORE_ONLY,
)

_PUSH_SEQ_HEADER = Header(
    alias="X-Push-Seq",
    ge=MIN_PUSH_SEQ,
    le=MAX_PUSH_SEQ,
    description=(
        "The delivering claim's identity. REQUIRED on every in-protocol intent PUT: an "
        "absent header is a 422, exactly as a value outside 1 … 2^63-1 is, and never a "
        "silent downgrade to an unkeyed write."
    ),
)

#: The one stream that implements ``?backfill_only=true`` (#1503 §4.4). Any other in-protocol
#: delivery carrying the flag is refused: silently running it as an ordinary full-replace is
#: precisely the before-image destruction the mode exists to prevent.
BACKFILL_ONLY_STREAM = "static_route"


async def get_intent_delivery(
    request: Request,
    x_push_seq: Annotated[int, _PUSH_SEQ_HEADER],
) -> IntentDelivery:
    """Return this request's delivery: the stream it lands in and what identifies it.

    ``X-Push-Seq`` is a DECLARED, REQUIRED parameter here, so the sixteen in-protocol intent
    PUTs carry it in the regenerated OpenAPI snapshot and the two out-of-protocol PUTs —
    which do not inject this dependency — carry no such parameter. OpenAPI truthfulness
    applies to headers too, and this is also the only place the header is parsed: presence
    and domain bounds are declared on the parameter, so an absent or out-of-domain value is
    refused by the same validator that renders the schema rather than by a second copy of the
    rule in a middleware.

    Requiredness is what makes the mutation resolvable (#1503 §5 O3.2). A header-less
    delivery used to commit without a receipt, so a lost response turned the plugin's retry
    into a SECOND operation rather than a replay; on the backfill stream it also lost the
    first response's ``removed_uncorrelated`` attribution. The refusal happens during
    dependency solving, before the endpoint runs: no mutation, no receipt.

    The stream comes from the MATCHED ROUTE, never from a literal at the call site, so an
    endpoint, its receipt and the tables it authorizes cannot drift apart. The digest is
    taken over the RAW body as parsed, not over any model FastAPI built from it, so the
    plugin can compute the same value without knowing the adapter's schemas; Starlette
    caches the body, so re-reading it here costs nothing.
    """
    endpoint = intent_endpoint(request.scope["route"].path)
    if BACKFILL_ONLY.get() and endpoint.stream != BACKFILL_ONLY_STREAM:
        raise api_error(
            422,
            "validation_error",
            f"?backfill_only is implemented for the {BACKFILL_ONLY_STREAM!r} stream only",
            {"reason": "backfill_only_unsupported", "section": endpoint.stream},
        )
    try:
        body = await request.json()
    except ValueError:
        raise api_error(422, "validation_error", "Request body must contain valid JSON") from None
    return IntentDelivery(
        stream=endpoint.stream,
        identity=PushIdentity(
            seq=x_push_seq,
            digest=digest_body(body),
            store_only=STORE_ONLY.get(),
            delete_origin=DELETE_ORIGIN.get(),
            backfill_only=BACKFILL_ONLY.get(),
        ),
    )


async def admit_or_replay(db: AsyncSession, device_id: int, delivery: IntentDelivery) -> JSONResponse | None:
    """Admit this delivery, or hand back the response the first one returned.

    ``None`` means "do the work". A refusal raises the wire error; a replay returns the
    stored response. Both roll the transaction back first — a refused or replayed delivery
    must leave no trace, and the caller's ``note_write`` revision bump is already in it.
    """
    try:
        admitted = await admit_push(db, device_id, delivery)
    except PushSequenceConflict as conflict:
        await db.rollback()
        raise push_conflict_error(conflict.code, conflict.message, conflict.detail) from None
    if admitted is None:
        return None
    stored, status_code = admitted
    if stored is None:
        return None
    await db.rollback()
    return JSONResponse(status_code=status_code, content=stored)


async def begin_delivery(db: AsyncSession, device_id: int, delivery: IntentDelivery) -> JSONResponse | None:
    """Record and admit one intent delivery under the device projection lock."""
    from nso_adapter.core.generation import note_write

    push_seq = delivery.identity.seq if delivery.identity is not None else None
    await note_write(db, device_id, delivery.stream, push_seq=push_seq)
    return await admit_or_replay(db, device_id, delivery)


__all__ = ["begin_delivery", "get_intent_delivery"]
