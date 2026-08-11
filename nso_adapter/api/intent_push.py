# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The intent-PUT delivery seam every in-protocol endpoint shares (#1522 §G2).

Two pieces, defined once so the sixteen endpoints cannot each spell them differently:

* :func:`get_intent_delivery` — the FastAPI dependency that resolves WHICH section this
  request lands in (from the matched route, via :mod:`core.intent_protocol`) and WHAT
  identifies the delivery (the ``X-Push-Seq``, the raw-body digest and the request mode);
* :func:`admit_or_replay` — the admission call itself, with the two refusals mapped onto
  the wire and the replay turned into a response.

Both run INSIDE the endpoint's mutation transaction, after ``note_write`` has taken the
device's projection lock. That ordering is the guarantee: two concurrent deliveries of one
sequence cannot both read "no receipt", and a refused or replayed delivery leaves nothing
behind because the same transaction is rolled back.
"""

from __future__ import annotations

from fastapi import Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.api.errors import push_conflict_error
from nso_adapter.core.intent_protocol import intent_endpoint
from nso_adapter.core.receipt import IntentDelivery, PushIdentity, PushSequenceConflict, admit_push, digest_body
from nso_adapter.core.request_flags import DELETE_ORIGIN, PUSH_SEQ, STORE_ONLY


async def get_intent_delivery(request: Request) -> IntentDelivery:
    """Return this request's delivery: the stream it lands in and what identifies it.

    Declared as a plain ``Request`` dependency rather than ``Header`` parameters so it adds
    nothing to the OpenAPI schema: the header is a transport-level delivery identity that
    every in-protocol intent PUT shares, documented once in ``docs/api-contract.md``.

    The stream comes from the MATCHED ROUTE, never from a literal at the call site, so an
    endpoint, its receipt and the tables it authorizes cannot drift apart. The digest is
    taken over the RAW body as parsed, not over any model FastAPI built from it, so the
    plugin can compute the same value without knowing the adapter's schemas; Starlette
    caches the body, so re-reading it here costs nothing.
    """
    endpoint = intent_endpoint(request.scope["route"].path)
    seq = PUSH_SEQ.get()
    identity = None
    if seq is not None:
        identity = PushIdentity(
            seq=seq,
            digest=digest_body(await request.json()),
            store_only=STORE_ONLY.get(),
            delete_origin=DELETE_ORIGIN.get(),
        )
    return IntentDelivery(stream=endpoint.stream, identity=identity)


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
    await db.rollback()
    return JSONResponse(status_code=status_code, content=stored)


__all__ = ["admit_or_replay", "get_intent_delivery"]
