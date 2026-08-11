# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Receipt admission for keyed intent pushes (#1522 §G2, §G5's replay case).

The plugin's outbox delivers each claim with an ``X-Push-Seq`` header. Delivery is at-least
once: a request whose response was lost is re-sent, and without a durable record of what
was already admitted the adapter promotes it a second time — a second generation, a second
device write, for one authorization.

One receipt per ``(device, stream)``, replaced as the sequence advances, decides:

* same seq, same request digest, same MODE → **replay**: the stored response comes back
  verbatim and nothing is applied again;
* same seq and either a DIFFERENT digest or a different mode → **refused**
  (``sequence_reuse``). Two different operations under one claim identity mean the plugin's
  own numbering broke; guessing which one is current would deploy the wrong document;
* a LOWER seq than the receipt → **refused** (``stale``). An out-of-order redelivery of a
  superseded push;
* a higher seq → admitted, and this row becomes that push's receipt.

**Why the mode is part of the identity.** The digest covers the wire BODY, and the body
alone does not say what the request does with it: ``?store_only=true`` mutates the store and
authorizes no device write, ``?delete_origin=true`` turns a shrink into a networked
retraction, the unmarked form detaches instead (#106), and ``?backfill_only=true`` adopts ids
and prunes uncorrelated rows while writing no content at all (#1503 §4.4). The same sequence
carrying the same bytes under two of those is two different operations, so the flags are
stored as receipt COLUMNS and compared at admission. The digest ALGORITHM is untouched — it is
pinned by the plugin, which computes it over the raw body it sent.

The receipt is written in the SAME transaction as the mutation it admits. A receipt that
outlived a rolled-back operation would turn the plugin's retry into a silent no-op, which is
the one outcome worse than a double apply.

Every in-protocol delivery is keyed: the header is REQUIRED on all sixteen PUTs, so a missing,
malformed or out-of-domain value is a 422 at the API boundary and nothing here ever sees an
unkeyed one (:mod:`api.intent_push`). The ratified #1503 contract keeps lacp and switchport
out of the protocol as claim-less direct-apply deliveries — they are POSTs, they never reach
admission, and they need no representation in these types.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nso_adapter.core.intent_protocol import INTENT_STREAMS
from nso_adapter.store.models import IntentPushReceipt

logger = structlog.get_logger(__name__)


class PushSequenceConflict(Exception):
    """A keyed push cannot be admitted. Carries the wire error code the plugin branches on.

    ``sequence_reuse`` — the same sequence already landed with a different body or in a
    different request mode.
    ``stale`` — a sequence older than the one already admitted for this stream.
    """

    def __init__(self, code: str, message: str, detail: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail = detail or {}


@dataclass(frozen=True)
class PushIdentity:
    """One delivery's identity: the claim sequence, the body digest, and the request MODE.

    The two mode flags are the canonical form of what the middleware parsed off the query
    string (``core.request_flags.parse_store_only``), so "absent", ``0`` and ``no`` are one
    identity and cannot be replayed as ``true``.
    """

    seq: int
    digest: str
    store_only: bool
    delete_origin: bool
    backfill_only: bool


@dataclass(frozen=True)
class IntentDelivery:
    """One intent PUT as the protocol sees it (see :mod:`core.intent_protocol`).

    *stream* is the receipt key AND the promotion unit — the endpoint's lane and the intent
    tables it owns. *identity* is not optional: an in-protocol delivery without a claim is
    refused at the boundary, so there is no unkeyed shape to represent.

    The document family the lane composes into is not carried: nothing a request does needs
    it. It is declared on :class:`core.intent_protocol.IntentEndpoint`, where
    :func:`core.projection.projection_streams` pins it against the stream's owned tables.
    """

    stream: str
    identity: PushIdentity

    @property
    def push_seq(self) -> int:
        """The claim sequence this delivery carries."""
        return self.identity.seq


def digest_body(body: object) -> str:
    """Return the sha256 the plugin and the adapter must agree on for one wire body.

    Canonical JSON over the body AS PARSED — not over any internal shape derived from it, so
    the digest cannot drift when the store's representation changes. Fixed by the plugin's
    landed implementation: the algorithm never changes, only what is compared alongside it.
    """
    return hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()


def _now() -> datetime:
    return datetime.now(UTC)


async def latest_receipt(db: AsyncSession, device_id: int, stream: str) -> IntentPushReceipt | None:
    """Return this device+stream's last admitted receipt, or None.

    The column is spelled ``section``: the receipt table's shape is pinned, and it has always
    held this same per-endpoint vocabulary.
    """
    return await db.scalar(
        select(IntentPushReceipt).where(
            IntentPushReceipt.device_id == device_id,
            IntentPushReceipt.section == stream,
        )
    )


def _mode(carrier) -> tuple[bool, bool, bool]:
    """Return the request mode as one comparable triple, receipt row and identity alike."""
    return bool(carrier.store_only), bool(carrier.delete_origin), bool(carrier.backfill_only)


async def admit_push(db: AsyncSession, device_id: int, delivery: IntentDelivery) -> tuple[dict | None, int] | None:
    """Admit one keyed push. Returns the stored ``(response, status)`` on a replay.

    ``None`` means "do the work": this sequence is new. The caller must already hold the
    device's projection lock, so two concurrent deliveries of the same sequence cannot both
    read "no receipt" and both proceed.

    Raises :class:`PushSequenceConflict` for a reused sequence or a stale one.
    """
    stream = delivery.stream
    if stream not in INTENT_STREAMS:
        raise RuntimeError(f"{stream!r} is not an in-protocol intent stream")
    identity = delivery.identity
    receipt = await latest_receipt(db, device_id, stream)
    if receipt is None:
        db.add(
            IntentPushReceipt(
                device_id=device_id,
                section=stream,
                push_seq=identity.seq,
                request_digest=identity.digest,
                store_only=identity.store_only,
                delete_origin=identity.delete_origin,
                backfill_only=identity.backfill_only,
            )
        )
        await db.flush()
        return None
    if receipt.push_seq == identity.seq:
        if receipt.request_digest == identity.digest and _mode(receipt) == _mode(identity):
            logger.info("push.replayed", device_id=device_id, stream=stream, push_seq=identity.seq)
            return receipt.response, receipt.status_code
        raise PushSequenceConflict(
            "sequence_reuse",
            f"push sequence {identity.seq} was already admitted for {stream!r} with a different body or mode",
            # The detail key stays ``section``: it is the wire name the plugin branches on.
            {
                "section": stream,
                "push_seq": identity.seq,
                "admitted_digest": receipt.request_digest,
                "admitted_store_only": receipt.store_only,
                "admitted_delete_origin": receipt.delete_origin,
                "admitted_backfill_only": receipt.backfill_only,
            },
        )
    if identity.seq < receipt.push_seq:
        raise PushSequenceConflict(
            "stale",
            f"push sequence {identity.seq} is older than the admitted {receipt.push_seq} for {stream!r}",
            {"section": stream, "push_seq": identity.seq, "admitted_push_seq": receipt.push_seq},
        )
    receipt.push_seq = identity.seq
    receipt.request_digest = identity.digest
    receipt.store_only = identity.store_only
    receipt.delete_origin = identity.delete_origin
    receipt.backfill_only = identity.backfill_only
    receipt.response = None
    receipt.status_code = 200
    receipt.generation_id = None
    receipt.updated_at = _now()
    await db.flush()
    return None


async def record_response(
    db: AsyncSession,
    device_id: int,
    delivery: IntentDelivery,
    response: dict,
    *,
    generation_id: int | None = None,
    status_code: int = 200,
) -> None:
    """Store what this push returned, so a redelivery replays it. Caller commits.

    Also stamps the generation this push authorized: the LAST one it enqueued (the apply
    when auto-apply fired, else the final removal), or null when it enqueued none. Taken
    from the request-scoped var :mod:`core.generation` writes, so no intent PUT has to thread
    it back. An explicit *generation_id* wins over that var.
    """
    from nso_adapter.core.generation import consume_last_enqueued_generation_id

    identity = delivery.identity
    enqueued_generation_id = consume_last_enqueued_generation_id()
    receipt = await latest_receipt(db, device_id, delivery.stream)
    if receipt is None or receipt.push_seq != identity.seq:  # pragma: no cover — admit_push just wrote it
        raise RuntimeError(f"no admitted receipt for device {device_id} stream {delivery.stream!r} seq {identity.seq}")
    receipt.response = response
    receipt.status_code = status_code
    receipt.generation_id = generation_id if generation_id is not None else enqueued_generation_id
    receipt.updated_at = _now()
    await db.flush()


__all__ = [
    "IntentDelivery",
    "PushIdentity",
    "PushSequenceConflict",
    "admit_push",
    "digest_body",
    "latest_receipt",
    "record_response",
]
