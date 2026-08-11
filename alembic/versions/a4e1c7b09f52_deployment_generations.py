# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Deployment generations, the projection promotion state and push receipts (#1558, #1522 §G1/§G2).

Four tables.

``device_generation_counter`` — one row per device: ``device_id`` (PK, FK devices CASCADE)
and ``last_seq``. It allocates ``seq`` under a row lock held to COMMIT, which is what makes
the sequence equal the commit order of the mutations that created the generations, and,
taken by every accepted projection write, is also the serialization point that gives
generation creation one consistent snapshot.

``device_projection_stream`` — the promotion triple per (device, stream), where a STREAM is
one intent-PUT endpoint's lane and owns an explicit set of intent tables (sixteen streams
compose fourteen document sections): ``desired_revision`` (any accepted write),
``authorized_revision`` (promoted), ``applied_revision`` (proven on the device), plus
``source_push_seq`` (the plugin's ``X-Push-Seq`` behind the last desired bump) and
``authorized_document`` (the stream's owned-table FRAGMENT as of its last promotion, which
every later generation composes in so its document is the COMPLETE outbound device
document).

``deployment_generation`` — one immutable device document with its ``mode``, ``digest``,
``allowed_removal_keys``, ``source_push_seq``, ``stream_revisions`` and, for a removal,
``removal_context``; ordered per device by ``seq``. The immutable columns are enforced by
the ``deployment_generation_immutable`` trigger below, not by convention: a rewritten
document silently deploys something nobody authorized.

``intent_push_receipt`` — the replay boundary, ONE row per (device, section), replaced as
the sequence advances. Its ``section`` column holds that same per-endpoint stream
vocabulary; the column name is the receipt table's pinned wire contract. Columns:

* ``id``              — surrogate PK.
* ``device_id``       — FK ``devices.id`` ON DELETE CASCADE, indexed.
* ``section``         — the intent-endpoint stream name, ≤32 chars.
* ``push_seq``        — the admitted ``X-Push-Seq``. BIGINT, domain 1 … 2^63-1 (the API
  boundary rejects 0, negatives and anything wider with 422, so this column never
  overflows).
* ``request_digest``  — sha256 hex of the canonical JSON of the RAW wire body.
* ``store_only`` / ``delete_origin`` — the delivery's request MODE, canonicalised to
  booleans. Compared at admission alongside the digest: the body does not say what the
  request does with it, so one sequence carrying one body under two modes is two different
  deployments.
* ``response``        — the response body that push returned, replayed verbatim.
* ``status_code``     — the status that response was returned with.
* ``generation_id``   — FK ``deployment_generation.id`` ON DELETE SET NULL; the generation
  this push authorized, NULL when it authorized none (store-only).
* ``created_at`` / ``updated_at`` — timestamptz.
* UNIQUE ``(device_id, section)`` — the keying: latest receipt per device+stream. Per-push
  history is not kept here; ``deployment_generation.source_push_seq`` already holds it.

No backfill: a device with no generations has an empty chain, and the first accepted write
after this migration creates generation 1. Seeding the authorized projection from the live
NSO instances is the cutover's job (§G3 step 5), not this migration's.

**AMENDED IN PLACE (#1558 rework 3, finding 1).** This revision is unpushed and remains the
single head, so the promotion tables were re-shaped here rather than in a follow-up. Against
the first draft of this file:

* ``device_projection_section`` is now ``device_projection_stream``; its ``section`` column
  is ``stream``, ``uq_projection_section`` is ``uq_projection_stream`` and
  ``ix_device_projection_section_device_id`` is ``ix_device_projection_stream_device_id``.
  Same column types; the KEY is now the sixteen endpoint streams, not the fourteen document
  sections, and ``authorized_document`` holds the stream's owned-table fragment rather than
  a whole section.
* ``deployment_generation.section_revisions`` is now ``stream_revisions`` (same JSON type,
  and still one of the trigger's immutable columns — ``store/ddl.py`` lists it by name).
* ``intent_push_receipt`` is UNCHANGED, columns and unique key included.

Revision ID: a4e1c7b09f52
Revises: d3a7f1c58e42
Create Date: 2026-08-11
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op
from nso_adapter.store.ddl import generation_immutability_ddl, generation_immutability_drop_ddl

revision: str = "a4e1c7b09f52"
down_revision: str | Sequence[str] | None = "d3a7f1c58e42"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_MODE = sa.Enum("networked", "detach", name="generationmode")
_STATUS = sa.Enum(
    "pending",
    "running",
    "settled",
    "failed",
    "outcome_unknown",
    "abandoned",
    name="generationstatus",
)


def upgrade() -> None:
    op.create_table(
        "device_generation_counter",
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("last_seq", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("device_id"),
    )

    op.create_table(
        "device_projection_stream",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("stream", sa.String(length=32), nullable=False),
        sa.Column("desired_revision", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("authorized_revision", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("applied_revision", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("source_push_seq", sa.BigInteger(), nullable=True),
        sa.Column("authorized_document", sa.JSON(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "stream", name="uq_projection_stream"),
    )
    op.create_index(
        op.f("ix_device_projection_stream_device_id"), "device_projection_stream", ["device_id"], unique=False
    )

    op.create_table(
        "deployment_generation",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("seq", sa.BigInteger(), nullable=False),
        sa.Column("mode", _MODE, nullable=False),
        sa.Column("status", _STATUS, nullable=False),
        sa.Column("document", sa.JSON(), nullable=False),
        sa.Column("digest", sa.String(length=64), nullable=False),
        sa.Column("allowed_removal_keys", sa.JSON(), nullable=False),
        sa.Column("source_push_seq", sa.JSON(), nullable=False),
        sa.Column("stream_revisions", sa.JSON(), nullable=False),
        sa.Column("removal_context", sa.JSON(), nullable=True),
        sa.Column("job_id", sa.Integer(), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_error", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("settled_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "seq", name="uq_generation_seq_per_device"),
    )
    op.create_index(op.f("ix_deployment_generation_device_id"), "deployment_generation", ["device_id"], unique=False)
    op.create_index("ix_generation_device_status", "deployment_generation", ["device_id", "status"], unique=False)
    op.create_index("ix_generation_job", "deployment_generation", ["job_id"], unique=False)

    op.create_table(
        "intent_push_receipt",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("device_id", sa.Integer(), nullable=False),
        sa.Column("section", sa.String(length=32), nullable=False),
        sa.Column("push_seq", sa.BigInteger(), nullable=False),
        sa.Column("request_digest", sa.String(length=64), nullable=False),
        sa.Column("store_only", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("delete_origin", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("response", sa.JSON(), nullable=True),
        sa.Column("status_code", sa.Integer(), nullable=False, server_default=sa.text("200")),
        sa.Column("generation_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["generation_id"], ["deployment_generation.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "section", name="uq_push_receipt_device_section"),
    )
    op.create_index(op.f("ix_intent_push_receipt_device_id"), "intent_push_receipt", ["device_id"], unique=False)

    # Immutability, enforced by the database (#1522 §G1). A generation's identity is what a
    # retry re-sends and what settlement stamps; nothing in the application may rewrite it.
    # Rendered from store.ddl so create_all installs the SAME trigger (schema parity does
    # not compare triggers, so a second copy here could drift unseen).
    for statement in generation_immutability_ddl():
        op.execute(statement)


def downgrade() -> None:
    for statement in generation_immutability_drop_ddl():
        op.execute(statement)
    op.drop_index(op.f("ix_intent_push_receipt_device_id"), table_name="intent_push_receipt")
    op.drop_table("intent_push_receipt")
    op.drop_index("ix_generation_job", table_name="deployment_generation")
    op.drop_index("ix_generation_device_status", table_name="deployment_generation")
    op.drop_index(op.f("ix_deployment_generation_device_id"), table_name="deployment_generation")
    op.drop_table("deployment_generation")
    op.drop_index(op.f("ix_device_projection_stream_device_id"), table_name="device_projection_stream")
    op.drop_table("device_projection_stream")
    op.drop_table("device_generation_counter")
    bind = op.get_bind()
    _STATUS.drop(bind, checkfirst=True)
    _MODE.drop(bind, checkfirst=True)
