# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Narrow job dedupe to one QUEUED job per (device, job_type).

The old index allowed at most one queued-or-running job per device, which conflated two
different questions. Two consequences, both wrong:

* a queued removal blocked every other job type on that device — and a removal is
  enqueued BEFORE its apply by design, so the apply was dropped outright;
* a running job refused its own successor, even though the successor is precisely what
  carries the newer intent.

Serializing EXECUTION is the device claim's job, not admission's: the claim is the only
gate that can span a whole run, since an apply goes terminal while its claim is still
held through the post-apply refresh.

Removals stay exempt from the uniqueness. `enqueue_removal` queues one job per scope on
the same device and every one must run; their FIFO ordering comes from the worker's
per-device head claim. Stated here so the exemption is not later read as an oversight.

Because this is an INDEX and not a constraint, an upsert must target it by conflict
INFERENCE (index columns plus a matching predicate). Naming it with
`ON CONFLICT ON CONSTRAINT` raises instead of returning empty.

Revision ID: f1a3c9e7b204
Revises: c7e4b8a05d19
Create Date: 2026-07-30
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "f1a3c9e7b204"
down_revision: str | Sequence[str] | None = "c7e4b8a05d19"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_OLD_NAME = "uq_job_active_per_device"
_OLD_WHERE = "status IN ('queued', 'running') AND job_type <> 'removal'"
_NEW_NAME = "uq_job_queued_per_device_type"
_NEW_WHERE = "status = 'queued' AND job_type <> 'removal'"


def upgrade() -> None:
    op.drop_index(_OLD_NAME, table_name="jobs")
    op.create_index(
        _NEW_NAME,
        "jobs",
        ["device_id", "job_type"],
        unique=True,
        postgresql_where=sa.text(_NEW_WHERE),
    )


def downgrade() -> None:
    op.drop_index(_NEW_NAME, table_name="jobs")
    op.create_index(
        _OLD_NAME,
        "jobs",
        ["device_id"],
        unique=True,
        postgresql_where=sa.text(_OLD_WHERE),
    )
