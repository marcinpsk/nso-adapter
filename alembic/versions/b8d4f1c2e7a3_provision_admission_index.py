# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""One active provision per (nso_instance, device_name), enforced by the database.

Provision rows carry ``device_id IS NULL`` — the adapter Device may not exist yet — so
``uq_job_queued_per_device_type`` cannot reach them (NULLs are distinct) and the dedupe key
lives in ``Job.context`` instead. Until now that dedupe was a check-then-insert with nothing
behind it: two concurrent onboardings of the same node both admitted and both ran.

An EXPRESSION index, so an upsert must target it by conflict inference with matching
expressions plus the predicate. Unlike the queued dedupe this one covers ``running`` too: a
provision has no successor semantics — re-running one mid-flight repeats the NSO node
creation and the sync-from.

Revision ID: b8d4f1c2e7a3
Revises: f1a3c9e7b204
Create Date: 2026-08-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b8d4f1c2e7a3"
down_revision: str | Sequence[str] | None = "f1a3c9e7b204"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_NAME = "uq_job_active_provision_pair"
_WHERE = "status IN ('queued', 'running') AND job_type = 'provision'"

# The duplicates the missing constraint could already have produced. Creating the index over
# them fails outright, and a migration that cannot install leaves the adapter unable to
# start — so the oldest row per pair is kept and the rest are terminalized, which is what
# the admission path would have done had the index existed.
#
# Rows with either context key NULL are left alone: NULLs are distinct to a unique index, so
# they were never in conflict, and partitioning them together would terminalize unrelated
# onboardings.
_RECONCILE_DUPLICATES = f"""
UPDATE jobs SET status = 'failed',
       error = '{{"code": "superseded",
                  "message": "A concurrent provision for the same device won admission",
                  "detail": {{}}}}'::json
 WHERE id IN (
    SELECT id FROM (
        SELECT id, row_number() OVER (
                   PARTITION BY context ->> 'nso_instance', context ->> 'device_name'
                   ORDER BY created_at, id) AS rn
          FROM jobs
         WHERE {_WHERE}
           AND context ->> 'nso_instance' IS NOT NULL
           AND context ->> 'device_name' IS NOT NULL
    ) ranked WHERE ranked.rn > 1
 )
"""


def upgrade() -> None:
    op.execute(sa.text(_RECONCILE_DUPLICATES))
    op.create_index(
        _NAME,
        "jobs",
        [sa.text("(context ->> 'nso_instance')"), sa.text("(context ->> 'device_name')")],
        unique=True,
        postgresql_where=sa.text(_WHERE),
    )


def downgrade() -> None:
    op.drop_index(_NAME, table_name="jobs")
