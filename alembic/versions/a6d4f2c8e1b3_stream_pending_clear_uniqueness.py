# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Keep one pending-clear row per device stream.

Revision ID: a6d4f2c8e1b3
Revises: e3a7c9d1b504
Create Date: 2026-08-26
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "a6d4f2c8e1b3"
down_revision: str | None = "e3a7c9d1b504"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE stream_pending_clear AS authorized
        SET revision = GREATEST(authorized.revision, store_only.revision),
            recorded_at = LEAST(authorized.recorded_at, store_only.recorded_at)
        FROM stream_pending_clear AS store_only
        WHERE authorized.device_id = store_only.device_id
          AND authorized.stream = store_only.stream
          AND authorized.provenance = 'authorized'
          AND store_only.provenance = 'store_only'
        """
    )
    op.execute(
        """
        DELETE FROM stream_pending_clear AS store_only
        WHERE store_only.provenance = 'store_only'
          AND EXISTS (
              SELECT 1
              FROM stream_pending_clear AS authorized
              WHERE authorized.device_id = store_only.device_id
                AND authorized.stream = store_only.stream
                AND authorized.provenance = 'authorized'
          )
        """
    )
    op.drop_constraint("uq_stream_pending_clear", "stream_pending_clear", type_="unique")
    op.create_unique_constraint(
        "uq_stream_pending_clear",
        "stream_pending_clear",
        ["device_id", "stream"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_stream_pending_clear", "stream_pending_clear", type_="unique")
    op.create_unique_constraint(
        "uq_stream_pending_clear",
        "stream_pending_clear",
        ["device_id", "stream", "provenance"],
    )
