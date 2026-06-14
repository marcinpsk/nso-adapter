"""community-list invert_match (read-mirror + intent)

Revision ID: f8a9b0c1d2e3
Revises: e7f8a9b0c1d2
Create Date: 2026-06-14

A community-list can carry an invert-match flag (Junos invert-match / Nokia
"expression NOT (…)"): the list matches routes carrying NONE of its members. Read
into the device read-mirror and round-tripped through the intent on apply. Both
the read-mirror (device_route_policy_community_list) and the pushed intent
(route_policy_object_intent) gain the column. server_default false so existing
rows keep the non-inverted default.
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "f8a9b0c1d2e3"
down_revision = "e7f8a9b0c1d2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "device_route_policy_community_list",
        sa.Column("invert_match", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.add_column(
        "route_policy_object_intent",
        sa.Column("invert_match", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )


def downgrade() -> None:
    op.drop_column("route_policy_object_intent", "invert_match")
    op.drop_column("device_route_policy_community_list", "invert_match")
