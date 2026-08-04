# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Apply Alembic migrations to head.

Invoked by the container entrypoint (``scripts/docker-entrypoint.sh``) before the
app starts, and usable in CI. Reads ``DATABASE_URL`` from the environment exactly
like ``alembic/env.py`` does, so no extra plumbing is required in the container
(where ``DATABASE_URL`` is already set).

This is the ONLY schema source: nothing else materializes tables, in production or in
tests (the test substrate clones a template built by this same module). ``create_all``
survives with exactly one consumer — ``tests/store/test_schema_parity.py`` — whose job is
proving ``Base.metadata`` still agrees with the migration head.

The Alembic CLI console-script is not reliably on PATH in the image, so we drive
Alembic programmatically.
"""

from __future__ import annotations

import os
from pathlib import Path

from alembic.config import Config

from alembic import command
from nso_adapter.store.db import require_postgresql_url

# repo root == parent of the nso_adapter package; alembic.ini + alembic/ live there
# in both the dev bind-mount (/app) and the prod image (see Dockerfile COPYs).
_ROOT = Path(__file__).resolve().parent.parent


def make_config() -> Config:
    cfg = Config(str(_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_ROOT / "alembic"))
    return cfg


def upgrade_head() -> None:
    # BEFORE alembic: the entrypoint runs this ahead of the app, so this is the first
    # thing that would touch the database. Left unchecked, a wrong DATABASE_URL executes
    # real DDL for several revisions before the chain dies on a dialect difference.
    require_postgresql_url(os.environ.get("DATABASE_URL", ""), label="DATABASE_URL")
    command.upgrade(make_config(), "head")


def main() -> None:
    upgrade_head()


if __name__ == "__main__":
    main()
