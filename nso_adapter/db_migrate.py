# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Apply Alembic migrations to head.

Invoked by the container entrypoint (``scripts/docker-entrypoint.sh``) before the
app starts, and usable in CI. Reads ``DATABASE_URL`` from the environment exactly
like ``alembic/env.py`` does, so no extra plumbing is required in the container
(where ``DATABASE_URL`` is already set).

This is the real-DB (PostgreSQL) schema source. ``create_all`` (in ``main.py``)
stays as the test bootstrap (sqlite) and is an idempotent no-op in production once
migrations have run. Parity between the two is asserted by
``tests/store/test_schema_parity.py``.

The Alembic CLI console-script is not reliably on PATH in the image, so we drive
Alembic programmatically.
"""
from __future__ import annotations

from pathlib import Path

from alembic.config import Config

from alembic import command

# repo root == parent of the nso_adapter package; alembic.ini + alembic/ live there
# in both the dev bind-mount (/app) and the prod image (see Dockerfile COPYs).
_ROOT = Path(__file__).resolve().parent.parent


def make_config() -> Config:
    cfg = Config(str(_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_ROOT / "alembic"))
    return cfg


def upgrade_head() -> None:
    command.upgrade(make_config(), "head")


def main() -> None:
    upgrade_head()


if __name__ == "__main__":
    main()
