# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""PostgreSQL-gated parity test: the alembic baseline must match create_all.

The unit suite runs on sqlite via create_all (fast, hermetic). Production runs on
PostgreSQL via `alembic upgrade head`. This test proves the two produce an
identical schema on PostgreSQL, so the create_all-based unit tests transitively
trust the deployed (migrated) schema.

It is skipped unless ``ALEMBIC_PARITY_DB_URL`` is set to a PostgreSQL URL whose
role may CREATE/DROP DATABASE (e.g. the CI ``postgres`` service:
``postgresql+psycopg2://postgres:postgres@localhost:5432/postgres``). The test
creates two throwaway databases, builds one with create_all and one with
``alembic upgrade head``, diffs the reflected schema, and drops both.
"""
from __future__ import annotations

import os
import uuid

import pytest
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.engine import make_url

_PARITY_URL = os.environ.get("ALEMBIC_PARITY_DB_URL")

pytestmark = pytest.mark.skipif(
    not _PARITY_URL,
    reason="ALEMBIC_PARITY_DB_URL not set — PostgreSQL parity lane (CI only)",
)


def _snapshot(engine) -> dict:
    insp = inspect(engine)
    snap: dict = {}
    for table in sorted(insp.get_table_names()):
        if table == "alembic_version":
            continue
        snap[table] = {
            "cols": {c["name"]: (str(c["type"]), c["nullable"]) for c in insp.get_columns(table)},
            "pk": tuple(insp.get_pk_constraint(table)["constrained_columns"]),
            "fks": sorted(
                (tuple(f["constrained_columns"]), f["referred_table"], tuple(f["referred_columns"]))
                for f in insp.get_foreign_keys(table)
            ),
            "uqs": sorted(tuple(u["column_names"]) for u in insp.get_unique_constraints(table)),
            "ixs": sorted((tuple(i["column_names"]), i["unique"]) for i in insp.get_indexes(table)),
        }
    return snap


def _db_url(base: str, dbname: str) -> str:
    # NB: str(URL) masks the password as '***'; render with hide_password=False
    # so the rendered DSN (used for create_engine and DATABASE_URL) actually auths.
    return make_url(base).set(database=dbname).render_as_string(hide_password=False)


def test_alembic_baseline_matches_create_all():
    base = make_url(_PARITY_URL)
    suffix = uuid.uuid4().hex[:10]
    ca_db = f"parity_ca_{suffix}"
    al_db = f"parity_al_{suffix}"

    admin = sa.create_engine(_PARITY_URL, isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as conn:
            conn.exec_driver_sql(f'CREATE DATABASE "{ca_db}"')
            conn.exec_driver_sql(f'CREATE DATABASE "{al_db}"')

        ca_url = _db_url(_PARITY_URL, ca_db)
        al_url = _db_url(_PARITY_URL, al_db)

        # 1) create_all
        from nso_adapter.store.models import Base

        ca_engine = sa.create_engine(ca_url)
        Base.metadata.create_all(ca_engine)

        # 2) alembic upgrade head — env.py reads DATABASE_URL
        from alembic import command

        from nso_adapter.db_migrate import make_config

        prev = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = al_url
        try:
            command.upgrade(make_config(), "head")
        finally:
            if prev is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = prev

        al_engine = sa.create_engine(al_url)

        ca_snap = _snapshot(ca_engine)
        al_snap = _snapshot(al_engine)
        ca_engine.dispose()
        al_engine.dispose()

        assert set(ca_snap) == set(al_snap), (
            f"table set differs: only_create_all={set(ca_snap) - set(al_snap)} "
            f"only_alembic={set(al_snap) - set(ca_snap)}"
        )
        for table in sorted(ca_snap):
            assert ca_snap[table] == al_snap[table], (
                f"schema mismatch in {table!r}:\n"
                f"  create_all={ca_snap[table]}\n  alembic   ={al_snap[table]}"
            )
    finally:
        with admin.connect() as conn:
            for dbname in (ca_db, al_db):
                conn.exec_driver_sql(
                    f'SELECT pg_terminate_backend(pid) FROM pg_stat_activity '
                    f"WHERE datname = '{dbname}'"
                )
                conn.exec_driver_sql(f'DROP DATABASE IF EXISTS "{dbname}"')
        admin.dispose()
