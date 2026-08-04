# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Guards for the naive-timestamp -> timestamptz normalization migration.

The migration widens the last 25 naive ``timestamp`` columns to ``timestamptz``.
Without an explicit ``USING <col> AT TIME ZONE 'UTC'`` PostgreSQL interprets the
stored wall clock in the *session* TimeZone, so a migration run under any non-UTC
zone silently shifts every historical instant. Every test here therefore runs the
alembic subprocess with ``PGOPTIONS=-c timezone=America/New_York`` and asserts the
session really is on that zone before trusting a result.

Three guards, per the 1329 plan §2.8.2:

* conversion  - seed EVERY affected column at the pre-migration revision with raw
  SQL, upgrade, and assert each value comes back as the same UTC instant. This is
  the only test that fails when a single ``USING`` clause is missing.
* structural  - the migration declares exactly the 25 pairs, and both directions
  carry the explicit UTC ``USING`` clause (asserted on the module source AND on
  the SQL alembic actually renders, per column).
* downgrade   - symmetric: seed at head, downgrade, same instants; upgrade again.
"""

from __future__ import annotations

import ast
import importlib.util
import os
import subprocess
import sys
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import sqlalchemy as sa

from tests.conftest import ADMIN_URL, _drop_database, _url_for

_REPO_ROOT = Path(__file__).resolve().parents[2]

# Referenced by path, not by walking the script directory: before the migration exists this
# import fails, which is the red evidence. down_revision is then read FROM the module so the
# test and the migration cannot drift apart.
_MIGRATION_PATH = _REPO_ROOT / "alembic" / "versions" / "f4c1b7d92a05_naive_timestamps_to_timestamptz.py"

# A non-UTC zone with a DST offset from UTC — under it, a missing USING clause shifts by 4h.
_TZ = "America/New_York"

# The 25 (table, column) pairs of plan §2.8.1, spelled out rather than derived from
# Base.metadata: deriving them from the models would make the test agree with whatever
# the models happen to say.
_COLUMNS: list[tuple[str, str]] = [
    ("device_failover", "last_probe_at"),
    ("device_failover", "last_switch_at"),
    ("device_failover", "next_oob_probe_at"),
    ("device_failover", "next_primary_probe_at"),
    ("device_failover", "oob_health_checked_at"),
    ("device_failover", "updated_at"),
    ("device_route_policy_as_path", "last_refreshed_at"),
    ("device_route_policy_community_list", "last_refreshed_at"),
    ("device_route_policy_prefix_list", "last_refreshed_at"),
    ("device_route_policy_route_map", "last_refreshed_at"),
    ("device_settings", "updated_at"),
    ("devices", "created_at"),
    ("devices", "last_sync_at"),
    ("devices", "updated_at"),
    ("failover_config", "updated_at"),
    ("interface_attr_state", "last_checked_at"),
    ("interface_intent", "accepted_at"),
    ("interface_intent", "last_apply_at"),
    ("jobs", "created_at"),
    ("jobs", "heartbeat_at"),
    ("jobs", "started_at"),
    ("jobs", "updated_at"),
    ("managed_scope", "updated_at"),
    ("route_policy_object_intent", "accepted_at"),
    ("route_policy_object_intent", "last_apply_at"),
]

# Distinct per column so a cross-column mixup cannot pass green.
# Deliberately naive: this seeds the PRE-migration schema shape (noqa: the one legit site).
_NAIVE = {pair: datetime(2026, 6, 1, 10, 0, 0) + timedelta(minutes=i) for i, pair in enumerate(_COLUMNS)}  # noqa: DTZ001
_AWARE = {pair: ts.replace(tzinfo=UTC) for pair, ts in _NAIVE.items()}

# One row per affected table, id=1, in FK order. The non-timestamp columns are the NOT NULL
# ones without a server default at revision b3d7f1a9c204 (that schema is frozen, so these
# literals are stable). `interfaces` carries no affected column — it is only the FK parent.
_SEED_ROWS: list[tuple[str, dict[str, str]]] = [
    (
        "devices",
        {
            "id": "1",
            "nso_instance": "'nso-ts'",
            "nso_device_name": "'ts-dev'",
            "mapping_status": "'mapped'",
            "source_epoch": "1",
        },
    ),
    ("interfaces", {"id": "1", "device_id": "1", "name": "'ge-0/0/0'"}),
    ("managed_scope", {"id": "1", "device_id": "1", "attribute": "'description'"}),
    (
        "interface_attr_state",
        {"id": "1", "interface_id": "1", "attribute": "'description'", "sync_state": "'imported'"},
    ),
    ("jobs", {"id": "1", "job_type": "'apply'", "status": "'queued'"}),
    ("device_settings", {"id": "1", "device_id": "1", "auto_apply": "false"}),
    ("device_failover", {"id": "1", "device_id": "1"}),
    ("failover_config", {"id": "1"}),
    ("interface_intent", {"id": "1", "interface_id": "1", "attribute": "'description'"}),
    (
        "device_route_policy_prefix_list",
        {"id": "1", "device_id": "1", "name": "'PL1'", "family": "4", "refresh_source": "'poll'"},
    ),
    (
        "device_route_policy_community_list",
        {"id": "1", "device_id": "1", "name": "'CL1'", "refresh_source": "'poll'"},
    ),
    ("device_route_policy_as_path", {"id": "1", "device_id": "1", "name": "'AP1'", "refresh_source": "'poll'"}),
    ("device_route_policy_route_map", {"id": "1", "device_id": "1", "name": "'RM1'", "refresh_source": "'poll'"}),
    (
        "route_policy_object_intent",
        {"id": "1", "device_id": "1", "family": "'prefix_list'", "name": "'PL1'", "entries": "'[]'::json"},
    ),
]


def _load_migration():
    """Import the migration module by path. Missing file == red, by design."""
    spec = importlib.util.spec_from_file_location("_ts_normalization_migration", _MIGRATION_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _alembic(sync_url: str, *args: str) -> str:
    """Run alembic in a SUBPROCESS (N13a: env.py's fileConfig reconfigures the root logger
    in-process) under the non-UTC session TimeZone the guard depends on."""
    proc = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=_REPO_ROOT,
        capture_output=True,
        env={**os.environ, "DATABASE_URL": sync_url, "PGOPTIONS": f"-c timezone={_TZ}"},
        check=False,
    )
    if proc.returncode != 0:
        raise AssertionError(f"alembic {args} failed:\n{proc.stdout.decode()}\n{proc.stderr.decode()}")
    return proc.stdout.decode()


@contextmanager
def _private_database(pg_admin, tag: str):
    """A database of our own — the template clone is useless here (already at head)."""
    name = f"nsoadp_ts{tag}_{uuid.uuid4().hex[:8]}"
    with pg_admin.connect() as conn:
        conn.exec_driver_sql(f'CREATE DATABASE "{name}"')
    try:
        yield _url_for(name, driver="postgresql+psycopg2")
    finally:
        _drop_database(pg_admin, name, expect_clean=False)


@contextmanager
def _engine_on(sync_url: str):
    engine = sa.create_engine(sync_url, poolclass=sa.pool.NullPool)
    try:
        yield engine
    finally:
        engine.dispose()


def _assert_session_timezone(engine) -> None:
    """The premise of every assertion below: the session really is NOT on UTC.

    ALTER DATABASE ... SET timezone only affects new connections and libpq startup options
    override it, so the zone is asserted from inside the database rather than assumed.
    """
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SHOW TimeZone").scalar_one() == _TZ


def _seed(engine, values: dict[tuple[str, str], datetime]) -> None:
    with engine.begin() as conn:
        for table, literals in _SEED_ROWS:
            cols = dict(literals)
            params = {}
            for (tbl, col), ts in values.items():
                if tbl == table:
                    cols[col] = f":{col}"
                    params[col] = ts
            names = ", ".join(cols)
            vals = ", ".join(cols.values())
            conn.execute(sa.text(f"INSERT INTO {table} ({names}) VALUES ({vals})"), params)


def _read_back(engine) -> dict[tuple[str, str], datetime]:
    out = {}
    with engine.connect() as conn:
        for table, col in _COLUMNS:
            out[table, col] = conn.execute(sa.text(f"SELECT {col} FROM {table} WHERE id = 1")).scalar_one()
    return out


def _reflected_tz_flags(engine) -> dict[tuple[str, str], bool]:
    insp = sa.inspect(engine)
    by_table = {table: {c["name"]: c["type"] for c in insp.get_columns(table)} for table, _ in _COLUMNS}
    return {(t, c): bool(by_table[t][c].timezone) for t, c in _COLUMNS}


def test_migration_converts_every_naive_column_to_the_same_utc_instant(pg_admin, monkeypatch):
    module = _load_migration()
    monkeypatch.setenv("PGOPTIONS", f"-c timezone={_TZ}")  # same mechanism the subprocess uses

    with _private_database(pg_admin, "conv") as sync_url:
        _alembic(sync_url, "upgrade", module.down_revision)

        with _engine_on(sync_url) as engine:
            _assert_session_timezone(engine)
            assert _reflected_tz_flags(engine) == dict.fromkeys(_COLUMNS, False)
            _seed(engine, _NAIVE)

        _alembic(sync_url, "upgrade", "head")

        with _engine_on(sync_url) as engine:
            _assert_session_timezone(engine)
            assert _reflected_tz_flags(engine) == dict.fromkeys(_COLUMNS, True)
            assert _read_back(engine) == _AWARE


def test_migration_declares_all_25_columns_with_an_explicit_utc_using_clause():
    module = _load_migration()
    assert sorted(tuple(pair) for pair in module._COLUMNS) == sorted(_COLUMNS)

    tree = ast.parse(_MIGRATION_PATH.read_text())
    functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
    using = ast.unparse(ast.parse("f\"{col} AT TIME ZONE 'UTC'\"").body[0].value)
    for name, expected_type in (("upgrade", "sa.DateTime(timezone=True)"), ("downgrade", "sa.DateTime()")):
        calls = [
            node
            for node in ast.walk(functions[name])
            if isinstance(node, ast.Call) and ast.unparse(node.func) == "op.alter_column"
        ]
        assert calls, f"{name}() emits no op.alter_column"
        for call in calls:
            kwargs = {kw.arg: kw.value for kw in call.keywords}
            assert ast.unparse(kwargs["postgresql_using"]) == using, f"{name}: wrong USING expression"
            assert ast.unparse(kwargs["type_"]) == expected_type, f"{name}: wrong target type"

    # ...and per column, on the SQL alembic actually renders. `--sql` is offline mode: the URL
    # is never connected to. The loop above proves one source-level clause; this proves all 25
    # rendered statements carry it.
    up = _alembic(ADMIN_URL, "upgrade", f"{module.down_revision}:{module.revision}", "--sql")
    down = _alembic(ADMIN_URL, "downgrade", f"{module.revision}:{module.down_revision}", "--sql")
    for table, col in _COLUMNS:
        assert (
            f"ALTER TABLE {table} ALTER COLUMN {col} TYPE TIMESTAMP WITH TIME ZONE USING {col} AT TIME ZONE 'UTC'" in up
        )
        assert (
            f"ALTER TABLE {table} ALTER COLUMN {col} TYPE TIMESTAMP WITHOUT TIME ZONE USING {col} AT TIME ZONE 'UTC'"
            in down
        )


def test_downgrade_restores_naive_utc_and_re_upgrade_is_idempotent(pg_admin, monkeypatch):
    module = _load_migration()  # fail with the same import error as the others when it is missing
    monkeypatch.setenv("PGOPTIONS", f"-c timezone={_TZ}")

    with _private_database(pg_admin, "down") as sync_url:
        _alembic(sync_url, "upgrade", "head")

        with _engine_on(sync_url) as engine:
            _assert_session_timezone(engine)
            _seed(engine, _AWARE)

        # Target the revision by name, not "-1": later migrations chain on top of this one.
        _alembic(sync_url, "downgrade", module.down_revision)

        with _engine_on(sync_url) as engine:
            _assert_session_timezone(engine)
            assert _reflected_tz_flags(engine) == dict.fromkeys(_COLUMNS, False)
            assert _read_back(engine) == _NAIVE

        _alembic(sync_url, "upgrade", "head")

        with _engine_on(sync_url) as engine:
            assert _reflected_tz_flags(engine) == dict.fromkeys(_COLUMNS, True)
            assert _read_back(engine) == _AWARE
