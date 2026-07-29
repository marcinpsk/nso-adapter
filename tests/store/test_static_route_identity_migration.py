# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Guards for the #1396 R1a schema: route identity, tombstones, deferrable identity.

The migration adds ``static_route_intent.route_id``/``deployed_key``, the
``static_route_tombstone`` table, the per-(device, route_id) partial unique index,
and converts ``uq_staticrouteintent_identity`` to ``DEFERRABLE INITIALLY DEFERRED``.

DDL assertions alone are not enough: a restrictive FK where CASCADE was intended
passes a "constraint exists" check and then makes offboard raise, so the FK actions
are read from ``information_schema`` AND exercised against real rows.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import sqlalchemy as sa

from tests.conftest import _drop_database, _url_for, seed_device, session

_REPO_ROOT = Path(__file__).resolve().parents[2]

# Referenced by path: before the migration exists this import fails, which is the red evidence.
# down_revision is read FROM the module so test and migration cannot drift apart.
_MIGRATION_PATH = _REPO_ROOT / "alembic" / "versions" / "d5f2a9b16e83_static_route_identity_and_tombstones.py"

_PARTIAL_UNIQUE_PREDICATE = "(route_id IS NOT NULL)"
_TOMBSTONE_UNCLAIMED_PREDICATE = "(job_id IS NULL)"


def _load_migration():
    spec = importlib.util.spec_from_file_location("_sr_identity_migration", _MIGRATION_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _alembic(sync_url: str, *args: str) -> str:
    """Run alembic in a SUBPROCESS (env.py's fileConfig reconfigures the root logger)."""
    proc = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=_REPO_ROOT,
        capture_output=True,
        env={**os.environ, "DATABASE_URL": sync_url},
        check=False,
    )
    if proc.returncode != 0:
        raise AssertionError(f"alembic {args} failed:\n{proc.stdout.decode()}\n{proc.stderr.decode()}")
    return proc.stdout.decode()


@contextmanager
def _private_database(pg_admin, tag: str):
    """A database of our own — the per-test template clone is already at head."""
    name = f"nsoadp_sr{tag}_{uuid.uuid4().hex[:8]}"
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


def _index_predicates(engine, table: str) -> dict[str, tuple[tuple, bool, str | None]]:
    insp = sa.inspect(engine)
    return {
        i["name"]: (
            tuple(i.get("expressions") or i["column_names"]),
            i["unique"],
            (i.get("dialect_options") or {}).get("postgresql_where"),
        )
        for i in insp.get_indexes(table)
    }


def _delete_rules(engine, table: str) -> dict[str, str]:
    """FK delete actions read from information_schema, not from the model."""
    sql = sa.text(
        """
        SELECT kcu.column_name, rc.delete_rule
          FROM information_schema.referential_constraints rc
          JOIN information_schema.key_column_usage kcu
            ON kcu.constraint_name = rc.constraint_name
           AND kcu.constraint_schema = rc.constraint_schema
         WHERE kcu.table_name = :table AND kcu.table_schema = 'public'
        """
    )
    with engine.connect() as conn:
        return {col: rule for col, rule in conn.execute(sql, {"table": table})}


def _deferrability(engine, name: str) -> tuple[bool, bool]:
    sql = sa.text("SELECT condeferrable, condeferred FROM pg_constraint WHERE conname = :name")
    with engine.connect() as conn:
        return tuple(conn.execute(sql, {"name": name}).one())


def _column(engine, table: str, name: str) -> dict:
    return next(c for c in sa.inspect(engine).get_columns(table) if c["name"] == name)


# ── structural ───────────────────────────────────────────────────────────────


def test_migration_chains_off_the_real_head():
    """A stale down_revision creates a second head and `upgrade head` then refuses."""
    module = _load_migration()
    heads = _alembic(_url_for("postgres", driver="postgresql+psycopg2"), "heads")
    assert module.revision in heads, f"{module.revision} is not the alembic head:\n{heads}"


def test_intent_columns_and_partial_unique_index(pg_admin):
    with _private_database(pg_admin, "cols") as sync_url:
        _alembic(sync_url, "upgrade", "head")
        with _engine_on(sync_url) as engine:
            route_id = _column(engine, "static_route_intent", "route_id")
            assert route_id["nullable"] is True
            assert isinstance(route_id["type"], sa.Integer)

            deployed_key = _column(engine, "static_route_intent", "deployed_key")
            assert deployed_key["nullable"] is True
            assert deployed_key["type"].__class__.__name__ == "JSONB"

            ixs = _index_predicates(engine, "static_route_intent")
            assert ixs["uq_sr_intent_device_route_id"] == (
                ("device_id", "route_id"),
                True,
                _PARTIAL_UNIQUE_PREDICATE,
            )


def test_identity_constraint_is_deferrable_initially_deferred(pg_admin):
    """§3.7: an immediate constraint rejects legal same-payload swaps and reclaims."""
    with _private_database(pg_admin, "defer") as sync_url:
        _alembic(sync_url, "upgrade", "head")
        with _engine_on(sync_url) as engine:
            assert _deferrability(engine, "uq_staticrouteintent_identity") == (True, True)


def test_tombstone_table_shape(pg_admin):
    with _private_database(pg_admin, "tomb") as sync_url:
        _alembic(sync_url, "upgrade", "head")
        with _engine_on(sync_url) as engine:
            insp = sa.inspect(engine)
            cols = {c["name"]: c for c in insp.get_columns("static_route_tombstone")}
            # route_id NOT NULL: the fence guarantees every tombstoned row carried one, and a
            # NULL there would be an uncorrelatable deletion authority for R2's CAS.
            assert cols["route_id"]["nullable"] is False
            assert cols["device_id"]["nullable"] is False
            assert cols["prefix"]["nullable"] is False
            assert cols["vrf"]["nullable"] is False
            assert cols["next_hop"]["nullable"] is False
            assert cols["marking"]["nullable"] is False
            assert cols["deployed_key"]["nullable"] is True
            assert cols["deployed_key"]["type"].__class__.__name__ == "JSONB"
            assert cols["job_id"]["nullable"] is True
            assert cols["created_at"]["nullable"] is False

            checks = [c["sqltext"] for c in insp.get_check_constraints("static_route_tombstone")]
            assert any("delete_origin" in c and "detach" in c for c in checks), checks

            ixs = _index_predicates(engine, "static_route_tombstone")
            assert ixs["ix_srt_unclaimed"] == (("device_id", "id"), False, _TOMBSTONE_UNCLAIMED_PREDICATE)


def test_new_foreign_keys_carry_their_intended_delete_rule(pg_admin):
    """B9: a DDL-only assertion passes against a restrictive FK that then breaks offboard."""
    with _private_database(pg_admin, "fk") as sync_url:
        _alembic(sync_url, "upgrade", "head")
        with _engine_on(sync_url) as engine:
            rules = _delete_rules(engine, "static_route_tombstone")
            assert rules["device_id"] == "CASCADE"
            assert rules["job_id"] == "SET NULL"


# ── backfill ─────────────────────────────────────────────────────────────────


_SEED_DEVICE = (
    "INSERT INTO devices (id, nso_instance, nso_device_name, mapping_status, source_epoch, created_at, updated_at) "
    "VALUES (1, 'nso-sr', 'sr-dev', 'mapped', 1, now(), now())"
)


def test_backfill_sets_deployed_key_only_for_applied_rows(pg_admin):
    module = _load_migration()
    with _private_database(pg_admin, "fill") as sync_url:
        _alembic(sync_url, "upgrade", module.down_revision)
        with _engine_on(sync_url) as engine, engine.begin() as conn:
            conn.exec_driver_sql(_SEED_DEVICE)
            conn.exec_driver_sql(
                "INSERT INTO static_route_intent (id, device_id, vrf, prefix, next_hop, last_apply_at) "
                "VALUES (1, 1, 'blue', '10.0.0.0/24', '192.0.2.1', now())"
            )
            conn.exec_driver_sql(
                "INSERT INTO static_route_intent (id, device_id, vrf, prefix, next_hop, last_apply_at) "
                "VALUES (2, 1, '', '10.0.1.0/24', '192.0.2.2', NULL)"
            )

        _alembic(sync_url, "upgrade", "head")

        with _engine_on(sync_url) as engine, engine.connect() as conn:
            applied = conn.exec_driver_sql("SELECT deployed_key FROM static_route_intent WHERE id = 1").scalar_one()
            assert applied == ["blue", "10.0.0.0/24", "192.0.2.1"]
            # SQL NULL, not 'null'::jsonb — the "no proven predecessor" test is IS NULL.
            assert (
                conn.exec_driver_sql("SELECT deployed_key IS NULL FROM static_route_intent WHERE id = 2").scalar_one()
                is True
            )
            # route_id is NOT backfilled: the adapter cannot learn NetBox pks, and NULL is
            # what keeps the rollout fence shut until R3 and the fleet resync fill it.
            assert (
                conn.exec_driver_sql("SELECT count(*) FROM static_route_intent WHERE route_id IS NULL").scalar_one()
                == 2
            )


def test_downgrade_restores_the_previous_shape(pg_admin):
    module = _load_migration()
    with _private_database(pg_admin, "down") as sync_url:
        _alembic(sync_url, "upgrade", "head")
        _alembic(sync_url, "downgrade", "-1")
        with _engine_on(sync_url) as engine:
            insp = sa.inspect(engine)
            assert "static_route_tombstone" not in insp.get_table_names()
            cols = {c["name"] for c in insp.get_columns("static_route_intent")}
            assert "route_id" not in cols
            assert "deployed_key" not in cols
            assert "uq_sr_intent_device_route_id" not in _index_predicates(engine, "static_route_intent")
            assert _deferrability(engine, "uq_staticrouteintent_identity") == (False, False)

        _alembic(sync_url, "upgrade", "head")
        with _engine_on(sync_url) as engine:
            assert "static_route_tombstone" in sa.inspect(engine).get_table_names()
            assert _deferrability(engine, "uq_staticrouteintent_identity") == (True, True)
        assert module.down_revision  # the module really was the one under test


# ── FK behavior, against real rows ───────────────────────────────────────────


async def _seed_tombstone(device_id: int, *, route_id: int = 7, job_id: int | None = None) -> int:
    from nso_adapter.store.models import StaticRouteTombstone

    async with session() as db:
        row = StaticRouteTombstone(
            device_id=device_id,
            route_id=route_id,
            vrf="",
            prefix="10.0.0.0/24",
            next_hop="192.0.2.1",
            deployed_key=None,
            marking="detach",
            job_id=job_id,
        )
        db.add(row)
        await db.commit()
        return row.id


async def test_offboard_cascades_tombstones(adapter_client):
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.store.models import Device, StaticRouteTombstone

    device_id = await seed_device(nso_device_name="sr-tomb-cascade", netbox_device_id=9601)
    await _seed_tombstone(device_id)

    async with session() as db:
        await offboard_device(db, await db.get(Device, device_id))
    async with session() as db:
        remaining = (
            (await db.execute(sa.select(StaticRouteTombstone).where(StaticRouteTombstone.device_id == device_id)))
            .scalars()
            .all()
        )
        assert remaining == []


async def test_deleting_the_owning_job_nulls_job_id_and_keeps_the_tombstone(adapter_client):
    """SET NULL, not CASCADE: the tombstone outlives its job and the sweeper re-owns it."""
    from nso_adapter.store.models import Job, JobStatus, JobType, StaticRouteTombstone

    device_id = await seed_device(nso_device_name="sr-tomb-job", netbox_device_id=9602)
    async with session() as db:
        job = Job(job_type=JobType.removal, device_id=device_id, status=JobStatus.succeeded, context={})
        db.add(job)
        await db.commit()
        job_id = job.id

    tomb_id = await _seed_tombstone(device_id, job_id=job_id)

    async with session() as db:
        await db.execute(sa.delete(Job).where(Job.id == job_id))
        await db.commit()
    async with session() as db:
        row = await db.get(StaticRouteTombstone, tomb_id)
        assert row is not None
        assert row.job_id is None


async def test_tombstone_route_id_null_is_rejected(adapter_client):
    from sqlalchemy.exc import IntegrityError

    from nso_adapter.store.models import StaticRouteTombstone

    device_id = await seed_device(nso_device_name="sr-tomb-null", netbox_device_id=9603)
    async with session() as db:
        db.add(
            StaticRouteTombstone(
                device_id=device_id,
                route_id=None,
                vrf="",
                prefix="10.0.0.0/24",
                next_hop="192.0.2.1",
                marking="detach",
            )
        )
        try:
            await db.commit()
        except IntegrityError:
            pass
        else:  # pragma: no cover - the guard is the point
            raise AssertionError("a NULL route_id tombstone was accepted")


async def test_tombstone_marking_is_constrained(adapter_client):
    from sqlalchemy.exc import IntegrityError

    from nso_adapter.store.models import StaticRouteTombstone

    device_id = await seed_device(nso_device_name="sr-tomb-mark", netbox_device_id=9604)
    async with session() as db:
        db.add(
            StaticRouteTombstone(
                device_id=device_id,
                route_id=7,
                vrf="",
                prefix="10.0.0.0/24",
                next_hop="192.0.2.1",
                marking="wat",
            )
        )
        try:
            await db.commit()
        except IntegrityError:
            pass
        else:  # pragma: no cover - the guard is the point
            raise AssertionError("an unknown marking was accepted")


async def test_route_id_is_unique_per_device_but_nulls_coexist(adapter_client):
    from sqlalchemy.exc import IntegrityError

    from nso_adapter.store.models import StaticRouteIntent

    device_id = await seed_device(nso_device_name="sr-rid-uq", netbox_device_id=9605)

    def _row(route_id, prefix):
        return StaticRouteIntent(device_id=device_id, vrf="", prefix=prefix, next_hop="192.0.2.1", route_id=route_id)

    async with session() as db:  # NULLs are not constrained — the pre-fence shape
        db.add_all([_row(None, "10.0.0.0/24"), _row(None, "10.0.1.0/24")])
        await db.commit()

    async with session() as db:
        db.add_all([_row(7, "10.0.2.0/24"), _row(7, "10.0.3.0/24")])
        try:
            await db.commit()
        except IntegrityError:
            pass
        else:  # pragma: no cover - the guard is the point
            raise AssertionError("two live rows claimed the same (device, route_id)")


async def test_deferred_identity_constraint_allows_an_in_transaction_swap(adapter_client):
    """The behavior §3.7 mandates: A↔B inside one transaction, checked at COMMIT."""
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await seed_device(nso_device_name="sr-swap", netbox_device_id=9606)
    async with session() as db:
        a = StaticRouteIntent(device_id=device_id, vrf="", prefix="10.0.0.0/24", next_hop="192.0.2.1", route_id=7)
        b = StaticRouteIntent(device_id=device_id, vrf="", prefix="10.0.1.0/24", next_hop="192.0.2.2", route_id=8)
        db.add_all([a, b])
        await db.commit()
        a_id, b_id = a.id, b.id

    async with session() as db:
        row_a = await db.get(StaticRouteIntent, a_id)
        row_b = await db.get(StaticRouteIntent, b_id)
        row_a.prefix, row_a.next_hop = "10.0.1.0/24", "192.0.2.2"
        row_b.prefix, row_b.next_hop = "10.0.0.0/24", "192.0.2.1"
        row_a.accepted_at = datetime.now(UTC)
        await db.commit()

    async with session() as db:
        assert (await db.get(StaticRouteIntent, a_id)).prefix == "10.0.1.0/24"
        assert (await db.get(StaticRouteIntent, b_id)).prefix == "10.0.0.0/24"
