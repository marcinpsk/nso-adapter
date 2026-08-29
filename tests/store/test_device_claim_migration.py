# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Guards for the ``device_claim`` table — R1b's exclusive per-device execution claim.

One row per device IS the mutual exclusion: ``device_id`` is the primary key, so
``INSERT … ON CONFLICT (device_id) DO NOTHING`` is decided by PostgreSQL across
connections and processes rather than by any application check.

``claim_token`` is unique because it is per-ACQUISITION, not per-process: two
acquisitions by the same process must never share one, or a revoked runner's writes
validate against its successor's claim.
"""

from __future__ import annotations

import sqlalchemy as sa

from tests.conftest import seed_device, session
from tests.store.migration_harness import (
    alembic,
    assert_single_head_containing,
    delete_rules,
    engine_on,
    load_migration,
    private_database,
)

_MIGRATION = "c7e4b8a05d19_device_claim.py"

PURPOSES = ("job", "intent_put", "teardown", "sweep", "failover")


def _module():
    return load_migration(_MIGRATION)


# ── structural ───────────────────────────────────────────────────────────────


def test_migration_chains_off_the_static_route_identity_revision():
    module = _module()
    assert module.down_revision == "d5f2a9b16e83"
    assert_single_head_containing(module.revision)


def test_device_claim_table_shape(pg_provisioner):
    module = _module()
    with private_database(pg_provisioner, "dcshape") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            insp = sa.inspect(engine)
            cols = {c["name"]: c for c in insp.get_columns("device_claim")}

            # device_id is the PRIMARY KEY — that is the exclusion mechanism.
            assert insp.get_pk_constraint("device_claim")["constrained_columns"] == ["device_id"]
            assert cols["device_id"]["nullable"] is False
            assert cols["claim_token"]["nullable"] is False
            assert cols["purpose"]["nullable"] is False
            # NULL at the worker's queued-head acquisition (an inserted FK takes FOR KEY
            # SHARE on the job and would block on an endpoint's winner lock); set later in
            # the guarded transaction. Provision is the exception and sets it at acquisition.
            assert cols["job_id"]["nullable"] is True
            assert cols["acquired_at"]["nullable"] is False
            assert cols["heartbeat_at"]["nullable"] is False

            uqs = {tuple(u["column_names"]) for u in insp.get_unique_constraints("device_claim")}
            assert ("claim_token",) in uqs

            checks = [c["sqltext"] for c in insp.get_check_constraints("device_claim")]
            assert any(all(p in c for p in PURPOSES) for c in checks), checks


def test_device_claim_foreign_keys_carry_their_intended_delete_rule(pg_provisioner):
    """Completes the four new FK actions this program owes (two here, two on the tombstone)."""
    module = _module()
    with private_database(pg_provisioner, "dcfk") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            rules = delete_rules(engine, "device_claim")
            assert rules["device_id"] == "CASCADE"
            assert rules["job_id"] == "SET NULL"


def test_downgrade_drops_the_table(pg_provisioner):
    """Named by revision id in both directions, never "head"/"-1"."""
    module = _module()
    with private_database(pg_provisioner, "dcdown") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        alembic(sync_url, "downgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            assert "device_claim" not in sa.inspect(engine).get_table_names()

        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            assert "device_claim" in sa.inspect(engine).get_table_names()


# ── behavior against real rows ───────────────────────────────────────────────


async def _insert_claim(device_id: int, *, token: str, purpose: str = "job", job_id: int | None = None):
    from nso_adapter.store.models import DeviceClaim

    async with session() as db:
        db.add(DeviceClaim(device_id=device_id, claim_token=token, purpose=purpose, job_id=job_id))
        await db.commit()


async def test_every_purpose_is_accepted(adapter_client):
    """All five holders acquire the same claim, so all five values must be storable."""
    from nso_adapter.store.models import DeviceClaim

    for index, purpose in enumerate(PURPOSES):
        device_id = await seed_device(nso_device_name=f"dc-purpose-{purpose}", netbox_device_id=9800 + index)
        await _insert_claim(device_id, token=f"tok-{purpose}", purpose=purpose)
        async with session() as db:
            row = await db.get(DeviceClaim, device_id)
            assert row.purpose == purpose


async def test_unknown_purpose_is_rejected(adapter_client):
    from sqlalchemy.exc import IntegrityError

    device_id = await seed_device(nso_device_name="dc-bad-purpose", netbox_device_id=9810)
    try:
        await _insert_claim(device_id, token="tok-bad", purpose="wat")
    except IntegrityError:
        return
    raise AssertionError("an unknown purpose was accepted")


async def test_one_claim_per_device(adapter_client):
    """The primary key is the mutual exclusion — a second acquisition must conflict."""
    from sqlalchemy.exc import IntegrityError

    device_id = await seed_device(nso_device_name="dc-one", netbox_device_id=9811)
    await _insert_claim(device_id, token="tok-first")
    try:
        await _insert_claim(device_id, token="tok-second")
    except IntegrityError:
        return
    raise AssertionError("two claims coexisted for one device")


async def test_claim_token_is_unique_across_devices(adapter_client):
    """Per-acquisition, never a process identity: reuse would let a revoked holder's
    writes validate against a successor's claim (ABA)."""
    from sqlalchemy.exc import IntegrityError

    first = await seed_device(nso_device_name="dc-tok-a", netbox_device_id=9812)
    second = await seed_device(nso_device_name="dc-tok-b", netbox_device_id=9813)
    await _insert_claim(first, token="shared-token")
    try:
        await _insert_claim(second, token="shared-token")
    except IntegrityError:
        return
    raise AssertionError("one token was reused across two devices")


async def test_deleting_the_device_cascades_the_claim(adapter_client):
    """The FK action itself, asserted at the database.

    Not driven through ``offboard_device`` any more: teardown is a claim holder, so it
    refuses to run while a rival claim exists and could never reach the delete.
    """
    from nso_adapter.store.models import Device, DeviceClaim

    # No managed scope: its FK is restrictive, and this test is about the claim's FK.
    device_id = await seed_device(nso_device_name="dc-cascade", netbox_device_id=9814, attributes=[])
    await _insert_claim(device_id, token="tok-cascade")

    async with session() as db:
        await db.execute(sa.delete(Device).where(Device.id == device_id))
        await db.commit()
    async with session() as db:
        assert await db.get(DeviceClaim, device_id) is None


async def test_offboard_leaves_no_claim_of_its_own(adapter_client):
    """Teardown's own claim goes with the device — it is never released separately."""
    from nso_adapter.core.onboarding import offboard_device
    from nso_adapter.store.models import Device, DeviceClaim

    device_id = await seed_device(nso_device_name="dc-cascade-own", netbox_device_id=9824)

    async with session() as db:
        await offboard_device(db, await db.get(Device, device_id))
    async with session() as db:
        assert await db.get(DeviceClaim, device_id) is None


async def test_deleting_the_owning_job_nulls_job_id_and_keeps_the_claim(adapter_client):
    """SET NULL, not CASCADE: losing the job must not silently free the device."""
    from nso_adapter.store.models import DeviceClaim, Job, JobStatus, JobType

    device_id = await seed_device(nso_device_name="dc-job-null", netbox_device_id=9815)
    async with session() as db:
        job = Job(job_type=JobType.sync, device_id=device_id, status=JobStatus.running, context={})
        db.add(job)
        await db.commit()
        job_id = job.id

    await _insert_claim(device_id, token="tok-job", job_id=job_id)

    async with session() as db:
        await db.execute(sa.delete(Job).where(Job.id == job_id))
        await db.commit()
    async with session() as db:
        row = await db.get(DeviceClaim, device_id)
        assert row is not None
        assert row.job_id is None


# ── Q4: the queued-dedupe index swap ─────────────────────────────────────────

_Q4_MIGRATION = "f1a3c9e7b204_queued_job_dedupe_index.py"


def test_queued_dedupe_index_replaces_the_active_one(pg_provisioner):
    from tests.store.migration_harness import index_predicates

    module = load_migration(_Q4_MIGRATION)
    assert module.down_revision == "c7e4b8a05d19"
    assert_single_head_containing(module.revision)

    with private_database(pg_provisioner, "q4idx") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            ixs = index_predicates(engine, "jobs")
            assert "uq_job_active_per_device" not in ixs
            assert ixs["uq_job_queued_per_device_type"] == (
                ("device_id", "job_type"),
                True,
                "((status = 'queued'::jobstatus) AND (job_type <> 'removal'::jobtype))",
            )

        alembic(sync_url, "downgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            ixs = index_predicates(engine, "jobs")
            assert "uq_job_queued_per_device_type" not in ixs
            assert "uq_job_active_per_device" in ixs


# ── the provision-pair admission index ───────────────────────────────────────

_PROVISION_INDEX_MIGRATION = "b8d4f1c2e7a3_provision_admission_index.py"


def test_provision_pair_index_is_on_the_two_context_expressions(pg_provisioner):
    """Parity proves model ≡ migration; this proves either one is RIGHT.

    Both expressions asserted verbatim: an index on a different context key — ``address``,
    say — deduplicates nothing while passing every parity and conflict-inference check.
    """
    from tests.store.migration_harness import index_predicates

    module = load_migration(_PROVISION_INDEX_MIGRATION)
    assert module.down_revision == "f1a3c9e7b204"
    assert_single_head_containing(module.revision)

    with private_database(pg_provisioner, "provix") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            assert index_predicates(engine, "jobs")["uq_job_active_provision_pair"] == (
                ("(context ->> 'nso_instance'::text)", "(context ->> 'device_name'::text)"),
                True,
                # queued AND running: a provision has no successor semantics.
                "((status = ANY (ARRAY['queued'::jobstatus, 'running'::jobstatus]))"
                " AND (job_type = 'provision'::jobtype))",
            )

        alembic(sync_url, "downgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            assert "uq_job_active_provision_pair" not in index_predicates(engine, "jobs")


def test_upgrade_reconciles_duplicates_the_missing_index_allowed(pg_provisioner):
    """The exact legacy state the old check-then-insert could produce must still upgrade.

    A migration that cannot install leaves the adapter unable to start, so the duplicates
    are terminalized — oldest kept — instead of colliding with CREATE UNIQUE INDEX. Rows
    with a NULL context key were never in conflict (NULLs are distinct) and must be left
    exactly as they are.
    """
    module = load_migration(_PROVISION_INDEX_MIGRATION)
    with private_database(pg_provisioner, "provdup") as sync_url:
        alembic(sync_url, "upgrade", module.down_revision)
        with engine_on(sync_url) as engine, engine.begin() as conn:
            for token, status, context in (
                ("winner", "queued", '{"nso_instance": "nso-dev", "device_name": "dup"}'),
                ("loser", "queued", '{"nso_instance": "nso-dev", "device_name": "dup"}'),
                ("running-loser", "running", '{"nso_instance": "nso-dev", "device_name": "dup"}'),
                ("other-node", "queued", '{"nso_instance": "nso-dev", "device_name": "solo"}'),
                ("no-context", "queued", "{}"),
                ("no-context-2", "queued", "{}"),
            ):
                conn.exec_driver_sql(
                    "INSERT INTO jobs (job_type, status, context, created_at, updated_at, result)"
                    " VALUES ('provision', %(status)s, %(context)s, now(), now(), %(token)s)",
                    {"status": status, "context": context, "token": f'"{token}"'},
                )

        alembic(sync_url, "upgrade", module.revision)

        with engine_on(sync_url) as engine, engine.connect() as conn:
            landed = {
                row[0]: (row[1], row[2])
                for row in conn.exec_driver_sql("SELECT result #>> '{}', status, error #>> '{}' FROM jobs")
            }
        assert landed["winner"][0] == "queued", "the oldest active provision was not kept"
        assert landed["loser"][0] == "failed" and "superseded" in landed["loser"][1]
        assert landed["running-loser"][0] == "failed"
        assert landed["other-node"][0] == "queued"
        # NULL context keys are distinct to the index: never in conflict, never touched.
        assert landed["no-context"][0] == "queued"
        assert landed["no-context-2"][0] == "queued"
