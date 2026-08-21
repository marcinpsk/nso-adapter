# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Migration guards for the two job queue classes."""

from __future__ import annotations

import json

import pytest
import sqlalchemy as sa

from tests.store.migration_harness import (
    alembic,
    assert_single_head_containing,
    engine_on,
    index_predicates,
    load_migration,
    private_database,
)

_MIGRATION = "b9e3d7a1c5f2_job_queue_classes.py"


def _module():
    return load_migration(_MIGRATION)


def _seed_device(conn, name: str) -> int:
    return conn.execute(
        sa.text(
            "INSERT INTO devices (nso_instance, nso_device_name, mapping_status, created_at, updated_at) "
            "VALUES ('nso-test', :name, 'mapped', now(), now()) RETURNING id"
        ),
        {"name": name},
    ).scalar_one()


def _seed_job(
    conn,
    token: str,
    job_type: str,
    status: str,
    device_id: int | None,
    *,
    context: dict | None = None,
) -> int:
    return conn.execute(
        sa.text(
            "INSERT INTO jobs (job_type, status, device_id, result, context, created_at, updated_at) "
            "VALUES (:job_type, :status, :device_id, CAST(:result AS json), CAST(:context AS json), now(), now()) "
            "RETURNING id"
        ),
        {
            "job_type": job_type,
            "status": status,
            "device_id": device_id,
            "result": json.dumps(token),
            "context": json.dumps(context) if context is not None else None,
        },
    ).scalar_one()


def _seed_generation(conn, device_id: int, job_id: int, seq: int, *, status: str = "pending") -> int:
    return conn.execute(
        sa.text(
            "INSERT INTO deployment_generation "
            "(device_id, seq, mode, status, document, digest, allowed_removal_keys, source_push_seq, "
            "stream_revisions, job_id, created_at, updated_at) "
            "VALUES (:device_id, :seq, 'networked', :status, '{}'::json, :digest, '{}'::json, '{}'::json, "
            "'{}'::json, :job_id, now(), now()) RETURNING id"
        ),
        {"device_id": device_id, "seq": seq, "status": status, "digest": f"digest-{seq}", "job_id": job_id},
    ).scalar_one()


def _insert_classified_job(
    conn,
    token: str,
    job_type: str,
    status: str,
    device_id: int | None,
    coalescible: bool,
) -> int:
    return conn.execute(
        sa.text(
            "INSERT INTO jobs "
            "(job_type, status, device_id, coalescible, result, context, created_at, updated_at) "
            "VALUES (:job_type, :status, :device_id, :coalescible, CAST(:result AS json), '{}'::json, now(), now()) "
            "RETURNING id"
        ),
        {
            "job_type": job_type,
            "status": status,
            "device_id": device_id,
            "coalescible": coalescible,
            "result": json.dumps(token),
        },
    ).scalar_one()


def test_job_queue_class_migration_chains_off_the_previous_head():
    module = _module()
    assert module.down_revision == "e4a8c2d1f6b9"
    assert_single_head_containing(module.revision)


def test_job_queue_class_schema_matches_the_lane_contract(pg_admin):
    module = _module()
    with private_database(pg_admin, "jobclasses") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine:
            inspector = sa.inspect(engine)
            column = {item["name"]: item for item in inspector.get_columns("jobs")}["coalescible"]
            assert isinstance(column["type"], sa.Boolean)
            assert column["nullable"] is False
            assert column["default"] is None
            assert {item["name"] for item in inspector.get_check_constraints("jobs")} >= {
                "ck_job_removal_not_coalescible",
                "ck_job_provision_not_coalescible",
                "ck_job_active_provision_without_device",
                "ck_job_detached_non_provision_terminal",
            }
            assert index_predicates(engine, "jobs")["uq_job_queued_per_device_type"] == (
                ("device_id", "job_type"),
                True,
                "((status = 'queued'::jobstatus) AND coalescible)",
            )


def test_existing_jobs_use_their_construction_lane_for_backfill(pg_admin):
    module = _module()
    with private_database(pg_admin, "jobclassfill") as sync_url:
        alembic(sync_url, "upgrade", module.down_revision)
        with engine_on(sync_url) as engine, engine.begin() as conn:
            device_id = _seed_device(conn, "queue-class-backfill")
            removal_with_generation = None
            multi_generation_apply = None
            cases = (
                ("removal-without-generation", "removal", "succeeded", device_id, False, None),
                ("removal-with-generation", "removal", "failed", device_id, False, None),
                ("queued-device-job", "sync", "queued", device_id, True, None),
                ("running-device-job", "connect", "running", device_id, True, None),
                ("succeeded-device-job", "detect_drift", "succeeded", device_id, True, None),
                ("failed-device-job", "sync_from_nso", "failed", device_id, True, None),
                ("multi-generation-apply", "apply", "succeeded", device_id, True, None),
                (
                    "queued-provision",
                    "provision",
                    "queued",
                    None,
                    False,
                    {"nso_instance": "nso-test", "device_name": "queued-provision"},
                ),
                (
                    "running-provision",
                    "provision",
                    "running",
                    None,
                    False,
                    {"nso_instance": "nso-test", "device_name": "running-provision"},
                ),
                ("attached-provision-history", "provision", "succeeded", device_id, False, None),
                ("detached-job-history", "sync", "succeeded", None, True, None),
                ("detached-removal-history", "removal", "succeeded", None, False, None),
            )
            expected = {}
            for token, job_type, status, target_device_id, coalescible, context in cases:
                job_id = _seed_job(conn, token, job_type, status, target_device_id, context=context)
                expected[token] = coalescible
                if token == "removal-with-generation":
                    removal_with_generation = job_id
                elif token == "multi-generation-apply":
                    multi_generation_apply = job_id
            assert removal_with_generation is not None
            assert multi_generation_apply is not None
            _seed_generation(conn, device_id, removal_with_generation, 1)
            _seed_generation(conn, device_id, multi_generation_apply, 2)
            _seed_generation(conn, device_id, multi_generation_apply, 3)

        alembic(sync_url, "upgrade", module.revision)

        with engine_on(sync_url) as engine, engine.connect() as conn:
            landed = dict(conn.exec_driver_sql("SELECT result #>> '{}', coalescible FROM jobs").all())
        assert landed == expected


def test_migration_rejects_a_removal_job_with_multiple_generations(pg_admin):
    module = _module()
    with private_database(pg_admin, "jobclassbadremoval") as sync_url:
        alembic(sync_url, "upgrade", module.down_revision)
        with engine_on(sync_url) as engine, engine.begin() as conn:
            device_id = _seed_device(conn, "queue-class-corrupt-removal")
            job_id = _seed_job(conn, "corrupt-removal", "removal", "failed", device_id)
            _seed_generation(conn, device_id, job_id, 1)
            _seed_generation(conn, device_id, job_id, 2)

        with pytest.raises(AssertionError, match="removal job carries more than one generation"):
            alembic(sync_url, "upgrade", module.revision)

        with engine_on(sync_url) as engine:
            assert "coalescible" not in {item["name"] for item in sa.inspect(engine).get_columns("jobs")}


@pytest.mark.parametrize(("job_status", "generation_status"), (("queued", "pending"), ("running", "running")))
def test_migration_rejects_an_active_device_bound_apply_retry(
    pg_admin,
    job_status: str,
    generation_status: str,
):
    module = _module()
    with private_database(pg_admin, f"jobclassactiveapply{job_status}") as sync_url:
        alembic(sync_url, "upgrade", module.down_revision)
        with engine_on(sync_url) as engine, engine.begin() as conn:
            device_id = _seed_device(conn, f"queue-class-active-apply-{job_status}")
            job_id = _seed_job(conn, f"active-apply-{job_status}", "apply", job_status, device_id)
            _seed_generation(conn, device_id, job_id, 1, status=generation_status)

        with pytest.raises(AssertionError, match="device-bound Apply job is queued or running"):
            alembic(sync_url, "upgrade", module.revision)

        with engine_on(sync_url) as engine:
            assert "coalescible" not in {item["name"] for item in sa.inspect(engine).get_columns("jobs")}


def test_lane_checks_reject_rows_outside_the_four_classes(pg_admin):
    module = _module()
    with private_database(pg_admin, "jobclasschecks") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine, engine.begin() as conn:
            device_id = _seed_device(conn, "queue-class-checks")

        invalid_cases = (
            ("coalescible-removal", "removal", "succeeded", device_id, True),
            ("coalescible-provision", "provision", "succeeded", device_id, True),
            ("queued-attached-provision", "provision", "queued", device_id, False),
            ("running-attached-provision", "provision", "running", device_id, False),
            ("queued-detached-sync", "sync", "queued", None, True),
            ("running-detached-connect", "connect", "running", None, False),
        )
        for case in invalid_cases:
            with pytest.raises(sa.exc.IntegrityError):
                with engine_on(sync_url) as engine, engine.begin() as conn:
                    _insert_classified_job(conn, *case)


def test_queued_deduplication_applies_only_to_coalescible_jobs(pg_admin):
    module = _module()
    with private_database(pg_admin, "jobclassdedupe") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        with engine_on(sync_url) as engine, engine.begin() as conn:
            device_id = _seed_device(conn, "queue-class-dedupe")
            _insert_classified_job(conn, "coalescible-apply", "apply", "queued", device_id, True)
            _insert_classified_job(conn, "dedicated-apply", "apply", "queued", device_id, False)

        with pytest.raises(sa.exc.IntegrityError):
            with engine_on(sync_url) as engine, engine.begin() as conn:
                _insert_classified_job(conn, "second-coalescible-apply", "apply", "queued", device_id, True)

        with engine_on(sync_url) as engine, engine.connect() as conn:
            landed = dict(conn.exec_driver_sql("SELECT result #>> '{}', coalescible FROM jobs").all())
        assert landed == {"coalescible-apply": True, "dedicated-apply": False}


def test_job_queue_class_schema_is_reversible(pg_admin):
    module = _module()
    with private_database(pg_admin, "jobclassesrev") as sync_url:
        alembic(sync_url, "upgrade", module.revision)
        alembic(sync_url, "downgrade", module.down_revision)
        with engine_on(sync_url) as engine:
            inspector = sa.inspect(engine)
            assert "coalescible" not in {item["name"] for item in inspector.get_columns("jobs")}
            assert {item["name"] for item in inspector.get_check_constraints("jobs")}.isdisjoint(
                {
                    "ck_job_removal_not_coalescible",
                    "ck_job_provision_not_coalescible",
                    "ck_job_active_provision_without_device",
                    "ck_job_detached_non_provision_terminal",
                }
            )
            assert index_predicates(engine, "jobs")["uq_job_queued_per_device_type"][2] == (
                "((status = 'queued'::jobstatus) AND (job_type <> 'removal'::jobtype))"
            )
