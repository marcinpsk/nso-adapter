# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The database refuses to rewrite a deployment generation's identity (#1558, #1522 §G1).

Direct SQL against the real schema, deliberately bypassing the ORM: the guarantee under test
is the trigger, and an application-level check proves nothing about a migration script, a
psql session, or a bug that reaches the table another way. What a generation DEPLOYS is
frozen; only its lifecycle moves.
"""

from __future__ import annotations

import pytest
import sqlalchemy as sa

from nso_adapter.store.ddl import GENERATION_IMMUTABLE_COLUMNS, _compare

_DOCUMENT = {"vlan": {"vlan_intent": [{"id": 1, "device_id": 1, "vlan_id": 10, "name": "v"}]}}

#: (column, a value that differs from the seeded row), one per immutable column.
#: ``device_id`` names the SECOND seeded device through a bind rather than a literal id: a
#: literal that happened to equal the generation's OWN device would make the UPDATE a no-op,
#: which ``IS DISTINCT FROM`` lets through, and the test would fail having proven nothing.
_REWRITES = {
    "device_id": ":other_device",
    "seq": "99",
    "mode": "'detach'",
    "document": "CAST('{\"vlan\": {}}' AS json)",
    "digest": "'0000000000000000000000000000000000000000000000000000000000000000'",
    "allowed_removal_keys": "CAST('{\"vlan\": [11]}' AS json)",
    "source_push_seq": "CAST('{\"vlan\": 42}' AS json)",
    "stream_revisions": "CAST('{\"vlan\": 7}' AS json)",
    "removal_context": 'CAST(\'{"scope": "vlan"}\' AS json)',
    "settlement_cohort": "42",
    "apply_attempt_id": "CAST('00000000-0000-4000-8000-000000000001' AS uuid)",
    "created_at": "now() - interval '1 day'",
}


def _seed(session) -> tuple[int, int, int]:
    """Seed two devices (so a device_id rewrite is not merely an FK failure) and one generation.

    Returns the generation's own device id, the second device's id and the generation id.
    Seeded through the ORM; only the UPDATEs under test are raw SQL, which is where the
    guarantee lives.
    """
    from nso_adapter.store.models import DeploymentGeneration, Device, GenerationMode, GenerationStatus

    devices = [
        Device(nso_instance="nso-dev", nso_device_name=name, netbox_device_id=nb)
        for name, nb in (("immutable-a", 8801), ("immutable-b", 8802))
    ]
    session.add_all(devices)
    session.flush()
    generation = DeploymentGeneration(
        device_id=devices[0].id,
        seq=1,
        mode=GenerationMode.networked,
        status=GenerationStatus.pending,
        document=_DOCUMENT,
        digest="a" * 64,
        allowed_removal_keys={},
        source_push_seq={},
        stream_revisions={"vlan": 1},
    )
    session.add(generation)
    session.commit()
    return devices[0].id, devices[1].id, generation.id


def test_the_immutable_column_set_is_exactly_what_the_trigger_guards():
    """The identity columns, restated ONCE. A new one must be a deliberate addition."""
    assert set(GENERATION_IMMUTABLE_COLUMNS) == set(_REWRITES), (
        "the trigger's guarded column set and this test's rewrite table disagree"
    )


def test_an_unknown_immutable_column_uses_the_json_safe_comparison():
    """A newly guarded JSON column must not generate PostgreSQL's invalid json equality."""
    assert _compare("future_document") == ("NEW.future_document::text IS DISTINCT FROM OLD.future_document::text")


@pytest.mark.parametrize("column", sorted(_REWRITES))
def test_rewriting_an_identity_column_is_rejected_by_the_database(pg_sync_session, column):
    _device_id, other_device_id, generation_id = _seed(pg_sync_session)

    with pytest.raises(sa.exc.IntegrityError) as excinfo:
        pg_sync_session.execute(
            sa.text(f"UPDATE deployment_generation SET {column} = {_REWRITES[column]} WHERE id = :gid"),
            {"gid": generation_id, "other_device": other_device_id},
        )
    assert "is immutable" in str(excinfo.value)
    pg_sync_session.rollback()


def test_the_lifecycle_columns_stay_writable(pg_sync_session):
    """Status, job binding, attempts, error and the timestamps are what execution MOVES."""
    from nso_adapter.store.models import Job, JobStatus, JobType

    _device_id, _other_device_id, generation_id = _seed(pg_sync_session)
    job = Job(job_type=JobType.apply, device_id=_device_id, status=JobStatus.queued, coalescible=True)
    pg_sync_session.add(job)
    pg_sync_session.flush()

    pg_sync_session.execute(
        sa.text(
            "UPDATE deployment_generation SET status = 'failed', job_id = :job_id, attempts = attempts + 1, "
            'last_error = CAST(\'{"code": "nso_commit_failed"}\' AS json), updated_at = now(), '
            "settled_at = now() WHERE id = :gid"
        ),
        {"gid": generation_id, "job_id": job.id},
    )
    pg_sync_session.commit()

    row = pg_sync_session.execute(
        sa.text("SELECT status, job_id, attempts, settled_at FROM deployment_generation WHERE id = :gid"),
        {"gid": generation_id},
    ).one()
    assert row.status == "failed"
    assert row.job_id == job.id
    assert row.attempts == 1
    assert row.settled_at is not None


def test_an_update_that_changes_nothing_is_allowed(pg_sync_session):
    """A no-op UPDATE touching an identity column must not trip the trigger.

    ``IS DISTINCT FROM``, not ``<>``: a NULL-to-NULL rewrite of ``removal_context`` is the
    common case (any status write on an apply generation) and would otherwise fail.
    """
    _device_id, _other_device_id, generation_id = _seed(pg_sync_session)

    pg_sync_session.execute(
        sa.text(
            "UPDATE deployment_generation SET removal_context = removal_context, "
            "document = document, status = 'running' WHERE id = :gid"
        ),
        {"gid": generation_id},
    )
    pg_sync_session.commit()

    assert (
        pg_sync_session.execute(
            sa.text("SELECT status FROM deployment_generation WHERE id = :gid"), {"gid": generation_id}
        ).scalar_one()
        == "running"
    )
