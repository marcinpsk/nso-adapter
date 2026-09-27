# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
import builtins
from builtins import setattr as assign
from nso_adapter.store.models import DeploymentGeneration, DeploymentGeneration as DG
import nso_adapter.store.models as models
from sqlalchemy import insert, literal, select, update as upd
from sqlalchemy.dialects.postgresql import insert as pg_insert
from nso_adapter.store.models import DeviceProjectionStream as DPS


async def create_generation(row):
    # ruleid: nso-authority-write
    row.authorized_revision = 5


async def refresh_consumed_carriers(row):
    # ruleid: nso-authority-write
    row.authorized_revision = 5


def _settle_wire_equivalent(row):
    # ruleid: nso-authority-write
    row.authorized_revision = 5


async def _stamp_applied_revisions(row):
    # ruleid: nso-authority-write
    row.applied_revision = 5


async def deauthorize_for_cutover(row):
    # ruleid: nso-authority-write
    row.authorized_document = {"_execution": {"context": {"source": "apply"}}}


def _store_generation(db):
    # ruleid: nso-generation-construction
    db.add(DeploymentGeneration(document={}))


def rogue(db, row, rows):
    # ruleid: nso-generation-construction
    db.add(DG(document={}))
    # ruleid: nso-generation-construction
    db.add(models.DeploymentGeneration(document={}))
    # ruleid: nso-authority-write
    assign(row, "authorized_revision", 5)
    # ruleid: nso-authority-write
    builtins.setattr(row, "authorized_revision", 5)
    # ruleid: nso-authority-augmented-write
    rows[0].authorized_revision += 1
    # ruleid: nso-authority-write
    db.execute(upd(DPS).values({DPS.authorized_revision: 5}))
    # ruleid: nso-authority-write
    db.execute(upd(DPS), {"authorized_revision": 5})
    # ruleid: nso-authority-write
    db.execute(upd(DPS), [{"id": 1, "authorized_revision": 5}, {"id": 2, "authorized_revision": 6}])
    # ruleid: nso-authority-write
    db.execute(upd(DPS), params=[{"id": 1, "authorized_revision": 5}])
    # ruleid: nso-authority-write
    fields = {"authorized_revision": 5}
    db.execute(upd(DPS).values(**fields))
    db.execute(pg_insert(DPS).values(device_id=row.device_id, stream=row.stream).on_conflict_do_update(index_elements=[DPS.device_id, DPS.stream], set_=fields))
    # ruleid: nso-authority-write
    db.execute(upd(DPS).values([{"authorized_revision": 5}, {"authorized_revision": 6}]))


def more_writes(db, row):
    # ruleid: nso-authority-write
    row.authorized_revision = row.applied_revision = 5
    # ruleid: nso-authority-write
    row.authorized_revision = temporary = 5
    # ruleid: nso-authority-write
    db.add(DPS(authorized_revision=5))
    # ruleid: nso-authority-write
    fields = dict(authorized_revision=5)
    db.execute(upd(DPS).values(**fields))
    fields = {}
    # ruleid: nso-authority-write
    fields["authorized_revision"] = 5
    # ruleid: nso-authority-write
    fields.update(authorized_revision=5)
    # ruleid: nso-authority-write
    db.execute(pg_insert(DPS).values(device_id=row.device_id, stream=row.stream).on_conflict_do_update(index_elements=[DPS.device_id, DPS.stream], set_=dict(authorized_revision=5)))
    # ruleid: nso-authority-write
    db.execute(upd(DPS).ordered_values((DPS.authorized_revision, 5)))
    # ruleid: nso-authority-write
    db.execute(upd(DPS).values({DPS.__table__.c["authorized_revision"]: 5}))
    # ruleid: nso-authority-write
    db.execute(upd(DPS).values({DPS.__table__.c.applied_revision: 5}))
    # ruleid: nso-authority-write
    db.execute(upd(DPS).ordered_values(("authorized_revision", 5)))
    # ruleid: nso-authority-write
    db.execute(upd(DPS).values(dict([("authorized_revision", 5)])))
    # ruleid: nso-authority-write
    db.execute(insert(DPS).from_select(["device_id", "stream", "authorized_revision"], select(literal(1), literal("static_route"), literal(5))))
