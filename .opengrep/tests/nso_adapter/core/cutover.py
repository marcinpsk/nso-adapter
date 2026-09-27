# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
from sqlalchemy import insert, literal, null, null as sql_null, select, update
from nso_adapter.store.models import DeviceProjectionStream


async def deauthorize_for_cutover(db, row):
    # ok: nso-authority-reset-value
    db.execute(update(DeviceProjectionStream).values(authorized_document=sql_null(), authorized_revision=0))
    # ok: nso-authority-reset-value
    db.execute(update(DeviceProjectionStream).values({DeviceProjectionStream.authorized_document: sql_null()}))

    def null():
        return {"_execution": {"context": {"source": "apply"}}}

    # ruleid: nso-authority-reset-rebound-null
    row.authorized_document = null()

    def sql_null():
        return 5
    # ruleid: nso-authority-reset-rebound-null
    row.authorized_document = sql_null()
    # ruleid: nso-authority-reset-rebound-null
    db.execute(update(DeviceProjectionStream).values(authorized_document=sql_null()))
    null = lambda: 5
    # ruleid: nso-authority-reset-rebound-null
    row.authorized_document = null()
    from helper import null as sql_null
    # ruleid: nso-authority-reset-value, nso-authority-reset-rebound-null
    row.authorized_document = sql_null()
    # ruleid: nso-authority-reset-value
    db.add(DeviceProjectionStream(authorized_document={"_execution": {"context": {"source": "apply"}}}))
    # ruleid: nso-authority-reset-value
    db.execute(update(DeviceProjectionStream).values(authorized_document=None))
    # ruleid: nso-authority-reset-value
    db.execute(update(DeviceProjectionStream).values(authorized_document=0))
    # ruleid: nso-authority-reset-value
    db.execute(update(DeviceProjectionStream).values({DeviceProjectionStream.authorized_document: 0}))
    # ruleid: nso-authority-reset-revision
    row.authorized_revision = temporary = 5
    # ruleid: nso-authority-reset-value
    row.authorized_document = temporary_document = {"_execution": {"context": {"source": "apply"}}}
    # ruleid: nso-authority-reset-revision
    db.execute(update(DeviceProjectionStream).values(authorized_revision=sql_null()))
    # ruleid: nso-authority-reset-revision
    db.execute(update(DeviceProjectionStream).values({DeviceProjectionStream.authorized_revision: 5}))
    # ruleid: nso-authority-reset-revision
    db.execute(update(DeviceProjectionStream), params=[{"authorized_revision": 5}])
    # ruleid: nso-authority-reset-revision
    fields = {"authorized_revision": 5}
    db.execute(update(DeviceProjectionStream).values(**fields))
    # ruleid: nso-authority-reset-revision
    db.execute(update(DeviceProjectionStream).ordered_values(("authorized_revision", 5)))
    # ruleid: nso-authority-reset-revision
    db.execute(update(DeviceProjectionStream).values(dict([("authorized_revision", 5)])))
    # ruleid: nso-authority-reset-revision
    db.execute(insert(DeviceProjectionStream).from_select(["device_id", "authorized_revision"], select(literal(1), literal(5))))


async def rogue(db):
    # ruleid: nso-authority-write-cutover
    db.execute(update(DeviceProjectionStream).values(authorized_revision=5))
    # ruleid: nso-authority-augmented-write-cutover
    db.authorized_revision += 1


async def deauthorize_for_cutover_chained(row):
    # ruleid: nso-authority-write-cutover
    row.authorized_revision = temporary = 5


async def deauthorize_for_cutover_shadow(row):
    def null():
        return 5
    # ruleid: nso-authority-write-cutover
    row.authorized_document = null()


class Rogue:
    async def deauthorize_for_cutover(self, row):
        # ruleid: nso-authority-write-cutover-shadow
        row.authorized_revision = 0


def outer(row):
    async def deauthorize_for_cutover():
        # ruleid: nso-authority-write-cutover-shadow
        row.applied_revision = 0
