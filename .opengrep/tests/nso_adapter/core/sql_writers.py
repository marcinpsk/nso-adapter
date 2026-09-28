# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
from sqlalchemy import text as sql


def rogue(db):
    # ruleid: nso-authority-raw-sql
    db.execute(sql('UPDATE "device_projection_stream" SET authorized_revision = 5'))
    # ruleid: nso-authority-raw-sql
    db.execute(sql("UPDATE public.device_projection_stream SET authorized_revision = 5"))
    # ruleid: nso-authority-raw-sql
    query = 'INSERT INTO "public"."device_projection_stream" ("authorized_document") VALUES (NULL)'
    # ruleid: nso-authority-raw-sql
    db.execute(sql(query))


def more_sql(db):
    # ruleid: nso-authority-raw-sql
    db.execute(sql("UPDATE device_projection_stream AS s SET authorized_revision = 5"))
    # ruleid: nso-authority-raw-sql
    db.execute(sql("UPDATE device_projection_stream s SET authorized_revision = 5"))
    # ruleid: nso-authority-raw-sql
    db.execute(sql("UPDATE ONLY device_projection_stream SET authorized_revision = 5"))
    # ruleid: nso-authority-raw-sql
    db.execute(sql("-- comment\nUPDATE device_projection_stream SET authorized_revision = 5"))
    # ruleid: nso-authority-raw-sql
    db.execute(sql("WITH q AS (SELECT 1) UPDATE device_projection_stream SET authorized_revision = 5"))
    # ruleid: nso-authority-raw-sql
    db.execute(sql("UPDATE device_projection_stream SET (authorized_revision, applied_revision) = (5, 5)"))
    # ruleid: nso-authority-raw-sql
    db.execute(sql("UPDATE device_projection_stream SET desired_revision = authorized_revision WHERE authorized_revision = 5"))
    # ruleid: nso-authority-raw-sql
    db.execute(sql("INSERT INTO device_projection_stream (device_id, stream) " "VALUES (1, :stream) ON CONFLICT (device_id, stream) DO UPDATE SET authorized_revision = 5"))
    # ruleid: nso-authority-raw-sql
    db.execute(sql("UPDATE device_projection_stream SET desired_revision = 5, (authorized_revision, applied_revision) = (5, 5)"))
    # ruleid: nso-authority-raw-sql
    db.execute(sql('UPDATE device_projection_stream AS "s" SET authorized_revision = 5'))
    # ruleid: nso-authority-raw-sql
    db.execute(sql("UPDATE device_projection_stream SET /* adopt */ authorized_revision = 5"))
    # ruleid: nso-authority-raw-sql
    db.execute(sql("UPDATE device_projection_stream SET prepared_tables = jsonb_build_object(:key, authorized_revision = 5)"))
    # ruleid: nso-authority-raw-sql
    db.execute(sql("UPDATE device_projection_stream SET desired_revision = 5 /* , authorized_revision = 5 */"))
    # ruleid: nso-authority-raw-sql
    db.execute(sql("/* authorized_revision */ UPDATE device_projection_stream SET desired_revision = 5"))
    # ok: nso-authority-raw-sql
    db.execute(sql("SELECT authorized_revision FROM device_projection_stream"))
    # ok: nso-authority-raw-sql
    description = "authorized_revision = 5"
    # ok: nso-authority-raw-sql
    description = "UPDATE device_projection_stream SET desired_revision = 5"; other = "authorized_revision = 5"
