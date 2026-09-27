# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
from nso_adapter.store.models import DeploymentGeneration, DeviceProjectionStream


async def refresh_consumed_carriers(row):
    # ok: nso-authority-write-generation
    row.authorized_document = {}


def _settle_wire_equivalent(row):
    # ok: nso-authority-write-generation
    row.applied_revision = 5


async def create_generation(row):
    # ok: nso-authority-write-generation
    row.authorized_revision = 5


async def _stamp_applied_revisions(row):
    # ok: nso-authority-write-generation
    row.applied_revision = 5


async def _store_generation(db):
    # ok: nso-generation-construction-producer
    db.add(DeploymentGeneration(document={}))

    def nested():
        # ruleid: nso-generation-construction-shadow
        db.add(DeploymentGeneration(document={}))

    async def async_nested():
        # ruleid: nso-generation-construction-shadow
        db.add(DeploymentGeneration(document={}))


def rogue(db, row, rows):
    # ruleid: nso-authority-write-generation
    row.authorized_revision = 5
    # ruleid: nso-authority-augmented-write-generation
    rows[0].authorized_revision += 1
    # ruleid: nso-generation-construction-producer
    db.add(DeploymentGeneration(document={}))


class Rogue:
    def _store_generation(self, db):
        # ruleid: nso-generation-construction-shadow
        db.add(DeploymentGeneration(document={}))

    def advance(self, row):
        # ruleid: nso-authority-augmented-write-generation, nso-authority-augmented-write-generation-shadow
        row.authorized_revision += 1

    async def create_generation(self, row):
        # ruleid: nso-authority-write-generation-shadow
        row.authorized_revision = 5

    async def _stamp_applied_revisions(self, row):
        # ruleid: nso-authority-write-generation-shadow
        row.applied_revision = 5


def outer(row):
    async def create_generation():
        # ruleid: nso-authority-write-generation-shadow
        row.authorized_revision = 5

    def _store_generation(db):
        # ruleid: nso-generation-construction-shadow
        db.add(DeploymentGeneration(document={}))


async def async_outer():
    async def _store_generation(db):
        # ruleid: nso-generation-construction-shadow
        db.add(DeploymentGeneration(document={}))


class AsyncRogue:
    async def _store_generation(self, db):
        # ruleid: nso-generation-construction-shadow
        db.add(DeploymentGeneration(document={}))
