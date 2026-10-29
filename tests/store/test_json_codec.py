# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Store JSON decoding must not permit a lossy read-modify-write."""

import json
import uuid

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.exc import StatementError

from nso_adapter.store.models import DeploymentApplyAttempt
from tests.conftest import seed_device, session

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize("sign", ["", "-"])
async def test_store_retains_unrepresentable_numbers_and_refuses_to_rewrite_them(store_engine, sign):
    device_id = await seed_device(nso_device_name="number-codec", netbox_device_id=18524)
    attempt_id = uuid.uuid4()
    number = sign + "1" + "0" * 400 + ".1"
    async with session() as db:
        db.add(
            DeploymentApplyAttempt(
                id=attempt_id,
                device_id=device_id,
                selected={},
                response={},
                admission_state="admitted",
                http_status=202,
            )
        )
        await db.commit()
        await db.execute(
            text("UPDATE deployment_apply_attempt SET response = CAST(:payload AS jsonb) WHERE id = :id"),
            {"payload": '{"finite": 1.25, "integer": 7, "nested": [' + number + "]}", "id": attempt_id},
        )
        await db.commit()
        stored = await db.scalar(select(DeploymentApplyAttempt.response).where(DeploymentApplyAttempt.id == attempt_id))
        assert type(stored["finite"]) is float
        assert stored["finite"] == 1.25
        assert type(stored["integer"]) is int
        assert stored["integer"] == 7
        with pytest.raises(TypeError):
            json.dumps(stored)
        assert stored["nested"][0].literal == number
        stored["extra"] = True
        with pytest.raises(StatementError) as error:
            await db.execute(
                update(DeploymentApplyAttempt).where(DeploymentApplyAttempt.id == attempt_id).values(response=stored)
            )
        assert isinstance(error.value.orig, TypeError)
        await db.rollback()
        persisted = await db.scalar(
            text("SELECT response::text FROM deployment_apply_attempt WHERE id = :id"), {"id": attempt_id}
        )
        assert number in persisted
        assert "extra" not in persisted
