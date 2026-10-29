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


@pytest.mark.parametrize(
    "literal,kind",
    [
        ("1.25", float),
        ("1.0", float),
        ("0.0", float),
        ("0.1", None),
        ("1e-400", None),
        ("1.00000000000000001", None),
        ("7", int),
        ("1" + "0" * 4300, None),
        ("-" + "1" * 4301, None),
    ],
    ids=[
        "fraction",
        "integral_float",
        "zero",
        "rounded",
        "underflow",
        "near_integer",
        "integer",
        "large_integer",
        "negative_large_integer",
    ],
)
async def test_decoder_retains_only_exact_native_numbers(literal, kind):
    from decimal import Decimal

    from nso_adapter.store.json_codec import UnrepresentableJSONNumber, load_store_json

    value = load_store_json(literal)
    if kind is None:
        assert isinstance(value, UnrepresentableJSONNumber)
        assert value.literal == literal
    else:
        assert type(value) is kind
        assert Decimal(value) == Decimal(literal)


async def test_number_marker_rejects_json_and_pydantic_serialization():
    from pydantic import TypeAdapter
    from pydantic_core import PydanticSerializationError

    from nso_adapter.store.json_codec import UnrepresentableJSONNumber

    value = {"nested": [UnrepresentableJSONNumber("1e-400")]}
    with pytest.raises(TypeError):
        json.dumps(value)
    with pytest.raises(PydanticSerializationError):
        TypeAdapter(dict).dump_json(value)


@pytest.mark.parametrize("tag", ["$json_number", "$datetime", "$float"])
async def test_canonical_tags_cannot_be_imitated_by_stored_objects(tag):
    from datetime import datetime

    from nso_adapter.api.execution import canonical_evidence, evidence_digest
    from nso_adapter.store.json_codec import UnrepresentableJSONNumber

    synthetic = {
        "$json_number": UnrepresentableJSONNumber("1e-400"),
        "$datetime": datetime.fromisoformat("2026-10-01T00:00:00"),  # noqa: DTZ007
        "$float": float("inf"),
    }[tag]
    stored = canonical_evidence(synthetic)
    assert evidence_digest(synthetic) != evidence_digest(stored)
    assert canonical_evidence({"$": {"$$": [stored]}}) == {"$$": {"$$$": [{"$" + tag: stored[tag]}]}}


async def test_numeric_decode_and_canonicalization_ignore_decimal_context():
    from decimal import FloatOperation, localcontext

    from nso_adapter.api.execution import evidence_digest
    from nso_adapter.store.json_codec import load_store_json

    expected = evidence_digest(load_store_json("1.25"))
    with localcontext() as context:
        context.prec = 1
        context.traps[FloatOperation] = True
        assert evidence_digest(load_store_json("1.25")) == expected
        assert evidence_digest(load_store_json("1.00000000000000001")) != evidence_digest(
            load_store_json("1.00000000000000002")
        )
