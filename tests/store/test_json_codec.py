# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Store JSON decoding must not permit a lossy read-modify-write."""

import json
import struct
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
        ("0.1", float),
        ("1e-400", None),
        ("1e400", None),
        ("1.00000000000000001", None),
        ("7", int),
        ("1" + "0" * 4300, None),
        ("-" + "1" * 4301, None),
    ],
    ids=[
        "fraction",
        "integral_float",
        "zero",
        "decimal_fraction",
        "underflow",
        "overflow",
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
        assert Decimal(repr(value)) == Decimal(literal)


async def test_writer_float_literals_round_trip_through_postgresql(store_engine):
    values = [
        struct.unpack(">d", ((sign << 63) | (exponent << 52) | mantissa).to_bytes(8, "big"))[0]
        for sign in (0, 1)
        for exponent in range(2047)
        for mantissa in (0, 1, (1 << 51) - 1, (1 << 52) - 1)
    ]
    values.extend([0.1, 1.25])
    async with session() as db:
        stored = await db.scalar(text("SELECT CAST(:payload AS json)"), {"payload": json.dumps(values)})
    assert len(stored) == len(values)
    assert all(type(decoded) is float and decoded == original for original, decoded in zip(values, stored, strict=True))


@pytest.mark.parametrize("container", ["array", "object"])
@pytest.mark.parametrize("sql_type,depth", [("json", 1200), ("jsonb", 1200), ("json", 10000)])
async def test_postgresql_decoder_retains_deep_containers(store_engine, container, sql_type, depth):
    opening, closing = ("[", "]") if container == "array" else ('{"nested":', "}")
    payload = opening * depth + "0.1" + closing * depth
    async with session() as db:
        stored = await db.scalar(text(f"SELECT CAST(:payload AS {sql_type})"), {"payload": payload})
    for _ in range(depth):
        stored = stored[0] if container == "array" else stored["nested"]
    assert type(stored) is float
    assert stored == 0.1


@pytest.mark.parametrize(
    "payload",
    [
        ' {"empty": {}, "list": [], "values": [null, true, false, "[{}],:\\"", {"x": "\\u00e9"}]} \n',
        '{"duplicate": 1, "duplicate": 2}',
        "[1, -2, 0.1, 1.25, 1e3, -0.0]",
        '"plain string"',
        "null",
    ],
)
async def test_iterative_decoder_matches_standard_json_values(payload):
    from nso_adapter.store.json_codec import load_store_json

    assert load_store_json(payload) == json.loads(payload)


@pytest.mark.parametrize("payload", ["", "[", "{", "[1,]", '{"x":1,}', "[1 2]", '{"x" 1}', "{1:2}", "[]{}", "[}"])
async def test_iterative_decoder_rejects_invalid_json(payload):
    from nso_adapter.store.json_codec import load_store_json

    with pytest.raises(json.JSONDecodeError):
        load_store_json(payload)


@pytest.mark.parametrize("container", ["array", "object"])
async def test_deep_canonical_digest_matches_exact_json_encoding(container):
    import hashlib

    from nso_adapter.api.execution import canonical_evidence, evidence_digest

    depth = 10000
    value = 0.1
    for _ in range(depth):
        value = [value] if container == "array" else {"$nested": value}
    canonical = canonical_evidence(value)
    for _ in range(depth):
        canonical = canonical[0] if container == "array" else canonical["$$nested"]
    assert canonical == {"$json_number": "1e-1"}
    opening, closing = ("[", "]") if container == "array" else ('{"$$nested":', "}")
    expected = opening * depth + '{"$json_number":"1e-1"}' + closing * depth
    assert evidence_digest(value) == hashlib.sha256(expected.encode()).hexdigest()


@pytest.mark.parametrize("literal", ["0.1", "1.25", "1e-400", "1e400", "1.00000000000000001"])
async def test_native_and_marker_numbers_share_exact_decimal_identity(literal):
    from nso_adapter.api.execution import evidence_digest
    from nso_adapter.store.json_codec import UnrepresentableJSONNumber, load_store_json

    assert evidence_digest(load_store_json(literal)) == evidence_digest(UnrepresentableJSONNumber(literal))


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
