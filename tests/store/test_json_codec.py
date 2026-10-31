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
@pytest.mark.parametrize(
    "sql_type,depth", [(kind, depth) for kind in ("json", "jsonb") for depth in (256, 257, 1200)] + [("json", 10000)]
)
async def test_postgresql_decoder_retains_deep_containers(store_engine, container, sql_type, depth):
    from nso_adapter.store import json_codec

    opening, closing = ("[", "]") if container == "array" else ('{"nested":', "}")
    payload = opening * depth + "0.1" + closing * depth
    async with session() as db:
        stored = await db.scalar(text(f"SELECT CAST(:payload AS {sql_type})"), {"payload": payload})
        raw = await db.scalar(text(f"SELECT CAST(CAST(:payload AS {sql_type}) AS text)"), {"payload": payload})
    if depth > 256:
        assert not isinstance(stored, (dict, list))
        assert isinstance(stored, json_codec.UnrepresentableJSONDocument)
        assert stored.text == raw
        return
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
async def test_store_decoder_matches_standard_json_values(payload):
    from nso_adapter.store.json_codec import load_store_json

    assert load_store_json(payload) == json.loads(payload)


@pytest.mark.parametrize("payload", ["", "[", "{", "[1,]", '{"x":1,}', "[1 2]", '{"x" 1}', "{1:2}", "[]{}", "[}"])
async def test_store_decoder_rejects_invalid_json(payload):
    from nso_adapter.store.json_codec import load_store_json

    with pytest.raises(json.JSONDecodeError):
        load_store_json(payload)


@pytest.mark.parametrize("container", ["array", "object"])
async def test_deep_canonical_digest_matches_exact_json_encoding(container):
    import hashlib

    from nso_adapter.api.execution import canonical_evidence, evidence_digest

    depth = 256
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


@pytest.mark.parametrize("marker", ["number", "document"])
async def test_stored_marker_rejects_json_and_pydantic_serialization(marker):
    from pydantic import TypeAdapter
    from pydantic_core import PydanticSerializationError

    from nso_adapter.store import json_codec

    value = {
        "nested": [
            json_codec.UnrepresentableJSONNumber("1e-400")
            if marker == "number"
            else json_codec.load_store_json("[" * 257 + "0" + "]" * 257)
        ]
    }
    with pytest.raises(TypeError):
        json.dumps(value)
    with pytest.raises(PydanticSerializationError):
        TypeAdapter(dict).dump_json(value)


@pytest.mark.parametrize("tag", ["$json_number", "$json_document", "$datetime", "$float"])
async def test_canonical_tags_cannot_be_imitated_by_stored_objects(tag):
    from datetime import datetime

    from nso_adapter.api.execution import canonical_evidence, evidence_digest
    from nso_adapter.store.json_codec import UnrepresentableJSONNumber, load_store_json

    synthetic = {
        "$json_number": UnrepresentableJSONNumber("1e-400"),
        "$json_document": load_store_json("[" * 257 + "0" + "]" * 257),
        "$datetime": datetime.fromisoformat("2026-10-01T00:00:00"),  # noqa: DTZ007
        "$float": float("inf"),
    }[tag]
    stored = canonical_evidence(synthetic)
    assert evidence_digest(synthetic) != evidence_digest(stored)
    assert canonical_evidence({"$": {"$$": [stored]}}) == {"$$": {"$$$": [{"$" + tag: stored[tag]}]}}


@pytest.mark.parametrize("leaf", ["0.1", "[]", "{}"])
@pytest.mark.parametrize("depth", [256, 257, 1200])
async def test_decoder_bounds_container_depth_and_keeps_document_identity(leaf, depth):
    from nso_adapter.api.execution import canonical_evidence, evidence_digest
    from nso_adapter.store import json_codec

    containers = depth - (leaf in {"[]", "{}"})
    payload = "[" * containers + leaf + "]" * containers
    value = json_codec.load_store_json(payload)
    if depth == 256:
        assert isinstance(value, list)
        for _ in range(containers):
            value = value[0]
        assert value == json.loads(leaf)
    else:
        assert not isinstance(value, list)
        assert isinstance(value, json_codec.UnrepresentableJSONDocument)
        assert value.text == payload
        assert canonical_evidence(value) == {"$json_document": payload}
        other = json_codec.load_store_json(payload.replace(leaf, "true" if leaf == "0.1" else "[1]"))
        assert evidence_digest(value) != evidence_digest(other)


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


async def test_evidence_digest_matches_the_documented_wire_format():
    import hashlib
    from datetime import datetime, timedelta, timezone

    from nso_adapter.api.execution import evidence_digest
    from nso_adapter.store.json_codec import UnrepresentableJSONNumber, load_store_json

    # docs/api-contract.md "Evidence envelopes" lists each rule this value exercises.
    value = {
        "z": [3, 1],
        "$tag": 100,
        "a": {"num": 1.25, "tenth": 0.1, "one": 1.0, "zero": 0, "neg": -0.5},
        "big": UnrepresentableJSONNumber("1E400"),
        "inf": float("-inf"),
        "aware": datetime(2026, 10, 1, 12, 0, tzinfo=timezone(timedelta(hours=2))),
        "naive": datetime(2026, 10, 1, 12, 0),  # noqa: DTZ001
        "stamp": "2026-10-01T12:00:00+02:00",
        "uuid": uuid.UUID(int=1),
        "deep": load_store_json("[" * 257 + "0" + "]" * 257),
        "text": "é\ud800",
        "flags": [True, None],
    }
    expected = (
        '{"$$tag":{"$json_number":"1e2"},'
        '"a":{"neg":{"$json_number":"-5e-1"},"num":{"$json_number":"125e-2"},"one":{"$json_number":"1e0"},'
        '"tenth":{"$json_number":"1e-1"},"zero":{"$json_number":"0"}},'
        '"aware":"2026-10-01T10:00:00Z",'
        '"big":{"$json_number":"1e400"},'
        '"deep":{"$json_document":"' + "[" * 257 + "0" + "]" * 257 + '"},'
        '"flags":[true,null],'
        '"inf":{"$float":"-inf"},'
        '"naive":{"$datetime":"2026-10-01T12:00:00"},'
        '"stamp":"2026-10-01T12:00:00+02:00",'
        '"text":"é\ud800",'
        '"uuid":"00000000-0000-0000-0000-000000000001",'
        '"z":[{"$json_number":"3e0"},{"$json_number":"1e0"}]}'
    )
    assert evidence_digest(value) == hashlib.sha256(expected.encode("utf-8", "surrogatepass")).hexdigest()
    assert evidence_digest({"x": 1}) == evidence_digest({"x": 1.0}) == evidence_digest({"x": load_store_json("1.00")})
