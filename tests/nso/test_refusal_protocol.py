# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The installed canonical corpus and the closed refusal catalog."""

import hashlib
import inspect
import json
from dataclasses import asdict, replace
from pathlib import Path

import pytest

from nso_adapter.nso import refusal_protocol as protocol
from tests._secret_discipline import assert_text_free_of

_CORPUS_PATH = Path(protocol.__file__).with_name("refusal_protocol_corpus.json")


def test_refusal_protocol_contract():
    corpus = json.loads(_CORPUS_PATH.read_bytes())
    for case in corpus["row_refs"]:
        path = tuple((step, tuple(keys)) for step, keys in case["natural_path"])
        assert protocol.canonical_identity(case["family"], case["subject"], path) == case["canonical"], case["name"]
        assert hashlib.sha256(case["canonical"].encode("ascii")).hexdigest() == case["key"], case["name"]
        row = protocol.row_ref(case["family"], case["subject"], path)
        assert (row.subject, row.key) == (case["subject"], case["key"]), case["name"]

    for case in corpus["valid"]:
        records = protocol.parse_refusals(case["message"])
        assert [asdict(record) for record in records] == case["refusals"], case["name"]
        assert protocol.render_refusals(records) == case["message"], case["name"]
        assert protocol.claims_refusal(case["message"]), case["name"]

    for case in corpus["invalid"]:
        assert protocol.claims_refusal(case["message"]) is case["claims"], case["name"]
        with pytest.raises(protocol.RefusalProtocolError):
            protocol.parse_refusals(case["message"])


def test_nested_row_identity_includes_all_ancestors():
    subject = "bgp.router.scope.peer"
    paths = [
        (("router", ("65001",)), ("scope", ("",)), ("peer", ("192.0.2.1",))),
        (("router", ("65001",)), ("scope", ("placeholder-vrf",)), ("peer", ("192.0.2.1",))),
        (("router", ("65002",)), ("scope", ("",)), ("peer", ("192.0.2.1",))),
    ]
    assert len({protocol.row_ref("bgp", subject, path).key for path in paths}) == 3
    for path in (paths[0][2:], (paths[0][0], paths[0][2]), (paths[0][1], paths[0][0], paths[0][2])):
        with pytest.raises(protocol.RefusalProtocolError):
            protocol.row_ref("bgp", subject, path)


def test_split_subjects_have_distinct_row_refs():
    interface = (("interface", ("placeholder-interface",)),)
    rows = (
        protocol.row_ref("interface", "interface.description", interface),
        protocol.row_ref("interface", "interface.enabled", interface),
        protocol.row_ref("interface", "interface.ipv4-address", (*interface, ("ipv4-address", ("192.0.2.1",)))),
    )
    assert len({row.key for row in rows}) == len({row.subject for row in rows}) == 3


def test_refusal_message_catalog_is_closed():
    invented = "placeholder-caller-supplied-message"
    assert_text_free_of(protocol.MESSAGES, [invented])
    assert "message" not in inspect.signature(protocol.refusal).parameters
    assert "reason" not in inspect.signature(protocol.refusal).parameters
    row = protocol.row_ref("static-route", "static-route", ())
    with pytest.raises(protocol.RefusalProtocolError):
        protocol.refusal("static-route", row, "static-route.no-such-message")
    built = protocol.refusal("static-route", row, "write-blocked")
    assert (built.reason, built.message) == (
        protocol.MESSAGES["write-blocked"].reason,
        protocol.MESSAGES["write-blocked"].text,
    )
    with pytest.raises(protocol.RefusalProtocolError):
        protocol.render_refusals((replace(built, message=invented),))
    with pytest.raises(protocol.RefusalProtocolError):
        protocol.parse_refusals(protocol.render_refusals((built,)).replace(built.message, invented))


def test_every_catalog_message_round_trips():
    for entry in protocol.MESSAGES.values():
        family = entry.family or "vlan"
        record = protocol.refusal(family, protocol.row_ref(family, family, ()), entry.id)
        assert (record.reason, record.message) == (entry.reason, entry.text)
        assert protocol.parse_refusals(protocol.render_refusals((record,))) == (record,)
