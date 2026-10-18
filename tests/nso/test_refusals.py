# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""The raw RESTCONF error classification boundary, before redaction."""

import json

import pytest

from nso_adapter.nso import refusal_protocol as protocol
from nso_adapter.nso.refusals import DefiniteRefusal, OrdinaryRejection, ProtocolFault, Unavailable, classify_error
from tests._secret_discipline import assert_chain_free_of, assert_text_free_of, exception_chain


def _message():
    record = protocol.refusal("vlan", protocol.row_ref("vlan", "vlan", ()), "write-blocked")
    return protocol.render_refusals((record,))


_REFUSAL_TAGS = {"error-type": "application", "error-tag": "operation-failed"}


def _envelope(message, root="ietf-restconf:errors", tags=_REFUSAL_TAGS):
    return json.dumps({root: {"error": [{**tags, "error-message": message}]}})


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (500, _envelope(_message()), "DefiniteRefusal"),
        (500, _envelope(_message(), "errors"), "DefiniteRefusal"),
        (500, json.dumps({"error": [{**_REFUSAL_TAGS, "error-message": _message()}]}), "DefiniteRefusal"),
        (400, _envelope(_message()), "ProtocolFault"),
        (503, _envelope(_message()), "ProtocolFault"),
        (200, _envelope(_message()), "ProtocolFault"),
        (500, _envelope("ordinary rejection"), "Unavailable"),
        (502, _envelope("ordinary rejection"), "Unavailable"),
        (503, "opaque response", "Unavailable"),
        (400, _envelope("ordinary rejection"), "OrdinaryRejection"),
        (404, "opaque response", "OrdinaryRejection"),
        (None, None, "Unavailable"),
        (101, "", "Unavailable"),
        (302, "opaque response", "Unavailable"),
        (307, _envelope(_message()), "ProtocolFault"),
    ],
)
def test_classify_error_table(status, body, expected):
    result = classify_error(status, body)
    assert type(result).__name__ == expected
    if expected == "DefiniteRefusal":
        assert result.records == protocol.parse_refusals(_message())


@pytest.mark.parametrize(
    "tags",
    [
        {"error-type": "application", "error-tag": "rollback-failed"},
        {"error-type": "application", "error-tag": "partial-operation"},
        {"error-type": "protocol", "error-tag": "operation-failed"},
        {"error-tag": "operation-failed"},
        {"error-type": "application"},
        {},
    ],
)
def test_claimed_refusal_with_other_than_nso_refusal_tags_is_protocol_fault(tags):
    assert isinstance(classify_error(500, _envelope(_message(), tags=tags)), ProtocolFault)


@pytest.mark.parametrize("status", [400, 500, 502, 503])
@pytest.mark.parametrize("level", ["root", "errors", "error", "message", "info"])
@pytest.mark.parametrize("claimed_first", [True, False])
def test_duplicate_restconf_members_cannot_hide_refusal(status, level, claimed_first):
    claimed = json.dumps(_message()).replace("device-intent", r"\u0064evice-intent")
    ordinary = json.dumps("ordinary rejection")
    messages = [claimed, ordinary] if claimed_first else [ordinary, claimed]
    errors = ['{"error-message":' + message + "}" for message in messages]
    if level == "message":
        body = '{"error":[{"error-message":' + messages[0] + ',"error-message":' + messages[1] + "}]}"
    elif level == "error":
        body = '{"error":[' + errors[0] + '],"error":[' + errors[1] + "]}"
    elif level == "errors":
        body = '{"errors":{"error":[' + errors[0] + ']},"errors":{"error":[' + errors[1] + "]}}"
    elif level == "info":
        body = '{"error":[{"error-message":' + ordinary + ',"error-info":{"detail":' + messages[0]
        body += ',"detail":' + messages[1] + "}}]}"
    else:
        body = '{"ietf-restconf:errors":{"error":[' + errors[0]
        body += ']},"ietf-restconf:errors":{"error":[' + errors[1] + "]}}"
    assert isinstance(classify_error(status, body), ProtocolFault)


@pytest.mark.parametrize("status", [400, 500, 502])
@pytest.mark.parametrize(
    "body",
    [
        r'{"\u0064evice-intent: refused": "ordinary rejection"}',
        r'{"errors":{"error":[{"error-message":"\u0064evice-intent: refused invalid"}]}}',
        r'{"errors":{"error":[{"error-message":"ordinary","error-info":{"detail":"\u0064evice-intent: refused"}}]}}',
        r'["ordinary",{"detail":["\u0064evice-intent: refused"]}]',
    ],
)
def test_escaped_marker_claim_fails_closed(status, body):
    assert not protocol.claims_refusal(body)
    assert isinstance(classify_error(status, body), ProtocolFault)


@pytest.mark.parametrize("status", [400, 500, 502, 503])
def test_malformed_claimed_server_error_is_protocol_fault(status):
    assert isinstance(classify_error(status, '{"error-message":"device-intent: refused'), ProtocolFault)


def test_escaped_marker_in_truncated_500_is_protocol_fault():
    body = r'{"error-message":"\u0064evice-intent: refused'
    assert not protocol.claims_refusal(body)
    assert isinstance(classify_error(500, body), ProtocolFault)


@pytest.mark.parametrize("body", ["{", "opaque response", "", b"\xff", b'{"message":"\xff"}'])
def test_malformed_unclaimed_500_is_protocol_fault(body):
    assert isinstance(classify_error(500, body), ProtocolFault)


@pytest.mark.parametrize("status", [502, 503])
@pytest.mark.parametrize("body", ["{", r'{"error-message":"\u0064evice-intent: refused', b"\xff"])
def test_malformed_unclaimed_server_error_is_unavailable(status, body):
    assert isinstance(classify_error(status, body), Unavailable)


@pytest.mark.parametrize("status", [502, 503])
@pytest.mark.parametrize("body", ["opaque response", "", '"ordinary rejection"', "null"])
def test_opaque_unclaimed_server_error_is_unavailable(status, body):
    assert isinstance(classify_error(status, body), Unavailable)


@pytest.mark.parametrize("as_bytes", [True, False])
def test_valid_refusal_with_restconf_metadata(as_bytes):
    body = json.dumps(
        {
            "ietf-restconf:errors": {
                "error": [
                    {
                        "error-type": "application",
                        "error-tag": "operation-failed",
                        "error-app-tag": "callback",
                        "error-path": "/device-intent",
                        "error-info": {"detail": "placeholder"},
                        "error-message": _message(),
                    }
                ]
            }
        }
    )
    result = classify_error(500, body.encode() if as_bytes else body)
    assert isinstance(result, DefiniteRefusal)
    assert result.records == protocol.parse_refusals(_message())


@pytest.mark.parametrize(
    "body",
    [
        _envelope(_message()).replace('"error": [', '"unexpected": ['),
        json.dumps({"errors": {"error": {"error-message": _message()}}}),
        json.dumps({"errors": {"error": [{"error-message": _message()}, {"error-message": "ordinary"}]}}),
        json.dumps({"errors": {"error": [{"error-message": _message()}, {"error-message": _message()}]}}),
        json.dumps({"errors": {"error": [{"error-message": _message(), "extra": "ordinary"}]}}),
        json.dumps({"errors": {"error": [{"error-message": _message(), "error-type": 1}]}}),
        json.dumps({"errors": {"error": [{"error-message": _message(), "error-info": "ordinary"}]}}),
        json.dumps({"errors": {"error": [{"error-message": _message(), "error-path": protocol.MARKER}]}}),
        json.dumps({"errors": {"error": [{"error-message": _message()}], "extra": "ordinary"}}),
        json.dumps({"errors": {"error": [{"error-message": _message()}]}, "extra": "ordinary"}),
        json.dumps({"errors": {"error": [{"error-message": _message()}]}, "error": []}),
        json.dumps({"error-message": _message()}),
        _envelope("NSO callback: " + _message()),
    ],
)
def test_invalid_or_mixed_claimed_envelope_is_protocol_fault(body):
    assert isinstance(classify_error(500, body), ProtocolFault)


@pytest.mark.parametrize("status", [400, 500, 502])
@pytest.mark.parametrize("as_bytes", [True, False])
@pytest.mark.parametrize("malformation", ["outer", "inner", "duplicate", "escaped"])
def test_malformed_claim_secret_never_reaches_result_or_exception_chain(status, as_bytes, malformation):
    secret = "placeholder-refusal-secret-canary"
    if malformation == "outer":
        body = '{"error-message":"device-intent: refused ' + secret
    elif malformation == "inner":
        body = _envelope('device-intent: refused {"' + secret + '":')
    elif malformation == "escaped":
        body = _envelope("device-intent: refused " + secret).replace("device-intent", r"\u0064evice-intent")
    else:
        body = '{"error":[{"error-message":' + json.dumps(_message()) + ',"error-message":' + json.dumps(secret) + "}]}"
    result = classify_error(status, body.encode() if as_bytes else body)
    assert_text_free_of((result, str(result), repr(result)), [secret])
    assert_chain_free_of(result, [secret])
    assert isinstance(result, ProtocolFault)
    assert str(result) == "NSO returned an invalid refusal protocol response"
    assert_text_free_of(vars(result), [secret])
    assert exception_chain(result) == [result]
    assert result.__cause__ is None and result.__context__ is None
    try:
        raise result
    except ProtocolFault as raised:
        captured = raised
    assert_chain_free_of(captured, [secret])
    assert exception_chain(captured) == [captured]


def test_a_transport_failure_cannot_carry_an_http_status():
    # An unread 4xx body cannot exclude a refusal claim, so a status never pairs with a transport failure.
    with pytest.raises(TypeError):
        classify_error(404, None, transport_failure=OSError("placeholder transport failure"))


def test_ordinary_rejection_has_no_server_text():
    result = classify_error(422, _envelope("placeholder-rejection-canary"))
    assert isinstance(result, OrdinaryRejection)
    assert_text_free_of(result, ["placeholder-rejection-canary"])
