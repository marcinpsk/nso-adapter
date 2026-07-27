# SPDX-License-Identifier: Apache-2.0
"""Golden vectors for the fully-qualified Vault reference parser.

The plugin mirrors this parser (``netbox_nso_plugin/vault_refs.py``) — keep the
vectors below in sync with its test suite so both repos agree on the grammar.
"""

import pytest

from nso_adapter.secrets.refs import VaultRef, VaultRefError, parse_vault_ref

GOOD_VECTORS = [
    (
        "network/netbox/snmp/community/9f2a41c3d0be77aa#community",
        VaultRef("network", "netbox/snmp/community/9f2a41c3d0be77aa", "community"),
    ),
    ("network/netbox/snmp/v3/nms", VaultRef("network", "netbox/snmp/v3/nms", None)),
    ("network/netbox/snmp/v3/nms#auth", VaultRef("network", "netbox/snmp/v3/nms", "auth")),
    ("kv/p#k", VaultRef("kv", "p", "k")),
]

BAD_VECTORS = [
    "",  # empty
    "no-slash",  # mount cannot be determined
    "no-slash#key",  # mount cannot be determined
    "mount/",  # empty path
    "/path#key",  # empty mount
    "m//p#k",  # empty path segment
    "m/p/#k",  # trailing empty segment
    "m/p#",  # empty key
    "m/p#a#b",  # more than one '#'
    "m/p a#k",  # whitespace
    "m/p\t#k",  # whitespace
]


@pytest.mark.parametrize(("ref", "expected"), GOOD_VECTORS)
def test_parse_vault_ref_good_vectors(ref, expected):
    parsed = parse_vault_ref(ref)
    assert parsed == expected
    assert str(parsed) == ref  # round-trips verbatim


@pytest.mark.parametrize("ref", BAD_VECTORS)
def test_parse_vault_ref_bad_vectors(ref):
    with pytest.raises(VaultRefError):
        parse_vault_ref(ref)


def test_require_key_modes():
    with pytest.raises(VaultRefError):
        parse_vault_ref("network/netbox/snmp/v3/nms", require_key=True)
    with pytest.raises(VaultRefError):
        parse_vault_ref("network/netbox/snmp/v3/nms#auth", require_key=False)
    assert parse_vault_ref("network/p", require_key=False).key is None
    assert parse_vault_ref("network/p#k", require_key=True).key == "k"


def test_non_string_rejected():
    with pytest.raises(VaultRefError):
        parse_vault_ref(None)  # type: ignore[arg-type]
