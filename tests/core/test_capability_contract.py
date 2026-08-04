# SPDX-License-Identifier: Apache-2.0
"""Anti-drift contract for the adapter half of the capability matrix.

Two guarantees that keep the matrix from silently lying:

1. **Vocabulary closure** — every construct NAME that ``preflight`` can look up
   (`_SET_KEY_CONSTRUCTS` / `_MATCH_KEY_CONSTRUCTS` values) must be a name something
   can actually PRODUCE: the NSO probe (representable half) or the apply-failed hook
   (accepted half). A lookup name nothing produces would always read ``native`` →
   the matrix could never flag it.
2. **Record→flag round-trip** — a rejection recorded by the apply hook for a construct
   must actually surface in a later ``preflight`` for that construct (and a probed
   ``skipped`` community kind must surface for a member of that kind).

The probe's construct vocabulary lives in nso-packages
(``route_policy_reconciler.main._PROBE_RM_SET`` / ``_PROBE_RM_MATCH``); it is mirrored
here and pinned on the producer side by
``nso-packages/tests/route_policy_reconciler/test_capability_probe_contract.py``
(``test_probe_construct_vocabulary_is_pinned``). Keep the two in lock-step.
"""

from __future__ import annotations

import pytest

from nso_adapter.core import capability
from nso_adapter.core.capability import (
    _MATCH_KEY_CONSTRUCTS,
    _REJECTION_CONSTRUCTS,
    _SET_KEY_CONSTRUCTS,
)

# adapter_client inits the DB (runs app lifespan -> init_db -> create_all).
from tests.conftest import session

# Mirror of nso-packages route_policy_reconciler.main._PROBE_RM_SET / _PROBE_RM_MATCH
# labels (the representable-half construct names the probe can emit).
_PROBE_RM_SET_LABELS = frozenset(
    {
        "set community",
        "set extcommunity",
        "set comm-list delete",
        "set extcomm-list delete",
        "set metric-type",
        "set tag",
        "set level",
        "set large-community",
    }
)
_PROBE_RM_MATCH_LABELS = frozenset(
    {
        "match route-type",
        "match local-preference",
        "match length",
        "match large-community",
        "match metric",
    }
)

_NED = "cisco-ios-cli-6.114"
_SW = "15.2(4)E10"


def test_every_preflight_lookup_name_is_producible():
    """No preflight construct name is orphaned — each is emittable by the probe or the apply hook."""
    apply_names = {name for _scope, name, _prefix in _REJECTION_CONSTRUCTS}
    producible = _PROBE_RM_SET_LABELS | _PROBE_RM_MATCH_LABELS | apply_names

    lookup_names = set()
    for names in _SET_KEY_CONSTRUCTS.values():
        lookup_names.update(names)
    for names in _MATCH_KEY_CONSTRUCTS.values():
        lookup_names.update(names)

    orphans = lookup_names - producible
    assert not orphans, f"preflight looks up names nothing produces (matrix can't flag them): {orphans}"


@pytest.mark.asyncio
async def test_recorded_rejection_surfaces_in_preflight(adapter_client):  # noqa: F811
    """Every set-/match-key, recorded as a device rejection, is flagged by a later preflight.

    This is the accepted-half round-trip: the apply hook records ``status='unsupported'``
    under a construct name; the operator's next attach must see it.
    """
    from nso_adapter.core.capability import get_device_capability, record_capability_rejection

    async with session() as db:
        for key, names in _SET_KEY_CONSTRUCTS.items():
            await record_capability_rejection(db, _NED, _SW, "rm-set", names[0], "% Invalid input")
            rows = await get_device_capability(db, _NED, _SW)
            res = capability.preflight(rows, set_keys=[key])
            assert not res["fully_supported"], f"set_key {key!r} (recorded {names[0]!r}) did not flag"
            assert all(u["element"] in names for u in res["unsupported"]), key

        for key, names in _MATCH_KEY_CONSTRUCTS.items():
            await record_capability_rejection(db, _NED, _SW, "rm-match", names[0], "% Invalid input")
            rows = await get_device_capability(db, _NED, _SW)
            res = capability.preflight(rows, match_keys=[key])
            assert not res["fully_supported"], f"match_key {key!r} (recorded {names[0]!r}) did not flag"
            assert all(u["element"] in names for u in res["unsupported"]), key


@pytest.mark.asyncio
async def test_probed_skipped_community_kind_surfaces_for_its_members(adapter_client):  # noqa: F811
    """A community kind probed as skipped flags any member of that kind (the panel/attach claim)."""
    from nso_adapter.core.capability import get_device_capability, record_probe_capability

    # representative member per kind → another member of the same kind used at preflight
    cases = {
        "color:0:128": "color:0:200",
        "large:1:2:3": "large:9:9:9",
        "bandwidth:1:100": "bandwidth:2:200",
    }
    async with session() as db:
        await record_probe_capability(
            db,
            _NED,
            _SW,
            [{"scope": "community", "name": probed, "status": "skipped", "detail": "no IOS home"} for probed in cases],
        )
        rows = await get_device_capability(db, _NED, _SW)
        for probed, attached in cases.items():
            res = capability.preflight(rows, community_members=[attached])
            assert not res["fully_supported"], f"{attached} (kind of {probed}) was not flagged"
            assert res["unsupported"][0]["element"] == attached
        # a standard member stays supported (no skipped 'standard' row)
        assert capability.preflight(rows, community_members=["65000:1"])["fully_supported"]
