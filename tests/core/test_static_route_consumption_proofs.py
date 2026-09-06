# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Authority and liveness are made to coincide (#1683, memo A11).

No path may consume a carrier while the live service still holds an unsuperseded key that
carrier claims. Three halves of that rule land before C9's aggregate sender, because each is
strictly more conservative than what it replaces: the ONE certified section reader, the
rendered-versus-predecessor supersession split, and the service-clean conjunct on ordinary
networked settlement and on the reclaimer's delete-origin proof.
"""

from __future__ import annotations

import ast
import pathlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from nso_adapter.core.static_route_reader import certified_static_route_section
from tests.conftest import seed_device, session
from tests.core.removal_helpers import seed_removal_job, seed_tomb
from tests.core.test_static_route_put import A, B, C, seed_rows, wire
from tests.core.test_static_route_removal import SrFake, run_removal_job, sr_client, tombstone_ids

pytestmark = pytest.mark.anyio

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _reader(state) -> object:
    client = SimpleNamespace()
    client.service_instance_state = AsyncMock(return_value=state)
    return client


class _State:
    def __init__(self, status: str, entry):
        self.status, self.entry = status, entry

    @property
    def inconclusive(self) -> bool:
        return self.status == "inconclusive"


_DEVICE = SimpleNamespace(id=1, nso_device_name="reader-dev")


async def test_the_reader_normalizes_the_legacy_and_the_aggregate_shapes_alike():
    """The aggregate nests the same routes under ``static-route``; consumers see one shape."""
    legacy = await certified_static_route_section(
        _reader(_State("present", {"device": "reader-dev", "route": [wire(A)]})), _DEVICE
    )
    aggregate = await certified_static_route_section(
        _reader(_State("present", {"device": "reader-dev", "static-route": {"route": [wire(A)]}})), _DEVICE
    )
    assert legacy.status == aggregate.status == "present"
    assert legacy.entry == aggregate.entry == {"route": [wire(A)]}
    assert legacy.routes == aggregate.routes == [wire(A)]


async def test_an_empty_container_certifies_absence_and_an_uncertifiable_read_does_not():
    """``absent`` is conclusive; anything uncertifiable refuses every consumer."""
    empty_instance = await certified_static_route_section(
        _reader(_State("present", {"device": "reader-dev", "static-route": {}})), _DEVICE
    )
    no_instance = await certified_static_route_section(_reader(_State("absent", None)), _DEVICE)
    unreadable = await certified_static_route_section(_reader(_State("inconclusive", None)), _DEVICE)

    assert (empty_instance.status, empty_instance.routes) == ("absent", [])
    assert (no_instance.status, no_instance.routes) == ("absent", [])
    assert unreadable.inconclusive and unreadable.entry is None


def test_the_shared_reader_is_the_only_certified_static_route_reader():
    """One module knows the path and the nesting, so the cutover changes one module.

    A second caller of ``service_instance_state`` would keep expecting a top-level ``route``
    list and certify ABSENCE over an aggregate-owned key after the cutover.
    """
    callers = set()
    for path in (_REPO_ROOT / "nso_adapter").rglob("*.py"):
        if path.name in {"client.py", "static_route_reader.py"}:
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "service_instance_state"
            ):
                callers.add(str(path.relative_to(_REPO_ROOT)))
    assert callers == set(), f"certified static-route reads outside the shared reader: {sorted(callers)}"


async def test_a_carrier_claiming_only_a_replaced_predecessor_key_is_not_superseded(adapter_client):
    """A deployed-only claim is not supersession, so its cleanup is still owed.

    The row's identity moved from A to B, so B is RENDERED and A is only its predecessor. A
    carrier claiming A keeps a non-empty authority and must clean A from the service before
    anything may consume it.
    """
    from sqlalchemy import select as sa_select

    from nso_adapter.core.static_route_plan import classify_removal_plan
    from nso_adapter.store.models import StaticRouteIntent

    device_id = await seed_device(nso_device_name="sr-predecessor", netbox_device_id=17201)
    await seed_rows(device_id, [{"triple": B, "route_id": 2, "deployed_key": list(A)}])
    async with session() as db:
        rows = list(
            (await db.execute(sa_select(StaticRouteIntent).where(StaticRouteIntent.device_id == device_id)))
            .scalars()
            .all()
        )
        tomb = SimpleNamespace(id=1, vrf=A[0], prefix=A[1], next_hop=A[2], deployed_key=None)
        plan = classify_removal_plan(rows, [tomb], allowed_removal_keys={}, context={})

    assert A in plan.authorized, "a predecessor key was subtracted as if a row still rendered it"
    assert A not in plan.reclaimed
    assert A in plan.claimed, "the orphan report still knows the key is claimed"
    assert B not in plan.authorized


async def test_a_networked_removal_whose_service_still_holds_the_key_keeps_its_carrier(adapter_client):
    """Ordinary settlement gains detach's certified service-clean conjunct.

    Without it, a device-clean commit consumes the carrier while the service still owns the
    key, and nothing is left to own its cleanup.
    """
    from nso_adapter.store.models import JobStatus

    device_id = await seed_device(nso_device_name="sr-service-sticky", netbox_device_id=17202)
    await seed_rows(device_id, [{"triple": B, "route_id": 2}])
    tomb = await seed_tomb(device_id, A, route_id=1)
    job_id = await seed_removal_job(device_id, {}, tombs=(tomb,))

    fake = SrFake("sr-service-sticky", service=[wire(A), wire(B)])
    original = fake.state

    def _sticky_service():
        # The commit really does clean the device; the SERVICE keeps A, which is the state
        # the conjunct exists to notice.
        state = original()
        if state.entry is not None and A not in {
            (e.get("vrf") or "", e.get("prefix") or "", e.get("next-hop") or "") for e in state.entry["route"]
        }:
            state.entry["route"] = [*state.entry["route"], wire(A)]
        return state

    fake.state = _sticky_service
    job = await run_removal_job(device_id, job_id, sr_client(fake))

    assert job.status is JobStatus.failed
    assert job.error["code"] == "static_route_removal_unproven"
    assert job.result["service_clean"] is False
    assert await tombstone_ids(device_id) == [tomb], "the carrier was consumed while the service held its key"


async def test_the_reclaimer_reissues_rather_than_consuming_when_the_service_still_holds_the_key(adapter_client):
    """Device-absent plus service-present is ``cleanup_pending``: retained, reissued, logged."""
    from structlog.testing import capture_logs

    from tests.core.removal_helpers import authorize_static_route
    from tests.core.test_static_route_reclaim import owners, queued_removals, run_reclaim, seed_succeeded_owner

    device_id = await seed_device(nso_device_name="sr-cleanup-pending", netbox_device_id=17203)
    owner = await seed_succeeded_owner(device_id)
    tomb = await seed_tomb(device_id, A, job_id=owner, route_id=1)
    await authorize_static_route(device_id)

    # The device is certifiably clean of A; the service still owns it.
    fake = SrFake("sr-cleanup-pending", service=[wire(A)], device=[wire(B)])
    with capture_logs() as logs:
        assert await run_reclaim(sr_client(fake)) == (0, 1)

    assert await tombstone_ids(device_id) == [tomb]
    (reissued,) = await queued_removals(device_id)
    assert (await owners(device_id))[tomb] == reissued.id, "the reissue did not become the carrier's owner"
    assert [log for log in logs if log["event"] == "static_route_reclaim.cleanup_pending"]


async def test_a_reclaim_consumes_only_when_the_device_and_the_service_are_both_clean(adapter_client):
    """The positive control: both certified clean, so the carrier is consumed."""
    from tests.core.test_static_route_reclaim import run_reclaim, seed_succeeded_owner

    device_id = await seed_device(nso_device_name="sr-both-clean", netbox_device_id=17204)
    owner = await seed_succeeded_owner(device_id)
    await seed_tomb(device_id, A, job_id=owner, route_id=1)

    fake = SrFake("sr-both-clean", service=[wire(C)], device=[wire(C)])
    assert await run_reclaim(sr_client(fake)) == (1, 0)
    assert await tombstone_ids(device_id) == []
