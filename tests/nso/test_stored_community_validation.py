# SPDX-License-Identifier: Apache-2.0
"""Boundary validation against the real store."""

import pytest

from nso_adapter.store.models import RoutePolicyObjectIntent
from tests.conftest import seed_device, session


@pytest.mark.parametrize("reader", ["encode", "report"])
@pytest.mark.parametrize("entry", [{"sequence": 10}, "invalid", {"community": None}])
async def test_route_policy_readers_refuse_invalid_stored_members(adapter_client, reader, entry):
    from nso_adapter.core.community_dialect import community_dialect_for
    from nso_adapter.nso.apply import (
        NsoApplyError,
        SectionExecution,
        encode_route_policy,
        unrenderable_route_policy_members,
    )
    from tests._secret_discipline import assert_chain_free_of

    device_id = await seed_device(nso_device_name="invalid-community")
    name = "placeholder-community-list"
    async with session() as db:
        row = RoutePolicyObjectIntent(device_id=device_id, name=name, family="community_list", entries=[entry])
        db.add(row)
        await db.commit()
        await db.refresh(row)
        rows = {"route_policy_object_intent": [row]}
        execution = SectionExecution(None, community_dialect_for(None))
        with pytest.raises(NsoApplyError) as error:
            if reader == "encode":
                encode_route_policy(rows, execution)
            else:
                unrenderable_route_policy_members(rows, execution.dialect)
        assert error.value.code == "invalid_route_policy_intent"
        assert_chain_free_of(error.value, [name])
