# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Bookkeeping snapshot records and cost against the migrated PostgreSQL store."""

from cProfile import Profile

import pytest

from nso_adapter.core.apply import encode_device_document
from nso_adapter.core.projection import _row_dict, intent_state
from nso_adapter.nso.origins import OriginContext, content_digest
from nso_adapter.nso.refusal_protocol import row_ref
from nso_adapter.store import execution_bookkeeping
from nso_adapter.store.execution_attempt_store import prepare_execution_attempt
from nso_adapter.store.models import (
    BgpPeerIntent,
    BgpRouterIntent,
    BgpScopeIntent,
    DeploymentGeneration,
    Device,
    DeviceClaim,
    DeviceProjectionStream,
    GenerationMode,
    Job,
    JobStatus,
    JobType,
    StaticRouteIntent,
)
from tests.store.test_execution_attempts import ACCEPTED, APPLIED

pytestmark = pytest.mark.anyio


async def _route_snapshot_inputs(db, count):
    device = Device(nso_instance="test", nso_device_name="placeholder-device")
    db.add(device)
    await db.flush()
    job = Job(device_id=device.id, job_type=JobType.apply, status=JobStatus.running, run_attempt=1, coalescible=False)
    rows = [
        StaticRouteIntent(
            device_id=device.id,
            route_id=41 + index,
            vrf="",
            prefix=f"198.18.{index // 256}.{index % 256}/32",
            next_hop="198.19.0.1",
            accepted_at=ACCEPTED,
            last_apply_at=APPLIED,
            last_apply_error={"code": "prior", "route": index},
        )
        for index in range(count)
    ]
    db.add_all([job, *rows])
    await db.flush()
    token = "placeholder-claim"
    db.add(DeviceClaim(device_id=device.id, job_id=job.id, claim_token=token, purpose="job"))
    db.add(
        DeviceProjectionStream(device_id=device.id, stream="static_route", desired_revision=7, authorized_revision=7)
    )
    generation = DeploymentGeneration(
        device_id=device.id,
        job_id=job.id,
        seq=1,
        mode=GenerationMode.networked,
        digest="a" * 64,
        stream_revisions={"static_route": 7},
        source_push_seq={"static_route": 41},
        document={
            "static_route": {
                "static_route_intent": [_row_dict(row) for row in reversed(rows)],
                "_execution": {"context": {"ned_id": None, "dialect": "identity"}},
            }
        },
    )
    db.add(generation)
    await db.flush()
    encoded = encode_device_document(generation.document, origin_context=OriginContext.from_generation(generation))
    logicals = tuple(
        dict.fromkeys(logical for binding in encoded.origins.bindings.values() for logical in binding.logical_rows)
    )
    await db.commit()
    return job, generation, token, encoded, logicals, rows


async def test_snapshot_preserves_route_records(db_session):
    job, generation, token, encoded, logicals, rows = await _route_snapshot_inputs(db_session, 4)
    attempt = await prepare_execution_attempt(
        db_session, job.id, 1, token, generation.id, origins=encoded.origins, logical_rows=logicals
    )
    expected = []
    for row in rows:
        reference = row_ref("static-route", "static-route.route", (("route", (row.vrf, row.prefix, row.next_hop)),))
        prior = {"last_apply_at": APPLIED.isoformat(), "last_apply_error": row.last_apply_error}
        expected.append(
            {
                "stream": "static_route",
                "frozen_revision": 7,
                "fence": {
                    "table": "static_route_intent",
                    "id": row.id,
                    "content_identity": content_digest(intent_state(row)),
                    "children": [],
                },
                "prior": prior,
                "expected": prior,
                "attribute": None,
                "contributors": [
                    {
                        "stream": "static_route",
                        "row": {"subject": "static-route.route", "key": reference.key},
                        "settlement_cohort": None,
                    }
                ],
            }
        )
    assert attempt.prior_bookkeeping == expected


async def test_snapshot_bounds_route_join_work(db_session):
    count = 200
    job, generation, token, encoded, logicals, rows = await _route_snapshot_inputs(db_session, count)
    with Profile(builtins=False) as profile:
        attempt = await prepare_execution_attempt(
            db_session, job.id, 1, token, generation.id, origins=encoded.origins, logical_rows=logicals
        )
    assert len(attempt.prior_bookkeeping) == len(rows)
    calls = sum(
        entry.callcount
        for entry in profile.getstats()
        if not isinstance(entry.code, str) and entry.code.co_filename == execution_bookkeeping.__file__
    )
    assert calls <= count * 100, f"Snapshot repeated {calls} bookkeeping calls for {count} routes"


async def test_snapshot_preserves_nested_carriers_and_contributor_order(db_session):
    device = Device(nso_instance="test", nso_device_name="placeholder-device")
    db_session.add(device)
    await db_session.flush()
    job = Job(device_id=device.id, job_type=JobType.apply, status=JobStatus.running, run_attempt=1, coalescible=False)
    routers = [
        BgpRouterIntent(device_id=device.id, asn=str(64512 + index), accepted_at=ACCEPTED, last_apply_at=APPLIED)
        for index in range(2)
    ]
    db_session.add_all([job, *routers])
    await db_session.flush()
    scopes = [BgpScopeIntent(router_id=router.id, vrf=vrf) for router in routers for vrf in ("scope-a", "scope-b")]
    db_session.add_all(scopes)
    await db_session.flush()
    peers = [
        BgpPeerIntent(scope_id=scope.id, peer_address=f"198.18.0.{index + 1}", remote_as="64514")
        for scope in scopes
        for index in range(2)
    ]
    db_session.add_all(peers)
    await db_session.flush()
    token = "placeholder-claim"
    db_session.add(DeviceClaim(device_id=device.id, job_id=job.id, claim_token=token, purpose="job"))
    db_session.add(DeviceProjectionStream(device_id=device.id, stream="bgp", desired_revision=7, authorized_revision=7))
    generation = DeploymentGeneration(
        device_id=device.id,
        job_id=job.id,
        seq=1,
        mode=GenerationMode.networked,
        digest="a" * 64,
        stream_revisions={"bgp": 7},
        source_push_seq={"bgp": 41},
        document={
            "bgp": {
                "bgp_router_intent": [_row_dict(row) for row in reversed(routers)],
                "bgp_scope_intent": [_row_dict(row) for row in reversed(scopes)],
                "bgp_peer_intent": [_row_dict(row) for row in reversed(peers)],
                "_execution": {"context": {"ned_id": None, "dialect": "identity"}},
            }
        },
    )
    db_session.add(generation)
    await db_session.flush()
    encoded = encode_device_document(generation.document, origin_context=OriginContext.from_generation(generation))
    logicals = tuple(
        dict.fromkeys(logical for binding in encoded.origins.bindings.values() for logical in binding.logical_rows)
    )
    logicals = tuple(reversed(logicals))
    await db_session.commit()
    attempt = await prepare_execution_attempt(
        db_session, job.id, 1, token, generation.id, origins=encoded.origins, logical_rows=logicals
    )

    def fence(row, children):
        return {
            "table": row.__tablename__,
            "id": row.id,
            "content_identity": content_digest(intent_state(row)),
            "children": children,
        }

    expected = []
    for router in routers:
        nested = []
        for scope in scopes:
            if scope.router_id != router.id:
                continue
            nested.append(
                fence(
                    scope,
                    [
                        {"table": "bgp_af_intent", "parent_column": "scope_id", "rows": []},
                        {
                            "table": "bgp_peer_intent",
                            "parent_column": "scope_id",
                            "rows": [
                                fence(peer, [{"table": "bgp_peer_af_intent", "parent_column": "peer_id", "rows": []}])
                                for peer in peers
                                if peer.scope_id == scope.id
                            ],
                        },
                    ],
                )
            )
        reference = row_ref("bgp", "bgp.router", (("router", (router.asn,)),))
        contributors = [
            {
                "stream": "bgp",
                "row": {"subject": logical.row.subject, "key": logical.row.key},
                "settlement_cohort": None,
            }
            for logical in logicals
            if logical.carrier.root_row == reference
        ]
        assert len(contributors) == 5
        prior = {"last_apply_at": APPLIED.isoformat(), "last_apply_error": None}
        expected.append(
            {
                "stream": "bgp",
                "frozen_revision": 7,
                "fence": fence(router, [{"table": "bgp_scope_intent", "parent_column": "router_id", "rows": nested}]),
                "prior": prior,
                "expected": prior,
                "attribute": None,
                "contributors": contributors,
            }
        )
    assert attempt.prior_bookkeeping == expected
