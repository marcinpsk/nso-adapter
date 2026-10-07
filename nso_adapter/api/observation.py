# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Typed observation responses for all published device projections."""

from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from nso_adapter.domain.observation import InterfaceAttributesDocument, InterfaceIpDocument, ObservationCoverage
from nso_adapter.domain.read_projection import VlanDocument
from nso_adapter.domain.redistribution_observation import RedistributionCoverage, RedistributionDocument
from nso_adapter.domain.routing_observation import BgpDocument, IsisDocument, OspfDocument, RoutePolicyDocument
from nso_adapter.domain.service_observation import (
    BfdDocument,
    L2ServiceDocument,
    LoggingDocument,
    SnmpDocument,
    StaticRouteDocument,
)
from nso_adapter.domain.switching_observation import (
    InterfaceMtuDocument,
    LagConfigDocument,
    LagTopologyDocument,
    SubinterfaceDocument,
    SviDocument,
    SwitchportDocument,
)


class ReadObservationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    revision: int
    source_epoch: int
    digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    observed_at: AwareDatetime
    coverage: ObservationCoverage


class InterfaceAttributesObservationOut(ReadObservationOut):
    family: Literal["interface_attributes"]
    document: InterfaceAttributesDocument


class InterfaceIpObservationOut(ReadObservationOut):
    family: Literal["interface_ip"]
    document: InterfaceIpDocument


class LagConfigObservationOut(ReadObservationOut):
    family: Literal["lag_config"]
    document: LagConfigDocument


class LagTopologyObservationOut(ReadObservationOut):
    family: Literal["lag"]
    document: LagTopologyDocument


class SwitchportObservationOut(ReadObservationOut):
    family: Literal["switchport"]
    document: SwitchportDocument


class InterfaceMtuObservationOut(ReadObservationOut):
    family: Literal["interface_mtu"]
    document: InterfaceMtuDocument


class SviObservationOut(ReadObservationOut):
    family: Literal["svi"]
    document: SviDocument


class SubinterfaceObservationOut(ReadObservationOut):
    family: Literal["subinterface"]
    document: SubinterfaceDocument


class BfdObservationOut(ReadObservationOut):
    family: Literal["bfd"]
    document: BfdDocument


class L2ServiceObservationOut(ReadObservationOut):
    family: Literal["l2_service"]
    document: L2ServiceDocument


class LoggingObservationOut(ReadObservationOut):
    family: Literal["logging"]
    document: LoggingDocument


class SnmpObservationOut(ReadObservationOut):
    family: Literal["snmp"]
    document: SnmpDocument


class StaticRouteObservationOut(ReadObservationOut):
    family: Literal["static_route"]
    document: StaticRouteDocument


class BgpObservationOut(ReadObservationOut):
    family: Literal["bgp"]
    document: BgpDocument


class IsisObservationOut(ReadObservationOut):
    family: Literal["isis"]
    document: IsisDocument


class OspfObservationOut(ReadObservationOut):
    family: Literal["ospf"]
    document: OspfDocument


class RoutePolicyObservationOut(ReadObservationOut):
    family: Literal["route_policy"]
    document: RoutePolicyDocument


class VlanObservationOut(ReadObservationOut):
    family: Literal["vlan"]
    document: VlanDocument


class RedistributionObservationOut(ReadObservationOut):
    family: Literal["redistribution"]
    document: RedistributionDocument
    coverage: RedistributionCoverage
