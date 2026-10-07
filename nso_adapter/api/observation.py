# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Typed observation responses for the two published device projections."""

from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from nso_adapter.domain.observation import InterfaceAttributesDocument, InterfaceIpDocument, ObservationCoverage


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
