# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Strict 802.1Q VLAN ID type for request models."""

from typing import Annotated

from pydantic import Field

from nso_adapter.nso.shape import VLAN_ID_MAX, VLAN_ID_MIN

VlanId = Annotated[int, Field(strict=True, ge=VLAN_ID_MIN, le=VLAN_ID_MAX)]
