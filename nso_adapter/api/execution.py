# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Public projections of frozen execution evidence."""

from typing import Literal

from pydantic import BaseModel, ConfigDict


class RowRefOut(BaseModel):
    model_config = ConfigDict(extra="forbid")
    subject: str
    key: str


class RelatedSiteOut(BaseModel):
    family: str
    row: RowRefOut | None
    field: str | None


class CatalogRefusalOut(BaseModel):
    family: str
    row: RowRefOut
    field: str | None
    reason: str
    message_id: str
    message: str
    related: RelatedSiteOut | None


class SettlementOwnerOut(BaseModel):
    stream: str
    row: RowRefOut
    source_push_seq: int | None
    frozen_revision: int | None
    accepted_at: str | None
    generation_id: int | None
    settlement_cohort: int | None


class LogicalIdentityOut(BaseModel):
    stream: str
    row: RowRefOut
    source_push_seq: int | None
    frozen_revision: int | None
    acceptance_identity: str | None
    generation_id: int | None
    settlement_cohort: int | None
    carrier: SettlementOwnerOut


class RowOutcomeOut(LogicalIdentityOut):
    disposition: Literal["in_sync", "refused", "not_applied", "unavailable"]
    refusals: list[CatalogRefusalOut]


class AttributedRefusalOut(CatalogRefusalOut):
    logical_rows: list[LogicalIdentityOut]
    provenance: Literal["current", "retained", "prior"]
    sender_phase: Literal["pre_commit_dry_run", "commit", "post_commit_verification"]


def evidence_projection(result: dict | None) -> dict:
    return {key: result.get(key) if result is not None else None for key in ("row_outcomes", "refusals")}
