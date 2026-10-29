# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Assertions for consumers of validated evidence envelopes."""


def valid_projection(envelope: dict) -> dict:
    assert envelope["evidence_error"] is None, envelope
    assert envelope["projection"] is not None, envelope
    assert len(envelope["digest"]) == 64
    return envelope["projection"]


def deployment_projections(body: dict) -> dict:
    return {
        **body,
        "head": valid_projection(body["head"]) if body["head"] is not None else None,
        "attempts": [
            {**valid_projection(attempt), "generations": [valid_projection(row) for row in attempt["generations"]]}
            for attempt in body["attempts"]
        ],
    }
