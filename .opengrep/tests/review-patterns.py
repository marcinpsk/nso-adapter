# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Positive and negative examples for the custom review checks."""


def raw_outcome_errors(logger, exc):
    # ruleid: nso-outcome-raw-exception-renderer
    logger.warning("family.outcome.read_record_failed", error=repr(exc))
    # ruleid: nso-outcome-raw-exception-renderer
    logger.warning("family.outcome.read_record_failed", error=str(exc))
    # ruleid: nso-outcome-raw-exception-renderer
    logger.warning("family.outcome.read_record_failed", error=str(exc) or repr(exc))


def classified_outcome_errors(logger, exc, failure_detail):
    # ok: nso-outcome-raw-exception-renderer
    logger.warning("family.outcome.read_record_failed", error=failure_detail(exc))
