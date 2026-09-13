# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com>
"""Positive and negative examples for the custom review checks."""


def raw_outcome_errors(logger, exc, failure_detail):
    # ruleid: nso-outcome-raw-exception-renderer
    logger.warning("family.outcome.read_record_failed", error=exc)
    # ruleid: nso-outcome-raw-exception-renderer
    logger.warning("family.outcome.read_record_failed", error=f"{exc}")
    # ruleid: nso-outcome-raw-exception-renderer
    logger.warning("family.outcome.read_record_failed", error=repr(exc))
    # ruleid: nso-outcome-raw-exception-renderer
    logger.warning("family.outcome.read_record_failed", error=str(exc))
    # ruleid: nso-outcome-raw-exception-renderer
    logger.warning("family.outcome.read_record_failed", error=str(exc) or repr(exc))
    # ruleid: nso-outcome-raw-exception-renderer
    logger.exception("family.outcome.read_record_failed")
    # ruleid: nso-outcome-raw-exception-renderer
    logger.exception("family.outcome.read_record_failed", error=failure_detail(exc))


def classified_outcome_errors(logger, exc, failure_detail):
    # ok: nso-outcome-raw-exception-renderer
    logger.warning("family.outcome.read_record_failed", error=failure_detail(exc))


def validation_error_messages(api_error, exc):
    # ruleid: nso-api-validation-error-raw-exception-renderer
    api_error(422, "validation_error", str(exc))
    # ruleid: nso-api-validation-error-raw-exception-renderer
    api_error(422, "validation_error", repr(exc))
    # ruleid: nso-api-validation-error-raw-exception-renderer
    api_error(422, "validation_error", f"Invalid request: {exc}")
    # ok: nso-api-validation-error-raw-exception-renderer
    api_error(422, "validation_error", "The request is invalid")


def returned_failure_details(exc):
    # ruleid: nso-failure-detail-raw-exception-renderer
    return repr(exc)


def returned_string_failure_details(exc):
    # ruleid: nso-failure-detail-raw-exception-renderer
    return str(exc)


def returned_formatted_failure_details(exc):
    # ruleid: nso-failure-detail-raw-exception-renderer
    return f"Failure: {exc}"


def returned_percent_failure_details(exc):
    # ruleid: nso-failure-detail-raw-exception-renderer
    return "Failure: %s" % exc


def returned_format_method_failure_details(exc):
    # ruleid: nso-failure-detail-raw-exception-renderer
    return "Failure: {}".format(exc)


def returned_builtin_format_failure_details(exc):
    # ruleid: nso-failure-detail-raw-exception-renderer
    return format(exc)


def returned_exception_property_details(exc):
    # ruleid: nso-failure-detail-raw-exception-renderer
    return repr(exc.args)


def returned_classification(exc):
    # ok: nso-failure-detail-raw-exception-renderer
    return type(exc).__name__
