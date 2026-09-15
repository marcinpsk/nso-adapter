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
    # ruleid: nso-outcome-raw-exception-renderer
    logger.warning("generation.interface_eligibility_unresolved", detail=str(exc))
    # ruleid: nso-outcome-raw-exception-renderer
    logger.warning("generation.interface_eligibility_unresolved", detail=exc)
    # ruleid: nso-outcome-raw-exception-renderer
    logger.warning("generation.interface_eligibility_unresolved", detail=f"{exc}")
    # ruleid: nso-outcome-raw-exception-renderer
    logger.warning("generation.interface_eligibility_unresolved", detail="{}".format(exc))
    # ruleid: nso-outcome-raw-exception-renderer
    logger.warning("generation.interface_eligibility_unresolved", detail="%s" % exc)
    # ruleid: nso-outcome-raw-exception-renderer
    logger.warning("generation.interface_eligibility_unresolved", detail="failure: " + str(exc))
    # ruleid: nso-outcome-raw-exception-renderer
    logger.warning("generation.interface_eligibility_unresolved", detail=format(exc))
    # ruleid: nso-outcome-raw-exception-renderer
    logger.warning("generation.interface_eligibility_unresolved", exc_info=True)


def classified_outcome_errors(logger, exc, failure_detail):
    # ok: nso-outcome-raw-exception-renderer
    logger.warning("family.outcome.read_record_failed", error=failure_detail(exc))
    # ok: nso-outcome-raw-exception-renderer
    logger.warning("family.outcome.read_record_failed", detail=failure_detail(exc))


def aliased_outcome_error(logger, failure_detail, http_status_of):
    try:
        work()
    except Exception as caught:
        alias = caught
        # ruleid: nso-outcome-raw-exception-alias-renderer
        logger.warning("family.outcome.read_record_failed", detail=alias)
        # ruleid: nso-outcome-raw-exception-alias-renderer
        logger.warning("family.outcome.read_record_failed", reason=alias)
        # ruleid: nso-outcome-raw-exception-alias-renderer
        logger.warning("family.outcome.read_record_failed", message=f"failure: {alias}")
        # ruleid: nso-outcome-raw-exception-alias-renderer
        logger.warning("family.outcome.read_record_failed", exc_info=alias)
        # ok: nso-outcome-raw-exception-alias-renderer
        logger.warning("family.outcome.read_record_failed", reason=failure_detail(caught))
        # ok: nso-outcome-raw-exception-alias-renderer
        logger.warning("family.outcome.read_record_failed", error_type=type(caught).__name__)
        # ok: nso-outcome-raw-exception-alias-renderer
        logger.warning("family.outcome.read_record_failed", http_status=http_status_of(caught))
        alias = "authored detail"
        # ok: nso-outcome-raw-exception-alias-renderer
        logger.warning("family.outcome.read_record_failed", detail=alias)
        # ok: nso-outcome-raw-exception-alias-renderer
        logger.warning("family.outcome.read_record_failed", detail=failure_detail(caught))


def authored_outcome_details(logger, reason):
    # ok: nso-outcome-raw-exception-renderer
    logger.warning("family.outcome.read_record_failed", detail=f"Reason: {reason}")
    # ok: nso-outcome-raw-exception-renderer
    logger.warning("family.outcome.read_record_failed", detail="Reason: {}".format(reason))
    # ok: nso-outcome-raw-exception-renderer
    logger.warning("family.outcome.read_record_failed", detail="Reason: %s" % reason)
    # ok: nso-outcome-raw-exception-renderer
    logger.warning("family.outcome.read_record_failed", detail="Reason: " + str(reason))
    # ok: nso-outcome-raw-exception-renderer
    logger.warning("family.outcome.read_record_failed", detail=format(reason))


def validation_error_messages(api_error, exc):
    # ruleid: nso-api-validation-error-raw-exception-renderer
    api_error(422, "validation_error", str(exc))
    # ruleid: nso-api-validation-error-raw-exception-renderer
    api_error(422, "validation_error", repr(exc))
    # ruleid: nso-api-validation-error-raw-exception-renderer
    api_error(422, "validation_error", f"Invalid request: {exc}")
    # ok: nso-api-validation-error-raw-exception-renderer
    api_error(422, "validation_error", "The request is invalid")


async def action_force_removal(device_id, body, db):
    outside_alias = body.scope
    if body.scope not in valid_removal_scopes():
        # ruleid: nso-api-unknown-request-renderer
        api_error(400, "bad_request", body.scope)
        # ruleid: nso-api-unknown-request-renderer
        api_error(400, "bad_request", f"Unknown removal scope {body.scope!r}")
        # ruleid: nso-api-unknown-request-renderer
        api_error(400, "bad_request", str(body.scope))
        # ruleid: nso-api-unknown-request-renderer
        api_error(400, "bad_request", repr(body.scope))
        # ruleid: nso-api-unknown-request-renderer
        api_error(400, "bad_request", "Unknown removal scope %s" % body.scope)
        # ruleid: nso-api-unknown-request-renderer
        api_error(400, "bad_request", "Unknown removal scope {}".format(body.scope))
        # ruleid: nso-api-unknown-request-renderer
        api_error(400, "bad_request", format(body.scope))
        # ruleid: nso-api-unknown-request-renderer
        api_error(400, "bad_request", outside_alias)
        inside_alias = body.scope
        # ruleid: nso-api-unknown-request-renderer
        api_error(400, "bad_request", inside_alias)
        # ok: nso-api-unknown-request-renderer
        api_error(400, "bad_request", "Unknown removal scope")

    try:
        work()
    except OperationSectionAbsent as absent:
        # ruleid: nso-api-unknown-request-renderer
        api_error(
            400,
            "bad_request",
            f"Nothing is authorized for {body.scope!r}",
            {"scope": body.scope, "reason": absent.reason},
        )


def safe_generic_conflicts(api_error):
    try:
        onboard()
    except DeviceIdentityRefused as exc:
        refused = api_error(409, "conflict", str(exc), {"reason": exc.reason})
    except LookupError:
        # ok: nso-api-conflict-handler-contract
        refused = api_error(
            409,
            "conflict",
            _NETBOX_DEVICE_CLAIMED_MESSAGE,
            {"reason": "netbox_device_claimed"},
        )
    except ValueError:
        refused = api_error(422, "validation_error", "Unknown instance")

    try:
        rekey()
    except LookupError:
        # ok: nso-api-conflict-handler-contract
        refused = api_error(
            409,
            "conflict",
            _DEVICE_IDENTITY_CLAIMED_MESSAGE,
            {"reason": "identity_claimed"},
        )


def safe_bound_generic_conflicts(api_error):
    try:
        onboard()
    except LookupError as exc:
        # ok: nso-api-conflict-handler-contract
        refused = api_error(
            409,
            "conflict",
            _NETBOX_DEVICE_CLAIMED_MESSAGE,
            {"reason": "netbox_device_claimed"},
        )

    try:
        rekey()
    except LookupError as exc:
        # ok: nso-api-conflict-handler-contract
        refused = api_error(
            409,
            "conflict",
            _DEVICE_IDENTITY_CLAIMED_MESSAGE,
            {"reason": "identity_claimed"},
        )


def bound_string_conflict(api_error):
    # ruleid: nso-api-conflict-handler-contract
    try:
        onboard()
    except LookupError as exc:
        refused = api_error(409, "conflict", str(exc))


def formatted_conflict(api_error):
    # ruleid: nso-api-conflict-handler-contract
    try:
        onboard()
    except LookupError as exc:
        refused = api_error(409, "conflict", f"Conflict: {exc}")


def percent_conflict(api_error):
    # ruleid: nso-api-conflict-handler-contract
    try:
        onboard()
    except LookupError as exc:
        refused = api_error(409, "conflict", "Conflict: %s" % exc)


def method_formatted_conflict(api_error):
    # ruleid: nso-api-conflict-handler-contract
    try:
        onboard()
    except LookupError as exc:
        refused = api_error(409, "conflict", "Conflict: {}".format(exc))


def direct_conflict(api_error):
    # ruleid: nso-api-conflict-handler-contract
    try:
        onboard()
    except LookupError as exc:
        refused = api_error(409, "conflict", exc)


def current_exception_conflict(api_error):
    # ruleid: nso-api-conflict-handler-contract
    try:
        onboard()
    except LookupError:
        refused = api_error(409, "conflict", sys.exception())


def current_exception_info_conflict(api_error):
    # ruleid: nso-api-conflict-handler-contract
    try:
        onboard()
    except LookupError:
        refused = api_error(409, "conflict", sys.exc_info()[1])


def conflict_without_detail(api_error):
    # ruleid: nso-api-conflict-handler-contract
    try:
        onboard()
    except LookupError:
        refused = api_error(409, "conflict", _NETBOX_DEVICE_CLAIMED_MESSAGE)


def conflict_with_empty_detail(api_error):
    # ruleid: nso-api-conflict-handler-contract
    try:
        onboard()
    except LookupError:
        refused = api_error(409, "conflict", _NETBOX_DEVICE_CLAIMED_MESSAGE, {})


def conflict_with_mismatched_reason(api_error):
    # ruleid: nso-api-conflict-handler-contract
    try:
        onboard()
    except LookupError:
        refused = api_error(
            409,
            "conflict",
            _NETBOX_DEVICE_CLAIMED_MESSAGE,
            {"reason": "identity_claimed"},
        )


def conflict_with_extra_statement(api_error):
    # ruleid: nso-api-conflict-handler-contract
    try:
        onboard()
    except LookupError:
        audit_conflict()
        refused = api_error(
            409,
            "conflict",
            _NETBOX_DEVICE_CLAIMED_MESSAGE,
            {"reason": "netbox_device_claimed"},
        )


def conflict_with_message_alias(api_error):
    # ruleid: nso-api-conflict-handler-contract
    try:
        onboard()
    except LookupError:
        message = _NETBOX_DEVICE_CLAIMED_MESSAGE
        refused = api_error(409, "conflict", message, {"reason": "netbox_device_claimed"})


def conflict_with_response_alias(api_error):
    # ruleid: nso-api-conflict-handler-contract
    try:
        onboard()
    except LookupError:
        error = api_error(
            409,
            "conflict",
            _NETBOX_DEVICE_CLAIMED_MESSAGE,
            {"reason": "netbox_device_claimed"},
        )
        refused = error


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
