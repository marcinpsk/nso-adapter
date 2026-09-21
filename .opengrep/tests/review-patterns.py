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
        # ruleid: nso-outcome-raw-exception-alias-renderer
        logger.warning(f"family outcome failed: {alias}")
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


async def validation_error_messages(api_error, exc, body):
    # ruleid: nso-api-validation-error-raw-exception-renderer
    api_error(422, "validation_error", str(exc))
    # ruleid: nso-api-validation-error-raw-exception-renderer
    api_error(422, "validation_error", repr(exc))
    # ruleid: nso-api-validation-error-raw-exception-renderer
    api_error(422, "validation_error", f"Invalid request: {exc}")
    # ruleid: nso-api-validation-error-raw-exception-renderer
    api_error(422, "validation_error", "Invalid request: %s" % exc)
    # ruleid: nso-api-validation-error-raw-exception-renderer
    api_error(422, "validation_error", "Invalid request: {}".format(exc))
    # ruleid: nso-api-validation-error-raw-exception-renderer
    api_error(422, "validation_error", format(exc))
    # ruleid: nso-api-validation-error-raw-exception-renderer
    api_error(422, "validation_error", "Invalid request: " + str(exc))
    request_alias = body.device_name
    # ruleid: nso-api-validation-error-raw-data-alias
    api_error(422, "validation_error", request_alias)
    # ruleid: nso-api-validation-error-raw-data-alias
    api_error(422, "validation_error", body.public_message)
    try:
        validate()
    except ValueError as caught:
        exception_alias = caught
        # ruleid: nso-api-validation-error-raw-data-alias
        api_error(422, "validation_error", exception_alias)
        # ok: nso-api-validation-error-raw-data-alias
        api_error(422, "validation_error", caught.public_message)
    # ok: nso-api-validation-error-raw-exception-renderer
    api_error(422, "validation_error", "The request is invalid")
    authored_message = "The requested NSO instance is not configured"
    # ok: nso-api-validation-error-raw-exception-renderer
    api_error(422, "validation_error", authored_message)


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
        # ok: nso-api-conflict-handler-contract
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


def raw_diagnostic_identifiers(logger, device, device_name, nso_instance, stream_url):
    # ruleid: nso-diagnostic-raw-identifier, nso-diagnostic-raw-identifier-alias
    logger.info("family.refresh.done", device_name=device.nso_device_name)
    # ruleid: nso-diagnostic-raw-identifier
    logger.info("family.refresh.done", device_name=device_name)
    # ruleid: nso-diagnostic-raw-identifier
    logger.warning("family.refresh.failed", device=device_name)
    # The NSO instance name is operator-authored configuration, not caller text: every
    # endpoint that takes one refuses a name absent from the configured set. Both spellings.
    # ok: nso-diagnostic-raw-identifier
    logger.warning("family.refresh.failed", nso_instance=nso_instance)
    # ok: nso-diagnostic-raw-identifier
    logger.info("nso.client.registered", instance=nso_instance)
    # ruleid: nso-diagnostic-raw-identifier, nso-diagnostic-raw-identifier-alias
    logger.info("sse_event", stream=stream_url)
    # ruleid: nso-diagnostic-raw-identifier, nso-diagnostic-raw-identifier-alias
    logger.info("sse.reconnect_after_error", stream_url=stream_url)
    # ruleid: nso-diagnostic-raw-identifier, nso-diagnostic-raw-identifier-alias
    logger.info("sse.stream.started", url=stream_url)
    # ok: nso-diagnostic-raw-identifier
    logger.info("family.refresh.done", device_id=device.id)
    # ok: nso-diagnostic-raw-identifier
    logger.info("sse_event", bytes=128)


def aliased_raw_diagnostic_identifiers(logger, device, body, stream_url):
    device_name = device.nso_device_name
    # ruleid: nso-diagnostic-raw-identifier-alias
    logger.info("family.refresh.done", context=device_name)
    ned_id = device.ned_id
    # ruleid: nso-diagnostic-raw-identifier-alias
    logger.info("family.refresh.done", context=ned_id)
    sw_version = device.sw_version
    # ruleid: nso-diagnostic-raw-identifier-alias
    logger.info("family.refresh.done", context=sw_version)
    request_name = body.nso_device_name
    # ruleid: nso-diagnostic-raw-identifier-alias
    logger.info("family.refresh.done", context=request_name)
    stream = stream_url
    # ruleid: nso-diagnostic-raw-identifier-alias
    logger.info("sse.stream.started", context=stream)
    # ok: nso-diagnostic-raw-identifier-alias
    logger.info("family.refresh.done", device_id=device.id)


async def legacy_query_api(session, model, select):
    # ruleid: nso-store-legacy-query-api
    session.query(model).all()
    # ok: nso-store-legacy-query-api
    return await session.scalars(select(model))


async def raw_string_statement(conn, op, query, table, value, text):
    # ruleid: nso-store-execute-raw-string
    await conn.execute("SELECT 1")
    # ruleid: nso-store-execute-raw-string
    await conn.execute("SELECT * FROM " + table)
    # ruleid: nso-store-execute-raw-string
    await conn.execute("SELECT * FROM %s" % table)
    # ruleid: nso-store-execute-raw-string
    await conn.execute(query.format(value))
    # ok: nso-store-execute-raw-string
    await conn.execute(text("SELECT 1"))
    # ok: nso-store-execute-raw-string
    await conn.execute(text("SELECT * FROM " + table))
    # ok: nso-store-execute-raw-string
    await conn.exec_driver_sql("SELECT * FROM " + table)
    # ok: nso-store-execute-raw-string
    op.execute("ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'sync_now'")


def vault_payload(client, mount, path, _secret_data, _secret_version):
    secret = client.secrets.kv.v2.read_secret_version(mount_point=mount, path=path)
    # ruleid: nso-vault-payload-unvalidated
    fields = secret["data"]["data"]
    # ruleid: nso-vault-payload-unvalidated
    version = secret["data"].get("metadata", {}).get("version")
    # ruleid: nso-vault-payload-unvalidated
    fields = secret["data"].get("data", {})
    # ruleid: nso-vault-payload-unvalidated
    metadata = secret["data"]["metadata"]
    # ok: nso-vault-payload-unvalidated
    fields = _secret_data(secret)
    # ok: nso-vault-payload-unvalidated
    version = _secret_version(secret)
    return fields, version, metadata


def wire_int_coercion(item, wire_int):
    # ruleid: nso-wire-int-coercion
    vid = int(item["vlan-id"])
    # ruleid: nso-wire-int-coercion
    mtu = int(item.get("mtu"))
    # ok: nso-wire-int-coercion
    untagged = wire_int(item.get("untagged-vlan"))
    return vid, mtu, untagged
