<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright (C) 2026 Marcin Zieba <marcinpsk@gmail.com> -->

# Local review checks

Run the custom checks before a commit:

```sh
scripts/check-review-patterns test
scripts/check-review-patterns scan
```

Install OpenGrep from its official release, or set `OPENGREP_BIN` to an
installed executable. The rules and fixtures are tested with OpenGrep 1.30.0.
Install the repository hooks with `pre-commit install --install-hooks`.

The pre-commit hooks scan the adapter package when Python code, the rules, or
the runner changes. Rule changes also run the annotated fixtures. Missing
OpenGrep, invalid rules, and findings fail the hook. An explicit target is also
supported: `scripts/check-review-patterns scan path/to/file.py`. Arguments after
`scan` are paths, not scanner options. Ruff excludes only `.opengrep/tests`
because its deliberate defects are invalid production style.

## Preserve CodeRabbit's default scan

The rules live in `.opengrep/nso-rules.yaml`. Keep this custom filename. A
standard OpenGrep or Semgrep configuration filename can replace CodeRabbit's
fallback rule packs.

Run these custom rules only in pre-commit. Do not add them to GitHub Actions.
CodeRabbit skips OpenGrep when OpenGrep already runs in GitHub workflows. The
existing CI test and lint gates continue to run.

See the [CodeRabbit OpenGrep configuration](https://docs.coderabbit.ai/tools/opengrep)
for its configuration names and workflow skip conditions.

## Coverage

`nso-outcome-raw-exception-renderer` rejects traceback logging and raw exception
values in every positional or structured log field. It covers `nso_adapter/main.py`,
`core/importer.py`, `core/generation.py`, `core/removal.py`, `notifications/sse_subscriber.py`,
`notifications/persistent_subscriber.py`, and the outcome bookkeeping logs in
`refresh_engine.py` and `redistribution.py`. These logs must use `failure_detail`
so an HTTP exception cannot repeat a request URL or server text. The behavioral
and AST regressions in `tests/core/test_importer_failure_sinks.py` remain
authoritative for the classification contract and complete Python syntax.
The pre-commit AST guard scans guarded modules for exception aliases after
context-manager exits. Its control-flow model tracks conditional and later
assignments that OpenGrep cannot classify reliably.

`nso-diagnostic-raw-identifier` rejects `device_name` and `device` fields in the
guarded importer, redistribution, client, refresh, capability, startup, and
subscriber diagnostics. It also rejects raw SSE stream URL fields. It matches the
keyword name, so it sees only a value written directly into the field.

It does **not** reject the NSO instance name, under either spelling. The instance
name is operator-authored configuration, not caller text: every endpoint that takes
one refuses a name absent from the configured set, so a caller selects from that set
and cannot inject a value. The fixture pins both `nso_instance=` and `instance=` as
allowed, and the alias rule carries no instance source, so the two rules agree.

`nso-diagnostic-raw-identifier-alias` covers the flow the keyword rule cannot
see: a taint rule carrying `nso_device_name`, `ned_id`, `sw_version` and the SSE
stream URL through a local alias into any logger field, whatever that field is
named. Both rules guard the same module list.

These records use `device_id` when a stored device exists and omit the external
identifier otherwise.

`nso-raised-message-raw-identifier` covers the other publication path: a RAISED
message. Both identifier rules above sink on `logger.*`, so neither sees a name
interpolated into an exception, which `str(exc)`, `repr(exc)` and any formatted
traceback then publish with no handler able to take it back out. The rule carries the
same identifier vocabulary into the message expression of a `raise`, through
f-strings, `format`, `%` and concatenation, a local alias, or a bare first argument.
`RemovalBlockedError` stores its argument in `orphans` and sets a fixed message.

It sinks on the MESSAGE, not on the `raise`: a `detail=` field is a different
question, answered by whether its consumer renders it. It is the only guard here with
no module allowlist, because the class applies to every module and the rule is silent
over the whole package. `$D.id` and `$RESPONSE.status_code` are sanitized, the
adapter's own identifier and a closed integer; `$RESPONSE.text` is deliberately not,
because a device-named URL lets the server echo the submitted name straight back.

`nso-api-validation-error-raw-exception-renderer` rejects every supported raw
renderer in device validation responses. `nso-api-validation-error-raw-data-alias`
tracks request and exception values through aliases to the same response field.
These responses use authored text so caller-controlled values do not return on the wire.

`nso-api-unknown-request-renderer` tracks the submitted removal scope into
any `api_error` in `action_force_removal`. It covers direct, formatted, converted, and aliased
values. The rule makes sure that no endpoint error response returns the submitted scope.
`nso-api-conflict-handler-contract` permits only the two complete generic conflict
handlers. Each uses its authored message and matching stable reason.

`nso-failure-detail-raw-exception-renderer` rejects common direct string,
representation, interpolation, and formatting of the shared formatter
parameter. The AST regression in `test_importer_failure_sinks.py` is the
complete guard: it rejects aliases and permits the input only in type checks,
the exact enum-kind lookup, and numeric HTTP status access.

OpenGrep 1.30.0 partially parses the adapter's PEP 695 type aliases and generic
functions. It reports each skipped line during a scan and analyzes the rest of
those files. Keep this limitation visible until the installed parser supports
the syntax.

No finite ruleset catches every review finding. Add an annotated defect and a
valid nearby shape before adding a rule. First confirm that the defect is
missed. Then implement the rule and run both commands above. Do not add a broad
suppression to make the tree pass.
