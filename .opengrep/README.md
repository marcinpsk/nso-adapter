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
fields. It covers `nso_adapter/main.py`, `core/importer.py`, `core/generation.py`,
both SSE subscriber modules, and the outcome bookkeeping logs in
`refresh_engine.py` and `redistribution.py`. These logs must use `failure_detail`
so an HTTP exception cannot repeat a request URL or server text. The behavioral
and AST regressions in `tests/core/test_importer_failure_sinks.py` remain
authoritative for the classification contract and complete Python syntax.

`nso-api-validation-error-raw-exception-renderer` rejects exception rendering
and formatted values in device validation responses. These responses use
authored text so caller-controlled NSO instance names do not return on the wire.

`nso-api-unknown-request-renderer` tracks the submitted removal scope into any
error built in the invalid-scope branch. It covers direct, formatted, converted,
and aliased values while allowing the later response to return a validated
scope. `nso-api-conflict-handler-contract` permits only the two complete generic
conflict handlers. Each uses its authored message and matching stable reason.

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
