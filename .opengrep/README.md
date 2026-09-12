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

`nso-outcome-raw-exception-renderer` rejects direct `str` and `repr` exception
rendering in the outcome bookkeeping logs in `refresh_engine.py` and
`redistribution.py`. These logs must use `failure_detail` so an HTTP exception
cannot repeat a request URL or server text. The behavioral and AST regressions
in `tests/core/test_importer_failure_sinks.py` remain authoritative for the
classification contract and compound expressions.

OpenGrep 1.30.0 partially parses the adapter's PEP 695 type aliases and generic
functions. It reports each skipped line during a scan and analyzes the rest of
those files. Keep this limitation visible until the installed parser supports
the syntax.

No finite ruleset catches every review finding. Add an annotated defect and a
valid nearby shape before adding a rule. First confirm that the defect is
missed. Then implement the rule and run both commands above. Do not add a broad
suppression to make the tree pass.
