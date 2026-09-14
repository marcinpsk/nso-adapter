# Zizmor dependency source

Status: ratified

## Problem

The adapter declares Zizmor in the uv development dependency group and also
pins the remote Zizmor pre-commit repository. Dependabot updates the uv
declaration and `uv.lock`, but it cannot update the independent pre-commit
revision. A test requires both declarations to have the same version, so a
normal Dependabot uv update fails by construction.

The repository needs one authoritative Zizmor dependency declaration. Local
pre-commit and CI must consume that declaration without a second manually
maintained version pin.

## Evidence

- PR 52 updates `pyproject.toml` and `uv.lock` from Zizmor 1.29.0 to 1.30.0.
- `.pre-commit-config.yaml` still pins `zizmor-pre-commit` at 1.29.0.
- `tests/test_lint_pins.py` compares those independent versions and fails PR 52.
- CI already installs the uv development group and invokes Zizmor with
  `uv run`.
- The NetBox NSO plugin uses a local pre-commit hook that invokes the uv-locked
  Zizmor dependency. Its CI invokes the same dependency.

## Constraints and acceptance criteria

- Dependabot's uv update must update the authoritative declaration and lockfile
  without requiring a synchronized edit to pre-commit configuration.
- Pre-commit and CI must run the Zizmor version resolved in `uv.lock`.
- A stale lockfile must fail instead of changing during a lint operation.
- The pre-commit hook must retain the upstream hook's GitHub configuration file
  scope.
- Tests may verify dependency wiring and scope. They must not compare or encode
  Zizmor release numbers.
- Dependabot configuration must not duplicate the Zizmor version.

## Candidate shapes

1. Own Zizmor in `pyproject.toml` and `uv.lock`. Use a local pre-commit hook and
   CI commands that execute it through uv.
2. Own Zizmor in the remote pre-commit repository revision. Remove the uv
   dependency and run the pre-commit environment in CI.
3. Generate one dependency declaration from the other and reject generated
   configuration drift.

## First draft

Use `pyproject.toml` as the editable dependency declaration and `uv.lock` as
its generated resolution. Replace the remote Zizmor pre-commit repository with
a local `language: system` hook. The hook must invoke `zizmor` through
`uv run --locked`, accept the filenames selected by pre-commit, and own a
stable repository file pattern for workflows, Dependabot configuration, and
action definitions.

Keep CI on the same dependency path. It must run Zizmor through
`uv run --locked` over the repository so Zizmor collects every supported
GitHub automation input. The preceding `uv sync --all-groups --locked` remains
the install and stale-lock boundary.

Replace the version parity test with a wiring test. The test must establish
that exactly one local Zizmor hook exists, that no remote Zizmor hook exists,
that the local hook and CI use locked uv execution, and that the local hook
selects the repository-owned GitHub configuration scope. It must not parse
`pyproject.toml`, `uv.lock`, or a Zizmor release number. `uv --locked` owns the
project-to-lock consistency check.

Candidate 2 puts dependency ownership in a pre-commit repository that the
configured Dependabot ecosystems cannot update. CI would also need to create
and invoke a pre-commit environment for one scanner, which makes the lint
boundary shallower and less direct.

Candidate 3 preserves two materialized declarations and adds a generator and
drift gate. That is more coordination than the repository needs because both
consumers can execute the uv dependency directly.

## Decision

Revision r2, ratified.

Both designers selected candidate 1. `pyproject.toml` owns the editable Zizmor
requirement. `uv.lock` is generated resolution state. Pre-commit and CI execute
that resolution with `uv run --locked`.

The blind draft also required preserving the upstream hook's YAML type filter,
serial execution, and `--no-progress` argument. Inspection of the pinned
upstream hook manifest confirmed those settings. The local hook therefore keeps
the following behavior:

- Select workflows, Dependabot configuration, and `action.yml` or
  `action.yaml` files.
- Accept only YAML files and pass their paths to Zizmor.
- Run serially with progress output disabled.
- Execute `zizmor` from the locked uv environment.

CI runs the same locked dependency over the repository root. It explicitly
selects Zizmor's `workflows`, `actions`, and `dependabot` collection kinds.
Zizmor then owns discovery within those semantic kinds, including future
action definitions. The existing locked sync remains the install boundary.

The replacement regression verifies this consumer wiring. It does not inspect
the declared or locked Zizmor version. Dependabot already demonstrated that the
configured uv ecosystem updates `pyproject.toml` and `uv.lock` together. No
Dependabot configuration change is needed.

This boundary has one dependency owner, two direct consumers, and no
synchronization procedure. A stale or missing resolution fails at uv's locked
execution boundary. Reintroducing a remote Zizmor hook, bypassing locked uv
execution, or narrowing the hook's owned file scope fails the wiring test.

The first ratification pass rejected an implicit repository-root collection.
Zizmor's default directory collection omits Dependabot configuration, while the
pre-commit hook selects it explicitly. Revision r2 closes that gap with
`--collect=workflows`, `--collect=actions`, and `--collect=dependabot`. The
pinned Zizmor CLI accepted that syntax and audited all current workflow and
Dependabot files. The wiring test checks these collection-kind tokens as a set
instead of copying one complete command string.

The adversarial ratifier accepted revision r2. No counterexample remains for
dependency ownership, Dependabot updates, stale-lock failure, local hook
fidelity, or CI coverage.
