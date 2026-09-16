# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2025 Marcin Zieba <marcinpsk@gmail.com>
"""Run the credential guard in the suite and test the scanner with real source."""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
import yaml

import tests.credential_discipline as credential_discipline
from tests.credential_discipline import (
    Violation,
    scan_source,
    scan_tree,
)


def test_repository_has_no_forbidden_credentials():
    bad = scan_tree()
    assert not bad, (
        "Unapproved credential literal(s):\n"
        + "\n".join(f"  {v}" for v in bad)
        + "\nUse neutral placeholders or an inline '# credential-ok: <reason>' comment."
    )


def test_baseline_update_argument_is_rejected_without_writing(monkeypatch, capsys, tmp_path):
    """Guard the path, not a guessed writer name: any writer at all would create this file."""
    baseline = tmp_path / "credential_discipline_baseline.txt"
    monkeypatch.setattr(credential_discipline, "_OBSOLETE_BASELINE_PATH", baseline)

    assert credential_discipline._main(["--update-baseline"]) == 2
    assert not baseline.exists()
    assert "usage:" in capsys.readouterr().err


def test_pre_commit_guard_selects_an_obsolete_baseline_file():
    config = yaml.safe_load((Path(__file__).parents[1] / ".pre-commit-config.yaml").read_text(encoding="utf-8"))
    hooks = [
        hook
        for repository in config["repos"]
        if repository["repo"] == "local"
        for hook in repository["hooks"]
        if hook["id"] == "credential-discipline"
    ]
    assert len(hooks) == 1
    files = re.compile(hooks[0]["files"])
    assert files.search("tests/test_example.py")
    assert files.search("tests/credential_discipline_baseline.txt")
    assert not (Path(__file__).parent / "credential_discipline_baseline.txt").exists()


@pytest.mark.parametrize(
    "statement",
    [
        'username = "admin"',
        'NSO_PASSWORD = "ADMIN"',
        'client.secret = "AdMiN"',
        'token: str = "admin"',
        'username, password = "admin", "placeholder"',
        'password += "admin"',
        'config["password"] = "admin"',
        'config = {"username": "admin"}',
        'client(username="admin")',
        'client(api_token="admin")',
        'NsoClient(config, "admin", "placeholder")',
        'subscriber(config, ("admin", "placeholder"))',
        'client(auth=("admin", "placeholder"))',
        'monkeypatch.setenv("NSO_USERNAME", "admin")',
        'def connect(password="admin"): pass',
        'def connect(*, token="admin"): pass',
    ],
)
def test_flags_new_credential_literals(statement):
    hits = scan_source(statement, "t.py")
    assert len(hits) == 1
    assert hits[0].path == "t.py"
    assert hits[0].lineno == 1


@pytest.mark.parametrize(
    "statement",
    [
        'client(*("admin", "placeholder"))',
        'username, password = "u", *("admin",)',
    ],
)
def test_flags_credential_literals_behind_a_STAR(statement):
    """``ast.Starred`` wraps the value, so an unwrapped scan walked straight past it."""
    assert len(scan_source(statement, "t.py")) == 1


@pytest.mark.parametrize(
    "statement",
    [
        'lambda password="admin": None',
        'lambda *, token="admin": None',
    ],
)
def test_flags_credential_literals_in_lambda_defaults(statement):
    assert len(scan_source(statement, "t.py")) == 1


@pytest.mark.parametrize(
    "statement",
    [
        'password = f"admin"',
        "password = f\"{'admin'}\"",
        "password = f\"{'admin'!s}\"",
        'password = "ad" + "min"',
        'username = "".join(("ad", "min"))',
        'username = "ADMIN".lower()',
    ],
)
def test_flags_constant_credential_string_expressions(statement):
    assert len(scan_source(statement, "t.py")) == 1


def test_flags_a_constant_alias_at_a_credential_sink():
    source = 'placeholder = "admin"\nusername = placeholder\n'

    hits = scan_source(source, "t.py")

    assert len(hits) == 1
    assert hits[0].lineno == 2


@pytest.mark.parametrize(
    "source",
    [
        'placeholder = "admin"\nplaceholder = supplied\nusername = placeholder\n',
        'placeholder = "admin"\nplaceholder = "placeholder-user"\nusername = placeholder\n',
        'placeholder = "admin"\nplaceholder += supplied\nusername = placeholder\n',
        'placeholder = "admin"\nplaceholder += "-suffix"\nusername = placeholder\n',
    ],
)
def test_reassigned_constant_alias_does_not_retain_its_old_value(source):
    assert scan_source(source, "t.py") == []


def test_conditional_reassignment_preserves_the_tainted_path():
    source = 'placeholder = "admin"\nif condition:\n    placeholder = supplied\nusername = placeholder\n'

    hits = scan_source(source, "t.py")

    assert len(hits) == 1
    assert hits[0].lineno == 4


@pytest.mark.parametrize(
    ("source", "expected_line"),
    [
        (
            'value = "admin"\nfor item in items:\n    value = item\nusername = value\n',
            4,
        ),
        (
            'async def check():\n    value = "admin"\n    async for item in items:\n        value = item\n    username = value\n',
            5,
        ),
        (
            'value = "admin"\nwhile condition:\n    value = supplied\nusername = value\n',
            4,
        ),
    ],
)
def test_loop_zero_iteration_paths_preserve_constant_aliases(source, expected_line):
    hits = scan_source(source, "t.py")

    assert len(hits) == 1
    assert hits[0].lineno == expected_line


@pytest.mark.parametrize(
    "source",
    [
        (
            'value = "admin"\nfor item in items:\n    value = "placeholder-user"\n'
            'else:\n    value = "other-user"\nusername = value\n'
        ),
        (
            'async def check():\n    value = "admin"\n    async for item in items:\n'
            '        value = "placeholder-user"\n    else:\n        value = "other-user"\n    username = value\n'
        ),
        (
            'value = "admin"\nwhile condition:\n    value = "placeholder-user"\n'
            'else:\n    value = "other-user"\nusername = value\n'
        ),
    ],
)
def test_loop_paths_that_all_clear_constant_aliases_are_accepted(source):
    assert scan_source(source, "t.py") == []


def test_later_loop_iterations_observe_constants_assigned_by_the_body():
    source = 'value = supplied\nfor item in items:\n    username = value\n    value = "admin"\n'

    hits = scan_source(source, "t.py")

    assert len(hits) == 1
    assert hits[0].lineno == 3


def test_each_loop_iteration_can_clear_an_incoming_constant_before_a_sink():
    source = 'value = "admin"\nfor item in items:\n    value = supplied\n    username = value\n'

    assert scan_source(source, "t.py") == []


@pytest.mark.parametrize("handler_keyword", ["except", "except*"])
def test_try_handler_paths_preserve_constant_aliases(handler_keyword):
    source = f'value = "admin"\ntry:\n    value = supplied\n{handler_keyword} Exception:\n    pass\nusername = value\n'

    hits = scan_source(source, "t.py")

    assert len(hits) == 1
    assert hits[0].lineno == 6


def test_try_else_paths_preserve_constant_aliases():
    source = (
        'value = supplied\ntry:\n    value = "admin"\n'
        "except Exception:\n    value = supplied\nelse:\n    pass\nusername = value\n"
    )

    hits = scan_source(source, "t.py")

    assert len(hits) == 1
    assert hits[0].lineno == 8


def test_try_paths_that_all_clear_constant_aliases_are_accepted():
    source = (
        'value = "admin"\ntry:\n    value = supplied\n'
        "except Exception:\n    value = supplied\nelse:\n    value = supplied\nusername = value\n"
    )

    assert scan_source(source, "t.py") == []


def test_try_finally_reassignment_clears_every_surviving_path():
    source = (
        'value = "admin"\ntry:\n    work()\nexcept Exception:\n    pass\n'
        "finally:\n    value = supplied\nusername = value\n"
    )

    assert scan_source(source, "t.py") == []


def test_match_case_paths_preserve_constant_aliases():
    source = (
        'value = "admin"\nmatch subject:\n    case 1:\n        value = supplied\n'
        "    case 2:\n        pass\nusername = value\n"
    )

    hits = scan_source(source, "t.py")

    assert len(hits) == 1
    assert hits[0].lineno == 7


def test_exhaustive_match_paths_that_all_clear_constant_aliases_are_accepted():
    source = (
        'value = "admin"\nmatch subject:\n    case 1:\n        value = supplied\n'
        '    case _:\n        value = "placeholder-user"\nusername = value\n'
    )

    assert scan_source(source, "t.py") == []


def test_match_capture_patterns_bind_constant_aliases():
    source = 'value = "admin"\nmatch value:\n    case captured:\n        username = captured\n'

    hits = scan_source(source, "t.py")

    assert len(hits) == 1
    assert hits[0].lineno == 4


def test_irrefutable_match_captures_can_clear_old_constant_aliases():
    source = 'captured = "admin"\nmatch supplied:\n    case captured:\n        pass\nusername = captured\n'

    assert scan_source(source, "t.py") == []


@pytest.mark.parametrize(
    "source",
    [
        (
            'value = supplied\ntry:\n    with context():\n        value = "admin"\n'
            "        work()\n        value = supplied\nexcept Exception:\n    pass\nusername = value\n"
        ),
        (
            "async def check():\n    value = supplied\n    try:\n        async with context():\n"
            '            value = "admin"\n            await work()\n            value = supplied\n'
            "    except Exception:\n        pass\n    username = value\n"
        ),
    ],
)
def test_with_body_failure_paths_preserve_constant_aliases(source):
    hits = scan_source(source, "t.py")

    assert len(hits) == 1


@pytest.mark.parametrize(
    "source",
    [
        'value = "admin"\nwith context() as value:\n    username = value\n',
        ('async def check():\n    value = "admin"\n    async with context() as value:\n        username = value\n'),
    ],
)
def test_with_targets_clear_old_constant_aliases(source):
    assert scan_source(source, "t.py") == []


def test_with_failure_paths_that_all_clear_constant_aliases_are_accepted():
    source = (
        'value = "admin"\ntry:\n    with context():\n        value = supplied\n'
        "        work()\nexcept Exception:\n    value = supplied\nusername = value\n"
    )

    assert scan_source(source, "t.py") == []


def test_with_exit_failure_preserves_the_post_body_constant_state():
    source = (
        'value = supplied\ntry:\n    with context():\n        value = "admin"\n'
        "except Exception:\n    pass\nelse:\n    value = supplied\nusername = value\n"
    )

    hits = scan_source(source, "t.py")

    assert len(hits) == 1
    assert hits[0].lineno == 9


def test_with_exit_paths_that_all_clear_constant_aliases_are_accepted():
    source = (
        'value = "admin"\ntry:\n    with context():\n        value = supplied\n'
        "except Exception:\n    value = supplied\nelse:\n    value = supplied\nusername = value\n"
    )

    assert scan_source(source, "t.py") == []


def test_break_paths_preserve_constants_that_bypass_loop_else():
    source = (
        'value = supplied\nfor item in items:\n    if condition:\n        value = "admin"\n'
        "        break\n    value = supplied\nelse:\n    value = supplied\nusername = value\n"
    )

    hits = scan_source(source, "t.py")

    assert len(hits) == 1
    assert hits[0].lineno == 9


def test_continue_paths_feed_constants_into_later_loop_iterations():
    source = (
        'value = supplied\nfor item in items:\n    username = value\n    value = "admin"\n'
        "    continue\n    value = supplied\n"
    )

    hits = scan_source(source, "t.py")

    assert len(hits) == 1
    assert hits[0].lineno == 3


@pytest.mark.parametrize("exit_statement", ["break", "continue"])
def test_loop_exit_paths_that_all_clear_constant_aliases_are_accepted(exit_statement):
    source = f"""\
value = "admin"
for item in items:
    value = supplied
    {exit_statement}
    value = "admin"
else:
    value = supplied
username = value
"""

    assert scan_source(source, "t.py") == []


@pytest.mark.parametrize("exit_statement", ["break", "continue"])
def test_finally_reassignment_clears_saved_loop_exit_states(exit_statement):
    source = f"""\
value = supplied
for item in items:
    try:
        value = "admin"
        {exit_statement}
    finally:
        value = supplied
username = value
"""

    assert scan_source(source, "t.py") == []


@pytest.mark.parametrize(
    "source",
    [
        'def first():\n    placeholder = "admin"\ndef second():\n    username = placeholder\n',
        'class First:\n    placeholder = "admin"\nclass Second:\n    username = placeholder\n',
    ],
)
def test_constant_aliases_do_not_leak_between_lexical_scopes(source):
    assert scan_source(source, "t.py") == []


@pytest.mark.parametrize(
    "source",
    [
        'role = "admin"',
        'client(role="admin")',
        'config = {"role": "admin"}',
        'assert by_name["admin"].enabled',
        'assert username == "admin"',
        'username = "placeholder-user"',
        'username_ref = "admin"',
        'config = {"password_ref": "admin"}',
        '"""username = admin"""',
        '# username = "admin"',
    ],
)
def test_accepts_legitimate_noncredential_uses(source):
    assert scan_source(source) == []


@pytest.mark.parametrize(
    "source",
    [
        'username = "admin"  # credential-ok: tests a rejected account\n',
        '# credential-ok: tests a rejected account\n# deliberate input\nusername = "admin"\n',
        'client(  # credential-ok: tests a rejected account\n    username="admin",\n)\n',
    ],
)
def test_accepts_comment_marker(source):
    assert scan_source(source) == []


@pytest.mark.parametrize(
    "source",
    [
        'label = "credential-ok: reason"\nusername = "admin"\n',
        '# credential-ok: reason\n\nusername = "admin"\n',
        'username = "admin"  # credential-ok:\n',
        'role = "admin"  # credential-ok: role\nusername = "admin"\n',
    ],
)
def test_marker_requires_a_reason_and_belongs_to_its_statement(source):
    assert len(scan_source(source)) == 1


def test_main_reports_forbidden_credentials(monkeypatch, capsys):
    violation = Violation("test_example.py", 3, "test_example")
    monkeypatch.setattr(credential_discipline, "scan_tree", lambda: [violation])

    assert credential_discipline._main([]) == 1
    assert str(violation) in capsys.readouterr().out


def test_scan_tree_skips_only_its_own_files(tmp_path):
    for name in ("credential_discipline.py", "test_credential_discipline.py", "test_example.py"):
        (tmp_path / name).write_text('username = "admin"\n', encoding="utf-8")
    nested = tmp_path / "nested"
    nested.mkdir()
    for name in ("credential_discipline.py", "test_credential_discipline.py"):
        (nested / name).write_text('username = "admin"\n', encoding="utf-8")
    assert [v.path for v in scan_tree(tmp_path)] == [
        "nested/credential_discipline.py",
        "nested/test_credential_discipline.py",
        "test_example.py",
    ]


def test_the_scanner_visits_every_ast_node_that_binds_a_value_to_a_target():
    """A binding form with no visitor is a silent bypass, so the set comes from ``ast`` itself.

    Three were reported one at a time (starred call arguments, a comprehension walrus, then an
    augmented assignment), which is what listing the forms by hand costs.
    """
    binding = {
        name
        for name, member in vars(ast).items()
        if isinstance(member, type)
        and issubclass(member, ast.AST)
        and "value" in getattr(member, "_fields", ())
        and {"target", "targets"} & set(member._fields)
    }

    assert binding == {"Assign", "AnnAssign", "AugAssign", "NamedExpr"}, (
        f"ast grew or lost a binding form; review the scanner against it: {sorted(binding)}"
    )
    missing = sorted(name for name in binding if not hasattr(credential_discipline._Scanner, f"visit_{name}"))
    assert missing == [], f"a binding form the scanner never inspects: {missing}"
