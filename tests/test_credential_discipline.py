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


def test_baseline_update_argument_is_rejected_without_writing(monkeypatch, capsys):
    wrote = False

    def record_write(*args, **kwargs):
        nonlocal wrote
        wrote = True

    monkeypatch.setattr(credential_discipline, "save_baseline", record_write, raising=False)

    assert credential_discipline._main(["--update-baseline"]) == 2
    assert wrote is False
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
