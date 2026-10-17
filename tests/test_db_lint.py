from besdk import protocol
from besdk.db import lint


def test_migration_lint_vectors():
    for c in protocol.vector("migration-lint")["cases"]:
        got = [{"file": p.file, "rule": p.rule} for p in lint.lint(c["files"], c.get("schema", ""))]
        assert got == [{"file": r["file"], "rule": r["rule"]} for r in c.get("refused") or []], c["id"]


def test_sdk_migrations_pass_the_lint():
    assert lint.lint(protocol.migrations(), "") == []
