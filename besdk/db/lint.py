"""Checks a component's migration files before they run (protocol 14, "structure migrations"; vector
migration-lint): file names, forward only, and the statements migrations may not contain."""
from __future__ import annotations

import re
from dataclasses import dataclass
from importlib.resources.abc import Traversable

_UP = re.compile(r"[0-9]+_[a-z0-9_]+\.up\.sql")
# statement rules in the order of the protocol's list; the first six match at the start of a statement
_RULES = [
    ("SET", re.compile(r"(?i)^(set|reset)\b"), "SET and RESET are not allowed: the migrate entry sets the schema and timeouts"),
    ("TX_CONTROL", re.compile(r"(?i)^(begin|start\s+transaction|commit|end|rollback|savepoint|release)\b"),
     "transaction control is not allowed: every file runs as one transaction"),
    ("GRANT", re.compile(r"(?i)^(grant|revoke|reassign)\b"), "grants belong to the provisioning script, not to migrations"),
    ("ROLE_SCHEMA", re.compile(r"(?i)^(create|alter|drop)\s+(role|user|group|schema)\b"),
     "roles and schemas belong to the provisioning script"),
    ("DEFAULT_PRIVILEGES", re.compile(r"(?i)^alter\s+default\s+privileges\b"),
     "default privileges belong to the provisioning script"),
    ("DROP_OWNED", re.compile(r"(?i)^drop\s+owned\b"), "DROP OWNED is not allowed"),
    ("OWNER", re.compile(r"(?i)\bowner\s+to\b"), "ownership is fixed by the provisioning script"),
    ("CONCURRENTLY", re.compile(r"(?i)\bconcurrently\b"),
     "CONCURRENTLY cannot run inside the file's transaction; use a maintenance task"),
]
_DOLLAR = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$")


@dataclass(frozen=True)
class LintProblem:
    file: str
    rule: str
    reason: str
    statement: str = ""

    def __str__(self) -> str:
        if not self.statement:
            return f"{self.file}: {self.reason} ({self.rule})"
        return f"{self.file}: {self.reason} ({self.rule}): {self.statement!r}"


def files_of(files: Traversable | dict[str, str]) -> dict[str, str | None]:
    """The .sql files of a directory (or a name -> body mapping); None for a body that cannot be read."""
    if isinstance(files, dict):
        return {n: b for n, b in files.items() if n.endswith(".sql")}
    out: dict[str, str | None] = {}
    for f in files.iterdir():
        if f.is_file() and f.name.endswith(".sql"):
            try:
                out[f.name] = f.read_text()
            except (OSError, UnicodeDecodeError):
                out[f.name] = None
    return out


def lint(files: Traversable | dict[str, str], schema: str) -> list[LintProblem]:
    """Refusals of the files; the schema's own name written into a migration is refused too, since the same files must
    run in whatever schema the assembly assigns."""
    problems: list[LintProblem] = []
    own = re.compile(r"(?i)(^|[^a-z0-9_$])\"?" + re.escape(schema) + r"\"?\s*\.") if schema else None
    versions: dict[int, str] = {}
    for name, body in sorted(files_of(files).items()):
        if name.endswith(".down.sql"):
            problems.append(LintProblem(name, "DOWN", "down migrations are not allowed: production only moves forward"))
            continue
        if not _UP.fullmatch(name):
            problems.append(LintProblem(name, "NAME", "the name must be <version>_<lower_snake_title>.up.sql"))
            continue
        v = int(name.split("_", 1)[0])
        if v in versions:
            problems.append(LintProblem(name, "DUP_VERSION", "same version as " + versions[v]))
        versions[v] = name
        if body is None:
            problems.append(LintProblem(name, "UNREADABLE", "cannot read the file"))
            continue
        for stmt in statements(body):
            problems += [LintProblem(name, code, reason, short(stmt)) for code, rx, reason in _RULES if rx.search(stmt)]
            if own and own.search(stmt):
                problems.append(LintProblem(name, "SCHEMA_NAME", "the schema name is written into the migration; "
                                            "leave names unqualified", short(stmt)))
    return problems


def statements(sql: str) -> list[str]:
    """SQL split on top-level semicolons, without comments and without the contents of string literals and dollar
    quoted bodies (a function body is checked as part of its CREATE statement, never as statements of its own)."""
    out, b, i, n = [], [], 0, len(sql)

    def flush() -> None:
        if s := "".join(b).strip():
            out.append(s)
        b.clear()

    while i < n:
        c = sql[i]
        if sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
            b.append(" ")
        elif c == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":  # an escaped quote
                        j += 2
                        continue
                    break
                j += 1
            b.append("''")
            i = j + 1
        elif c == '"':
            j = sql.find('"', i + 1)
            j = n - 1 if j < 0 else j
            ident = sql[i:j + 1]
            b.append('"_"' if "." in ident else ident)  # one identifier with a dot in it, never a qualified name
            i = j + 1
        elif c == "$" and (m := _DOLLAR.match(sql, i)):
            tag = m.group(0)
            j = sql.find(tag, i + len(tag))
            i = n if j < 0 else j + len(tag)
            b.append("$$")
        elif c == ";":
            flush()
            i += 1
        else:
            b.append(c)
            i += 1
    flush()
    return out


def short(s: str) -> str:
    s = " ".join(s.split())
    return s if len(s) <= 80 else s[:77] + "..."
