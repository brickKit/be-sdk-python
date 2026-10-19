"""Statements that would leave state on a pooled connection after the transaction (P5.30). Every setting belongs in
the transaction (P5.5), so a pooled connection carries nothing to the next transaction and needs no session reset on
release (asyncpg's default reset cost every short transaction a round trip); a statement that would break that fails
where it is written instead of being cleaned up unseen."""
from __future__ import annotations

import re

_WORD = re.compile(r"[A-Za-z_]+")
_ARG = r"\s*(?:'(?:[^']|'')*'|\$\d+)\s*"
_SESSION_SET_CONFIG = re.compile(rf"set_config\s*\({_ARG},{_ARG},\s*false\s*\)", re.IGNORECASE)


def _first_word(sql: str) -> tuple[str, str]:
    """The first SQL keyword in upper case, past blanks and comments, and the text after it."""
    s = sql
    while True:
        s = s.lstrip(" \t\r\n")
        if s.startswith("/*"):
            end = s.find("*/")
            if end < 0:
                return "", ""
            s = s[end + 2:]
        elif s.startswith("--"):
            end = s.find("\n")
            if end < 0:
                return "", ""
            s = s[end + 1:]
        else:
            m = _WORD.match(s)
            return (m.group(0).upper(), s[m.end():]) if m else ("", s)


def session_state(sql: str) -> str:
    """What sql would leave on the pooled connection, or "": SET without LOCAL (SET TRANSACTION and SET CONSTRAINTS
    end with the transaction), set_config(…, false), RESET, DISCARD, LISTEN, UNLISTEN and session-level advisory
    locks. A setting made inside a function or a DO block is not seen here."""
    word, rest = _first_word(sql)
    if word == "SET":
        return "" if _first_word(rest)[0] in ("LOCAL", "TRANSACTION", "CONSTRAINTS") else "SET without LOCAL"
    if word in ("RESET", "DISCARD", "LISTEN", "UNLISTEN"):
        return word
    if word == "DO":
        from besdk.db.lint import statements_with_bodies  # lint imports this module

        for code, bodies in statements_with_bodies(sql):
            if what := body_session_state(code, bodies):
                return what + " in a DO block"
    if ("et_config" in sql or "ET_CONFIG" in sql) and _SESSION_SET_CONFIG.search(sql):
        return "set_config(…, false)"
    if "dvisory" in sql or "DVISORY" in sql:
        lower = sql.lower()
        if "pg_advisory_lock" in lower or "pg_try_advisory_lock" in lower:
            return "a session-level advisory lock"
    return ""


_BODY_OWNER = re.compile(r"(?i)^(create\s+(or\s+replace\s+)?(function|procedure)|do)\b")
# SET or RESET at the start of a body statement: at the body's start, after a semicolon, or after BEGIN, DECLARE,
# THEN, ELSE or LOOP; the word after SET is checked separately, as the Go SDK must (RE2 has no look-ahead)
_BODY_STATEMENT = re.compile(r"(?i)(?:^|;|\b(?:begin|declare|then|else|loop))\s*\b(set|reset)\b")


def body_session_state(code: str, bodies: list[str]) -> str:
    """The session state a function, procedure or DO body would leave on the connection, or "" (P5.31): the
    statement-level rules applied inside the body. Strings run by a dynamic EXECUTE are not seen."""
    if not _BODY_OWNER.match(code):
        return ""
    for body in bodies:
        text = _without_comments(body)
        for m in _BODY_STATEMENT.finditer(text):
            if m.group(1).upper() == "RESET":
                return "RESET"
            if _first_word(text[m.end(1):])[0] in ("LOCAL", "TRANSACTION", "CONSTRAINTS"):
                continue
            return "SET without LOCAL"
        if _SESSION_SET_CONFIG.search(text):
            return "set_config(…, false)"
        lower = text.lower()
        if "pg_advisory_lock" in lower or "pg_try_advisory_lock" in lower:
            return "a session-level advisory lock"
    return ""


def _without_comments(sql: str) -> str:
    """sql without -- and /* */ comments outside string literals."""
    out, i, n = [], 0, len(sql)
    while i < n:
        if sql[i] == "'":
            j = i + 1
            while j < n and not (sql[j] == "'" and (j + 1 >= n or sql[j + 1] != "'")):
                j += 2 if sql[j] == "'" else 1  # an escaped quote
            out.append(sql[i:min(j + 1, n)])
            i = min(j + 1, n)
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            if j < 0:
                break
            i = j
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            if j < 0:
                break
            out.append(" ")
            i = j + 2
        else:
            out.append(sql[i])
            i += 1
    return "".join(out)


def guard(sql: str) -> None:
    if what := session_state(sql):
        raise RuntimeError(f"db: {what} would stay on the pooled connection after the transaction; use SET LOCAL, "
                           "set_config(…, true) or pg_advisory_xact_lock (protocol P5.30)")


async def no_reset(_conn) -> None:
    """The pool's reset: nothing past asyncpg's own rollback of a transaction left open (P5.30)."""
