"""Finds the code that breaks when a module shares a process with other members of a shell (component implementation
conventions, "merge safety"): reading the process environment, initialising process-wide state, exiting the process,
and building servers, listeners or connections behind the SDK's back.

It parses Python files and resolves imports, so `from os import environ as e` and `import os as o` are caught too;
it is a lint, not a proof (getattr or a call through a stored function escape it).

    python3 -m besdk.mergesafety <file or directory>...   # exit 1 when anything is found
"""
from __future__ import annotations

import ast
import pathlib
import sys
from dataclasses import dataclass

_ENV = "reads or changes the process environment, shared by every member of a shell"
_EXIT = "exits the whole shell; raise instead"
_LISTEN = "a module never serves or listens; the launcher or the shell does"
_POOL = "connects outside the SDK; use the runtime"
_CALL = "calls outside the SDK (no deadline, retry budget or breaker); use the runtime's clients"
_GLOBAL_LOG = "touches the process-wide logging configuration; use the runtime's logger"
_PROM = "registers on the process-wide default registry; the second member crashes"

# (module, attribute) -> why; "*" matches every attribute of the module
RULES: dict[tuple[str, str], str] = {
    ("os", "environ"): _ENV, ("os", "getenv"): _ENV, ("os", "putenv"): _ENV, ("os", "unsetenv"): _ENV,
    ("os", "environb"): _ENV, ("os", "_exit"): _EXIT, ("sys", "exit"): _EXIT, ("builtins", "exit"): _EXIT,
    ("builtins", "quit"): _EXIT,
    ("logging", "basicConfig"): _GLOBAL_LOG, ("logging", "getLogger"): _GLOBAL_LOG, ("logging", "root"): _GLOBAL_LOG,
    ("logging", "disable"): _GLOBAL_LOG, ("logging", "setLoggerClass"): _GLOBAL_LOG,
    ("logging", "captureWarnings"): _GLOBAL_LOG, ("logging.config", "*"): _GLOBAL_LOG,
    ("prometheus_client", "REGISTRY"): _PROM, ("prometheus_client", "start_http_server"): _LISTEN,
    ("opentelemetry.trace", "set_tracer_provider"): "sets the process-wide tracer provider; use the runtime's tracer",
    ("opentelemetry.propagate", "set_global_textmap"): "sets the process-wide propagator",
    ("opentelemetry.metrics", "set_meter_provider"): "sets the process-wide meter provider",
    ("signal", "signal"): "signal handling belongs to the launcher",
    ("signal", "set_wakeup_fd"): "signal handling belongs to the launcher",
    ("asyncio", "run"): "the launcher runs the event loop", ("asyncio", "set_event_loop_policy"): "process-wide",
    ("fastapi", "FastAPI"): "builds an application outside the SDK's middleware chain",
    ("starlette.applications", "Starlette"): "builds an application outside the SDK's middleware chain",
    ("uvicorn", "*"): _LISTEN, ("socket", "create_server"): _LISTEN, ("grpc", "server"): _LISTEN,
    ("grpc.aio", "server"): _LISTEN,
    ("asyncpg", "connect"): _POOL, ("asyncpg", "create_pool"): _POOL, ("psycopg", "connect"): _POOL,
    ("nats", "connect"): "opens a bus connection outside the SDK; use the runtime's bus",
    ("aioboto3", "Session"): "opens object storage outside the SDK",
    ("grpc", "insecure_channel"): _CALL, ("grpc", "secure_channel"): _CALL, ("grpc.aio", "insecure_channel"): _CALL,
    ("grpc.aio", "secure_channel"): _CALL, ("httpx", "*"): _CALL, ("requests", "*"): _CALL,
    ("urllib.request", "urlopen"): _CALL, ("aiohttp", "ClientSession"): _CALL,
}
# metric constructors are fine with an explicit registry= and a violation without one
_METRICS = {"Counter", "Gauge", "Histogram", "Summary", "Info", "Enum"}


# Where the SDK itself owns what modules may not do: the launcher runs the loop and exits, the host serves, the HTTP
# layer builds each member's application, the configuration source reads the environment for the launcher.
SDK_ALLOW: dict[str, set[str]] = {
    "besdk/config.py": {"os.environ"},
    "besdk/host.py": {"fastapi.FastAPI", "uvicorn.Server", "uvicorn.Config"},
    "besdk/http.py": {"fastapi.FastAPI"},
    "besdk/launcher.py": {"asyncio.run", "sys.exit"},
    "besdk/mergesafety.py": {"sys.exit"},
}


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    name: str
    why: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.name}: {self.why}"


class _Scan(ast.NodeVisitor):
    def __init__(self, path: str):
        self.path, self.aliases, self.findings = path, {}, []  # local name -> dotted origin

    def visit_Import(self, node: ast.Import) -> None:
        for a in node.names:
            if a.asname:
                self.aliases[a.asname] = a.name
            else:
                self.aliases[a.name.split(".")[0]] = a.name.split(".")[0]

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for a in node.names:
            origin = f"{node.module}.{a.name}" if node.module else a.name
            self.aliases[a.asname or a.name] = origin
            self._check(origin, node)

    def _dotted(self, node: ast.expr) -> str | None:
        if isinstance(node, ast.Name):
            if node.id in self.aliases:
                return self.aliases[node.id]
            return "builtins." + node.id if node.id in ("exit", "quit") else None
        if isinstance(node, ast.Attribute) and (base := self._dotted(node.value)):
            return base + "." + node.attr
        return None

    def _check(self, dotted: str, node: ast.AST) -> bool:
        module, _, attr = dotted.rpartition(".")
        why = RULES.get((module, attr)) or RULES.get((module, "*"))
        if why is None and "." in module:  # e.g. uvicorn.config.Config under ("uvicorn", "*")
            top = module.split(".")[0]
            why = RULES.get((top, "*")) if (top, "*") in RULES else None
        if why:
            self.findings.append(Finding(self.path, getattr(node, "lineno", 0), dotted, why))
        return bool(why)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        dotted = self._dotted(node)
        if not (dotted and self._check(dotted, node)):
            self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if node.id in ("exit", "quit") and node.id not in self.aliases and isinstance(node.ctx, ast.Load):
            self._check("builtins." + node.id, node)

    def visit_Call(self, node: ast.Call) -> None:
        dotted = self._dotted(node.func)
        if dotted and dotted.rpartition(".")[0] == "prometheus_client" and dotted.rpartition(".")[2] in _METRICS:
            if not any(k.arg == "registry" for k in node.keywords):
                self.findings.append(Finding(self.path, node.lineno, dotted, _PROM + "; pass registry=rt.metrics"))
        self.generic_visit(node)


def scan_source(source: str, path: str = "<source>") -> list[Finding]:
    s = _Scan(path)
    s.visit(ast.parse(source, path))
    return sorted(set(s.findings), key=lambda f: (f.path, f.line, f.name))


def scan(paths: list[str], allow: dict[str, set[str]] | None = None) -> list[Finding]:
    """Every .py file under paths; allow maps a file name (relative path suffix) to the names it may use."""
    out = []
    for p in paths:
        files = [pathlib.Path(p)] if p.endswith(".py") else sorted(pathlib.Path(p).rglob("*.py"))
        for f in files:
            permitted = next((names for suffix, names in (allow or {}).items() if f.as_posix().endswith(suffix)), set())
            out += [x for x in scan_source(f.read_text(), f.as_posix()) if x.name not in permitted]
    return out


if __name__ == "__main__":
    found = scan(sys.argv[1:])
    for f in found:
        print(f)
    sys.exit(1 if found else 0)
