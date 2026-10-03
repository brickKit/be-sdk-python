"""``besdk.main(spec)``: the one entry point of a component image (be-protocol P1.1).

No argument serves; ``migrate up`` / ``migrate down <n>`` / ``migrate status`` migrate; ``job run <name>`` runs
one declared job once. An argument it does not recognise exits 64 before the configuration is read.
This is the only place the SDK reads the process environment and the only place it exits the process.
Exit codes: 0 clean, 1 fatal, 64 usage, 78 configuration (P1 exit codes).
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import NoReturn, Sequence

from besdk import logs
from besdk.config import Config, ConfigErrors, Manifest
from besdk.runtime import Spec

EX_USAGE, EX_CONFIG, EX_FATAL = 64, 78, 1
USAGE = "usage: <entrypoint> [migrate up | migrate down <n> | migrate status | job run <name>]"


def parse(argv: Sequence[str]) -> tuple | None:
    a = list(argv)
    if not a:
        return ("serve",)
    if a in (["migrate", "up"], ["migrate", "status"]):
        return tuple(a)
    if len(a) == 3 and a[:2] == ["migrate", "down"] and a[2].isdigit() and int(a[2]) > 0:
        return ("migrate", "down", int(a[2]))
    if len(a) == 3 and a[:2] == ["job", "run"] and a[2]:
        return ("job", "run", a[2])
    return None


def _exit(code: int) -> NoReturn:
    sys.stdout.flush()
    raise SystemExit(code)


def load(spec: Spec, env: dict[str, str]) -> tuple[Manifest, Config]:
    """Manifest and configuration, or exit 78 with one JSON line per problem (P1.2)."""
    lg = logs.member_logger(spec.id, env.get("COMPONENT_VERSION", ""))
    try:
        manifest = Manifest.load(spec.manifest_path())
    except (OSError, KeyError, ValueError) as e:
        lg.error("manifest_unreadable", extra={"error": f"{type(e).__name__}: {e}"})
        _exit(EX_CONFIG)
    if manifest.id != spec.id:
        lg.error("config_invalid", extra={"key": "metadata.id", "error": f"component.yaml says {manifest.id}"})
        _exit(EX_CONFIG)
    try:
        return manifest, Config.load(env, manifest)
    except ConfigErrors as ce:
        for e in ce.errors:
            lg.error("config_invalid", extra={"key": e.key or "", "reason": e.reason, "error": e.detail})
        _exit(EX_CONFIG)


def main(spec: Spec, *, argv: Sequence[str] | None = None, env: dict[str, str] | None = None) -> NoReturn:
    cmd = parse(sys.argv[1:] if argv is None else argv)
    if cmd is None:
        print(json.dumps({"level": "error", "msg": "usage", "error": USAGE}), file=sys.stdout)
        _exit(EX_USAGE)
    env = dict(os.environ) if env is None else env
    manifest, config = load(spec, env)
    if cmd[0] == "migrate":
        from besdk.serve import migrate

        _exit(migrate(spec, manifest, config, cmd))
    if cmd[0] == "job":
        from besdk.serve import job_run

        _exit(asyncio.run(job_run(spec, env, manifest, config, cmd[2])))
    from besdk.serve import serve

    _exit(asyncio.run(serve(spec, env, manifest, config)))
