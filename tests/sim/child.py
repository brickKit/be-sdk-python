"""One replica of the simulation: `python -m tests.sim.child a|b|shell`. SIM_CRASH names the crash point (besdk.crash)
where it ends itself (exit 137, "SIM CRASH <point>" on stderr); the environment is the members' (a or b alone) or the
shell's (both members, BRICKKIT_SERVED_MEMBERS_CONFIG)."""
import asyncio
import os
import signal
import sys

from besdk import crash, launcher, shell
from besdk.config import EnvSource
from tests.sim.member import definition

POINT = os.environ.get("SIM_CRASH", "")


def hook(name: str) -> None:
    if name == POINT:
        sys.stderr.write(f"SIM CRASH {name}\n")
        sys.stderr.flush()
        os._exit(137)


def installed(d):
    """The members are this test package's, not installed distributions: they stand as installed at their version."""
    return shell.Installed(d.id.replace("/", "-"), d.version, None, ("be-sdk>=3.0.0a1",)), d.id.replace("/", "_")


async def run_shell() -> int:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    return await shell.run(shell.Identity("be/py-sim", "3.0.0"), [definition("sim/a"), definition("sim/b")],
                           shell.Options(env=EnvSource(lookup_env=os.environ.get), installed_of=installed,
                                         host="127.0.0.1"), stop)


def main() -> None:
    crash.set_hook(hook)
    mode = sys.argv[1]
    if mode == "shell":
        sys.exit(asyncio.run(run_shell()))
    launcher.main(definition({"a": "sim/a", "b": "sim/b"}[mode]))


if __name__ == "__main__":
    main()
