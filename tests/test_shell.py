import asyncio
import io
import json
import socket

import httpx
from fastapi.responses import JSONResponse

import besdk
from besdk import shell
from besdk.component import Definition
from besdk.config import EnvSource, Key, Type
from besdk.http import Route

SELF = shell.Identity("be/py-render", "3.0.0")


class Greeter:
    def __init__(self, rt):
        self.id, self.greeting = rt.member.id, rt.config.string("GREETING")

    def routes(self, r):
        async def hello(c):
            return JSONResponse({"from": self.id, "greeting": self.greeting})

        r.add(Route("GET", "/api/hello", hello, public=True))


def definition(cid, version):
    return Definition(cid, version, Greeter, keys=[Key("GREETING", Type.STRING, required=True)])


def installed(versions, local=(), requires=None):
    """Fake metadata: each member its own distribution at the given version, optionally installed from a path, with
    its Requires-Dist (by default a minimum SDK every test SDK meets)."""

    def lookup(d):
        if d.id not in versions:
            return None, d.id.replace("/", "_")
        url = {"url": f"file:///src/{d.id}", "dir_info": {"editable": True}} if d.id in local else None
        reqs = (requires or {}).get(d.id, ("be-sdk>=3.0.0a1",))
        return shell.Installed(d.id.replace("/", "-"), versions[d.id], url, tuple(reqs)), d.id.replace("/", "_")

    return lookup


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def served(*members):
    return json.dumps([{"componentId": cid, "version": v, "httpPort": port, "config": cfg} for cid, v, port, cfg in members])


def shell_env(port, members_json, **extra):
    values = {"COMPONENT_ID": "be/py-render", "COMPONENT_VERSION": "3.0.0", "PORT": str(port),
              "BRICKKIT_SERVED_MEMBERS_CONFIG": members_json, "STOP_DRAIN_DELAY": "100ms", **extra}
    return EnvSource(lookup_env=values.get)


async def refuse(env, lookup, *defs):
    err = io.StringIO()
    code = await shell.run(SELF, list(defs), shell.Options(env=env, stdout=io.StringIO(), stderr=err, installed_of=lookup),
                           asyncio.Event())
    assert code == 78
    return err.getvalue()


async def wait_ready(url):
    async with httpx.AsyncClient() as c:
        for _ in range(500):
            try:
                if (await c.get(url + "/readyz")).status_code == 200:
                    return
            except httpx.ConnectError:
                pass
            await asyncio.sleep(0.01)
    raise AssertionError("never ready")


async def test_hosts_members_on_their_own_ports():
    pa, pb, ps = free_port(), free_port(), free_port()
    js = served(("demo/alpha", "0.1.0", pa, {"GREETING": "hi from alpha"}),
                ("demo/beta", "0.1.0", pb, {"GREETING": "hi from beta"}))
    stop, out = asyncio.Event(), io.StringIO()
    run = asyncio.create_task(shell.run(SELF, [definition("demo/alpha", "0.1.0"), definition("demo/beta", "0.1.0")],
                                        shell.Options(env=shell_env(ps, js), stdout=out, stderr=io.StringIO(),
                                                      host="127.0.0.1", installed_of=installed(
                                                          {"demo/alpha": "0.1.0", "demo/beta": "0.1.0"})), stop))
    base = f"http://127.0.0.1:{ps}"
    await wait_ready(base)
    async with httpx.AsyncClient() as c:
        assert (await c.get(f"http://127.0.0.1:{pa}/api/hello")).json() == {"from": "demo/alpha", "greeting": "hi from alpha"}
        assert (await c.get(f"http://127.0.0.1:{pb}/api/hello")).json() == {"from": "demo/beta", "greeting": "hi from beta"}
        desc = (await c.get(base + "/.well-known/be-component")).text
        assert '"component_id":"be/py-render"' in desc and '"demo/beta"' in desc
        metrics = (await c.get(base + "/metrics")).text
        assert 'component="demo/alpha"' in metrics and 'component="demo/beta"' in metrics
    ready = [json.loads(line) for line in out.getvalue().splitlines() if '"event":"ready"' in line]
    assert {r["component"] for r in ready} == {"demo/alpha", "demo/beta", SELF.id}, "each member says it is ready (P1.12)"
    stop.set()
    assert await run == 0
    async with httpx.AsyncClient() as c:
        try:
            await c.get(f"http://127.0.0.1:{pa}/healthz")
            raise AssertionError("a member still listens after the stop")
        except httpx.ConnectError:
            pass


async def test_a_shell_in_recovery():
    # P1.22: the shell and its members are recovery_ready in the phase one member sets; the user surface answers
    # MAINTENANCE (P1.23); the self-descriptions say the phase
    pa, pb, ps = free_port(), free_port(), free_port()
    js = served(("demo/alpha", "0.1.0", pa, {"GREETING": "x", "RECOVERY_PHASE": "frozen"}),
                ("demo/beta", "0.1.0", pb, {"GREETING": "x"}))
    stop = asyncio.Event()
    run = asyncio.create_task(shell.run(SELF, [definition("demo/alpha", "0.1.0"), definition("demo/beta", "0.1.0")],
                                        shell.Options(env=shell_env(ps, js), stdout=io.StringIO(), stderr=io.StringIO(),
                                                      host="127.0.0.1", installed_of=installed(
                                                          {"demo/alpha": "0.1.0", "demo/beta": "0.1.0"})), stop))
    base = f"http://127.0.0.1:{ps}"
    await wait_ready(base)
    async with httpx.AsyncClient() as c:
        for port in (ps, pa, pb):
            r = await c.get(f"http://127.0.0.1:{port}/readyz")
            assert r.json() == {"state": "recovery_ready", "phase": "frozen"}, port
        r = await c.get(f"http://127.0.0.1:{pb}/api/hello")
        assert r.status_code == 503 and r.json()["reason"] == "MAINTENANCE"
        desc = (await c.get(base + "/.well-known/be-component")).json()
        assert desc["recovery_phase"] == "frozen" and all(m["recovery_phase"] == "frozen" for m in desc["members"])
    stop.set()
    assert await run == 0


async def test_refuses_member_not_installed():
    js = served(("demo/alpha", "0.1.0", 18081, {"GREETING": "x"}), ("demo/beta", "0.1.0", 18082, {"GREETING": "x"}))
    out = await refuse(shell_env(18080, js), installed({"demo/alpha": "0.1.0"}), definition("demo/alpha", "0.1.0"))
    assert "demo/beta:componentId" in out and "this shell has no module demo/beta" in out


async def test_refuses_other_version_in_the_metadata():
    js = served(("demo/alpha", "0.1.0", 18081, {"GREETING": "x"}))
    out = await refuse(shell_env(18080, js), installed({"demo/alpha": "0.2.0"}), definition("demo/alpha", "0.1.0"))
    assert "distribution demo-alpha at 0.1.0, but the environment has 0.2.0" in out


async def test_prerelease_versions_compare_by_meaning():
    js = served(("demo/alpha", "3.0.0-alpha.1", 18081, {"GREETING": "x"}))
    plans, refusals, _ = shell.check(js, [definition("demo/alpha", "3.0.0-alpha.1")],
                                     installed({"demo/alpha": "3.0.0a1"}), False)
    assert refusals == [] and len(plans) == 1


async def test_refuses_local_installs_unless_allowed():
    pa, ps = free_port(), free_port()
    js = served(("demo/alpha", "0.1.0", pa, {"GREETING": "x"}))
    lookup = installed({"demo/alpha": "0.1.0"}, local=("demo/alpha",))
    out = await refuse(shell_env(ps, js), lookup, definition("demo/alpha", "0.1.0"))
    assert "no local install in a release build" in out
    stop, logs = asyncio.Event(), io.StringIO()
    run = asyncio.create_task(shell.run(SELF, [definition("demo/alpha", "0.1.0")], shell.Options(
        env=shell_env(ps, js, SHELL_ALLOW_REPLACE="true"), stdout=logs, stderr=io.StringIO(), host="127.0.0.1",
        installed_of=lookup), stop))
    await wait_ready(f"http://127.0.0.1:{ps}")
    stop.set()
    assert await run == 0
    assert "shell built for development" in logs.getvalue()


async def test_refuses_a_member_that_requires_a_newer_sdk():
    # P1.21: the shell's SDK comes from its own build (override-dependencies) and can be older than a member requires
    js = served(("demo/alpha", "0.1.0", 18081, {"GREETING": "x"}), ("demo/beta", "0.1.0", 18082, {"GREETING": "x"}))
    lookup = installed({"demo/alpha": "0.1.0", "demo/beta": "0.1.0"}, requires={"demo/beta": ("httpx", "be-sdk>=99.0")})
    out = await refuse(shell_env(18080, js), lookup, definition("demo/alpha", "0.1.0"), definition("demo/beta", "0.1.0"))
    assert "demo/beta:be-sdk" in out and "be-sdk>=99.0" in out and besdk.Version in out
    assert "demo/alpha:be-sdk" not in out


async def test_refuses_a_member_that_declares_no_sdk_minimum():
    # without a minimum in the metadata the shell cannot tell; a Git tag in [tool.uv.sources] never reaches it
    js = served(("demo/alpha", "0.1.0", 18081, {"GREETING": "x"}), ("demo/beta", "0.1.0", 18082, {"GREETING": "x"}))
    lookup = installed({"demo/alpha": "0.1.0", "demo/beta": "0.1.0"},
                       requires={"demo/alpha": ("be-sdk",), "demo/beta": ("httpx>=0.27",)})
    out = await refuse(shell_env(18080, js), lookup, definition("demo/alpha", "0.1.0"), definition("demo/beta", "0.1.0"))
    assert "demo/alpha:be-sdk" in out and "demo/beta:be-sdk" in out and "be-sdk>=" in out


def test_sdk_requirement_compares_by_meaning():
    js = served(("demo/alpha", "0.1.0", 18081, {"GREETING": "x"}))
    defs = [definition("demo/alpha", "0.1.0")]

    def refusals(*reqs):
        _, out, _ = shell.check(js, defs, installed({"demo/alpha": "0.1.0"}, requires={"demo/alpha": reqs}), False,
                                sdk="3.0.0a35")
        return [r.key for r in out]

    assert refusals("be-sdk>=3.0.0a35") == []
    assert refusals("be_sdk>=3.0.0a34") == []  # names compare normalized
    assert refusals('be-sdk>=3.0.0a34; python_version >= "3"') == []
    assert refusals("be-sdk>=3.0.0a36") == ["demo/alpha:be-sdk"]
    assert refusals("be-sdk>=3.0.0") == ["demo/alpha:be-sdk"]  # a final release is newer than its alphas


async def test_refuses_version_mismatch_and_bad_config_all_at_once():
    js = served(("demo/alpha", "0.1.0", 18081, {}), ("demo/beta", "0.2.0", 18082, {"LOG_LEVEL": "loud", "GREETING": "x"}))
    out = await refuse(shell_env(18080, js), installed({"demo/alpha": "0.1.0", "demo/beta": "0.2.0"}),
                       definition("demo/alpha", "0.1.0"), definition("demo/beta", "0.1.0"))
    assert "demo/alpha:GREETING" in out and "demo/beta:LOG_LEVEL" in out
    assert "0.2.0, the version installed is 0.1.0" in out


async def test_refuses_process_keys_that_disagree():
    # gamma leaves OTEL_URL unset: it does not use that resource, so it agrees with both (P2.11)
    js = served(("demo/gamma", "0.1.0", 18083, {"GREETING": "x"}),
                ("demo/alpha", "0.1.0", 18081, {"GREETING": "x", "OTEL_URL": "http://otel-a:4318"}),
                ("demo/beta", "0.1.0", 18082, {"GREETING": "x", "OTEL_URL": "http://otel-b:4318"}))
    out = await refuse(shell_env(18080, js), installed({"demo/alpha": "0.1.0", "demo/beta": "0.1.0", "demo/gamma": "0.1.0"}),
                       definition("demo/alpha", "0.1.0"), definition("demo/beta", "0.1.0"), definition("demo/gamma", "0.1.0"))
    assert "demo/beta:OTEL_URL" in out
    assert "demo/gamma" not in out and "demo/alpha:OTEL_URL" not in out


async def test_a_member_without_a_phase_runs_in_the_shells_phase():
    # P1.22: the shell's phase is the members' one; a member whose manifest declares none (a probe's) still runs in
    # the phase the others set, never on while they are frozen; two phases disagree like any process-level key
    js = served(("demo/alpha", "0.1.0", 18081, {"GREETING": "x", "RECOVERY_PHASE": "frozen"}),
                ("demo/beta", "0.1.0", 18082, {"GREETING": "x"}))
    plans, refusals, _ = shell.check(js, [definition("demo/alpha", "0.1.0"), definition("demo/beta", "0.1.0")],
                                     installed({"demo/alpha": "0.1.0", "demo/beta": "0.1.0"}), False)
    assert refusals == []
    assert {p.definition.id: p.values.string("RECOVERY_PHASE") for p in plans} == {"demo/alpha": "frozen",
                                                                                  "demo/beta": "frozen"}
    js = served(("demo/alpha", "0.1.0", 18081, {"GREETING": "x", "RECOVERY_PHASE": "frozen"}),
                ("demo/beta", "0.1.0", 18082, {"GREETING": "x", "RECOVERY_PHASE": "verifying"}))
    _, refusals, _ = shell.check(js, [definition("demo/alpha", "0.1.0"), definition("demo/beta", "0.1.0")],
                                 installed({"demo/alpha": "0.1.0", "demo/beta": "0.1.0"}), False)
    assert [r.key for r in refusals] == ["demo/beta:RECOVERY_PHASE"]


async def test_shell_does_not_migrate():
    err = io.StringIO()
    code = await shell.run(SELF, [], shell.Options(args=["migrate"], env=EnvSource(lookup_env={}.get), stderr=err,
                                                   stdout=io.StringIO()), asyncio.Event())
    assert code == 64 and "each member's own image" in err.getvalue()


def test_installed_of_finds_the_real_distribution():
    # a definition whose new function lives in an installed distribution (FastAPI's JSONResponse stands in)
    inst, top = shell.installed_of(Definition("demo/x", "1.0.0", JSONResponse))
    assert top == "starlette" and inst is not None and inst.name.lower() == "starlette"
    assert any(r.startswith("anyio") for r in inst.requires)  # its Requires-Dist, read for P1.21
