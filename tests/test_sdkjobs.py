"""The SDK's own jobs (P1.23, chapter 07): every function of the SDK that makes a job is called where the host builds
a member, and every job the SDK names says how it behaves in a recovery (besdk/sdkjobs.py) - no more, no fewer. A job
made and tested but never registered never runs and nothing fails (be-sdk-go found one in tiering)."""
import ast
import pathlib

import pytest

from besdk import sdkjobs

ROOT = pathlib.Path(__file__).resolve().parents[1] / "besdk"


def sources():
    return [p for p in ROOT.rglob("*.py") if "_protocol" not in p.parts]


def makers() -> list[tuple[str, str]]:
    """(module, function) of every module-level function whose name ends in _job."""
    out = []
    for p in sources():
        for node in ast.parse(p.read_text()).body:
            if isinstance(node, ast.FunctionDef) and node.name.endswith("_job"):
                out.append((p.relative_to(ROOT).as_posix(), node.name))
    return out


def job_names() -> set[str]:
    """The names of the jobs the SDK declares: Job(...) calls whose first argument is a "be." literal."""
    out = set()
    for p in sources():
        for node in ast.walk(ast.parse(p.read_text())):
            if isinstance(node, ast.Call) and getattr(node.func, "id", getattr(node.func, "attr", "")) == "Job" \
                    and node.args and isinstance(node.args[0], ast.Constant) and str(node.args[0].value).startswith("be."):
                out.add(node.args[0].value)
    return out


def test_the_host_registers_every_job_maker():
    found = makers()
    assert len(found) >= 4, "the check finds the SDK's job makers (else it checks nothing)"
    # by module and function: two modules both have a cleanup_job
    called = {f"{n.func.value.id}.{n.func.attr}" for n in ast.walk(ast.parse((ROOT / "host.py").read_text()))
              if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name)}
    missing = [f"{m}:{f}" for m, f in found if f"{pathlib.PurePosixPath(m).stem}.{f}" not in called]
    assert missing == [], "made but never registered by the host"


def test_every_sdk_job_says_what_it_does_in_a_recovery():
    names = job_names()
    assert names, "the check finds the SDK's jobs"
    assert sorted(names - set(sdkjobs.IN_RECOVERY)) == [], "a job without a recovery rule"
    assert sorted(set(sdkjobs.IN_RECOVERY) - names) == [], "a rule for a job the SDK no longer has"


def test_a_job_without_a_rule_is_refused():
    from besdk.work import LOCAL, SINGLETON, Job

    class Runner:
        def add(self, job):
            raise AssertionError("not reached")

    with pytest.raises(ValueError, match="recovery phases"):
        sdkjobs.add(Runner(), Job("be.new.thing", SINGLETON, lambda r: None, LOCAL, interval=60))
