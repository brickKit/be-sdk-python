"""One schema migrated by both SDKs (be-protocol P5.12, A111): the Go SDK's migrate entry (tests/interop/gomigrate,
pinned to be-sdk-go v3.0.0-alpha.1) and the Python one read and write the same state tables under the same lock."""
import asyncio
import os
import pathlib
import subprocess

import pytest

from tests import pgtest
from tests.test_db_integration import LOG, values
from besdk.db import migrate as mig
from besdk.db.lint import files_of

pytestmark = pytest.mark.integration
HELPER = pathlib.Path(__file__).parent / "interop" / "gomigrate"
# the SDK's own migrations, from the protocol copy: both SDKs carry the same set (the Go helper pins the Go SDK of the
# same protocol), so the count follows the protocol instead of being written down
SDK_FILES = len([n for n in files_of(mig.sdk_migrations().files) if n.endswith(".up.sql")])


def go_migrate(name: str, files_dir: pathlib.Path) -> subprocess.CompletedProcess:
    env = {**os.environ, **pgtest.member_env(name), "MEMBER_ID": "demo/" + name, "MIGRATIONS_DIR": str(files_dir)}
    return subprocess.run(["go", "run", ".", "migrate"], cwd=HELPER, env=env, capture_output=True, text=True, timeout=300)


async def py_migrate(name: str, files_dir: pathlib.Path) -> None:
    c, _ = mig.config_from("demo/" + name, values(name), LOG)
    await mig.migrate(c, mig.sdk_migrations(), mig.MigrationSet("demo/" + name, "schema_migrations",
                                                                {f.name: f.read_text() for f in files_dir.iterdir()}))


async def state(name: str):
    a = await pgtest.admin()
    try:
        return (tuple(await a.fetchrow(f"SELECT version, dirty FROM {name}.be_sdk_migrations")),
                tuple(await a.fetchrow(f"SELECT version, dirty FROM {name}.schema_migrations")),
                await a.fetchval(f"SELECT count(*) FROM {name}.runs"))
    finally:
        await a.close()


@pytest.mark.parametrize("first", ["python", "go"])
async def test_one_schema_two_sdks(tmp_path, first):
    name = f"py_interop_{first}"
    await pgtest.member(name)
    (tmp_path / "0001_runs.up.sql").write_text("CREATE TABLE runs (n int); INSERT INTO runs VALUES (1);")
    (tmp_path / "0002_more.up.sql").write_text("INSERT INTO runs VALUES (2);")

    async def run(which):
        if which == "python":
            await py_migrate(name, tmp_path)
        else:
            p = await asyncio.to_thread(go_migrate, name, tmp_path)
            assert p.returncode == 0, p.stdout + p.stderr

    second = "go" if first == "python" else "python"
    await run(first)
    assert await state(name) == ((SDK_FILES, False), (2, False), 2)
    await run(second)
    assert await state(name) == ((SDK_FILES, False), (2, False), 2), "the other SDK finds everything applied"
    (tmp_path / "0003_last.up.sql").write_text("INSERT INTO runs VALUES (3);")
    await run(second)
    await run(first)
    assert await state(name) == ((SDK_FILES, False), (3, False), 3), "a file applied by one SDK is not run again by the other"


async def test_both_sdks_at_once_run_every_file_once(tmp_path):  # P5.16 across languages: the same advisory lock
    name = "py_interop_both"
    await pgtest.member(name)
    (tmp_path / "0001_runs.up.sql").write_text("CREATE TABLE runs (n int); INSERT INTO runs VALUES (1);")
    for i in range(2, 8):
        (tmp_path / f"000{i}_step.up.sql").write_text(f"INSERT INTO runs VALUES ({i}); SELECT pg_sleep(0.05);")
    go = asyncio.create_task(asyncio.to_thread(go_migrate, name, tmp_path))
    await py_migrate(name, tmp_path)
    p = await go
    assert p.returncode == 0, p.stdout + p.stderr
    assert await state(name) == ((SDK_FILES, False), (7, False), 7)
