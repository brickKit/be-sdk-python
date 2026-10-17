from datetime import timedelta

import pytest

from besdk import config, protocol
from besdk.config import ConfigError, Key, MapSource, Type


def test_config_parse_vectors():
    for c in protocol.vector("config-parse")["cases"]:
        k = Key("K", Type(c["type"]), required=c.get("required", False), default=c.get("default"),
                values=tuple(c.get("values") or ()))
        src = MapSource({} if c.get("value") is None else {"K": c["value"]})
        expect = c["expect"]
        if expect.get("error"):
            with pytest.raises(ConfigError) as e:
                config.parse([k], src)
            assert [p.kind for p in e.value.problems] == [expect["error"]], c["id"]
            continue
        v = config.parse([k], src)
        if expect.get("value") is None:
            assert not v.has("K"), c["id"]
            continue
        got = {Type.INTEGER: lambda: v.int("K"), Type.BOOLEAN: lambda: v.bool("K"),
               Type.DURATION: lambda: v.duration("K") / timedelta(milliseconds=1)}.get(k.type, lambda: v.string("K"))()
        assert got == expect["value"], c["id"]


def test_all_problems_at_once_and_no_secret_values():
    keys = [Key("A", Type.INTEGER, required=True), Key("B", Type.BOOLEAN, required=True),
            Key("PASSWORD", Type.STRING, required=True, secret=True),
            Key("LEVEL", Type.ENUM, values=("info", "warn"), default="info"),
            Key("PIN", Type.INTEGER, required=True, secret=True)]
    with pytest.raises(ConfigError) as e:
        config.parse(keys, MapSource({"A": "x1", "LEVEL": "loud", "PIN": "hunter2"}))
    joined = "\n".join(str(p) for p in e.value.problems)
    assert len(e.value.problems) == 5, joined
    for part in ["A: invalid", "B: missing", "PASSWORD: missing", "LEVEL: unknown_enum", "PIN: invalid"]:
        assert part in joined
    assert "hunter2" not in joined, "a secret's value is never printed"


def test_declaration_rules():
    with pytest.raises(ConfigError) as e:
        config.parse([Key("DEMO_ALPHA_ENDPOINT", Type.STRING), Key("PORT", Type.INTEGER), Key("lower", Type.STRING),
                      Key("E", Type.ENUM)], MapSource({}))
    assert len(e.value.problems) == 4


class FakeEnv:
    def __init__(self, env, files):
        self.env, self.files = env, files

    def lookup(self, name):
        return self.env.get(name)

    def read_file(self, path):
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path].encode()


def test_secret_from_file_and_refresh():
    env = FakeEnv({"PG_PASSWORD_FILE": "/run/secrets/pg"}, {"/run/secrets/pg": "first\n"})
    v = config.parse([Key("PG_PASSWORD", Type.STRING, required=True, secret=True)], env)
    s = v.secret("PG_PASSWORD")
    assert s.value() == "first" and v.string("PG_PASSWORD") == "first"
    assert len(v.file_secrets()) == 1
    assert not s.refresh(env.read_file)
    env.files["/run/secrets/pg"] = "second"
    assert s.refresh(env.read_file)
    assert v.string("PG_PASSWORD") == "second"
    del env.files["/run/secrets/pg"]
    with pytest.raises(OSError):
        s.refresh(env.read_file)
    assert s.value() == "second", "a failed re-read keeps the old value"


def test_secret_value_and_file_conflict():
    env = FakeEnv({"PG_PASSWORD": "a", "PG_PASSWORD_FILE": "/x"}, {"/x": "b"})
    with pytest.raises(ConfigError) as e:
        config.parse([Key("PG_PASSWORD", Type.STRING, required=True, secret=True)], env)
    assert [p.kind for p in e.value.problems] == ["conflict"]


def test_range_and_accessor_misuse():
    with pytest.raises(ConfigError) as e:
        config.parse([Key("N", Type.INTEGER, required=True, min=1, max=10)], MapSource({"N": "11"}))
    assert [p.kind for p in e.value.problems] == ["out_of_range"]
    v = config.parse([Key("N", Type.INTEGER, default=3)], MapSource({}))
    assert v.int("N") == 3
    with pytest.raises(TypeError):
        v.string("N")
    with pytest.raises(KeyError):
        v.int("UNDECLARED")
