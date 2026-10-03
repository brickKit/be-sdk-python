"""be-protocol vectors `config` (P2): typed values, secrets, addresses, key names and declarations."""
import pytest

from besdk import config_values as V
from tests.unit.vectors._load import cases, expect


@pytest.mark.parametrize("case", cases("config", "values", "parse_value"))
def test_parse_value(case):
    i = case["input"]
    spec = V.KeySpec(name="K", format=i["type"], required=i.get("required", False), default=i.get("default"),
                     secret=i.get("secret", False), minimum=i.get("minimum"), schemes=tuple(i.get("schemes") or ()),
                     json_kind=i.get("json_kind"), enum=tuple(i.get("enum") or ()))

    def run():
        got = V.parse_value(spec, i["value"])
        if not got.set:
            return {"set": False}
        if spec.secret:
            return {"set": True, "source": "file", "path": got.value}
        value = got.value
        if i["type"] == "duration":
            value = str(value)
        elif i["type"] == "duration_list":
            value = [str(v) for v in value]
        return {"set": True, "value": value}

    expect(case, run)


@pytest.mark.parametrize("case", cases("config", "values", "read_undeclared"))
def test_read_undeclared(case):
    i = case["input"]
    expect(case, lambda: {"allowed": V.check_readable(i["key"], set(i["declared"]))})


@pytest.mark.parametrize("case", cases("config", "values", "secret_text"))
def test_secret_text(case):
    i = case["input"]

    def run():
        v = V.secret_text(i["content"], required=i["required"], key="K")
        return {"set": False} if v is None else {"set": True, "value": v}

    expect(case, run)


@pytest.mark.parametrize("case", cases("config", "endpoints", "endpoint_name"))
def test_endpoint_name(case):
    i = case["input"]
    expect(case, lambda: {"name": V.endpoint_name(i["dependency"], i["port"])})


@pytest.mark.parametrize("case", cases("config", "endpoints", "endpoint_value"))
def test_endpoint_value(case):
    def run():
        a = V.endpoint_address(case["input"]["value"])
        return {"present": False} if a is None else {"present": True, "address": a}

    expect(case, run)


@pytest.mark.parametrize("case", cases("config", "endpoints", "family_address"))
def test_family_address(case):
    i = case["input"]

    def run():
        a = V.family_address(i["key"], i["value"])
        if a is None:
            return {"present": False}
        return {"present": True, ("target" if i["key"].endswith("_GRPC_URL") else "base"): a}

    expect(case, run)


@pytest.mark.parametrize("case", cases("config", "keys", "key_name"))
def test_key_name(case):
    def run():
        V.check_key_name(case["input"]["key"])
        return {"valid": True}

    expect(case, run)


@pytest.mark.parametrize("case", cases("config", "keys", "key_declaration"))
def test_key_declaration(case):
    i = case["input"]

    def run():
        V.check_declaration(i["key"], secret=i.get("secret", False), mount=i.get("mount"), type_=i.get("type"))
        return {"valid": True}

    expect(case, run)
