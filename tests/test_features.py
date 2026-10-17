"""The feature matrix stays honest (protocol 18): exactly the SDK-owned capabilities of the vendored protocol, known
statuses, evidence that names real tests, and pending rules that belong to their capability."""
import ast
import json
import pathlib

from besdk import protocol

ROOT = pathlib.Path(__file__).resolve().parent.parent


def known_tests() -> set[str]:
    names = set()
    for f in (ROOT / "tests").glob("test_*.py"):
        for node in ast.walk(ast.parse(f.read_text())):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_"):
                names.add(f"{f.stem}::{node.name}")
    return names


def check(caps: dict, matrix: dict) -> list[str]:
    problems = []
    if matrix["protocol"] != caps["protocol"]:
        problems.append(f"features.json is for protocol {matrix['protocol']}, the copy is {caps['protocol']}")
    want = {c["id"]: c for c in caps["capabilities"] if c["owner"] == "sdk"}
    have, known = matrix["features"], known_tests()
    problems += [f"{i}: not listed" for i in sorted(set(want) - set(have))]
    problems += [f"{i}: not an SDK capability of the protocol" for i in sorted(set(have) - set(want))]
    for i, f in have.items():
        if i not in want:
            continue
        if f["status"] not in ("verified", "implemented", "not_implemented"):
            problems.append(f"{i}: unknown status {f['status']!r}")
        if f["status"] != "not_implemented" and not f.get("evidence"):
            problems.append(f"{i}: {f['status']} without evidence")
        problems += [f"{i}: evidence {e} is not a test of this repository" for e in f.get("evidence", []) if e not in known]
        problems += [f"{i}: pending {p['rule']} is not one of its rules" for p in f.get("pending", [])
                     if p["rule"] not in want[i]["rules"]]
    return problems


def test_the_feature_matrix():
    matrix = json.loads((ROOT / "features.json").read_text())
    assert check(protocol.capabilities(), matrix) == []
