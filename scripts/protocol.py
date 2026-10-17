#!/usr/bin/env python3
"""Copy be-protocol's machine-readable parts at an exact tag into besdk/_protocol, or check that the copy matches.

    python3 -I scripts/protocol.py fetch v3.0.0-alpha.1   # replace the copy with the tag's files
    python3 -I scripts/protocol.py check                  # fetch the recorded tag again and compare byte for byte

The copy ships with the package (the SDK's own migrations run from it); it is never edited by hand.
"""
import filecmp
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile

REPO = "https://github.com/brickKit/be-protocol.git"
PARTS = ["vectors", "migrations", "migrations-bus", "capabilities.json"]
DEST = pathlib.Path(__file__).resolve().parents[1] / "besdk" / "_protocol"


def clone(tag, into):
    subprocess.run(["git", "-c", "advice.detachedHead=false", "clone", "-q", "--depth", "1", "--branch", tag, REPO, str(into)], check=True)
    commit = subprocess.run(["git", "-C", str(into), "rev-parse", "HEAD"], check=True, capture_output=True,
                            text=True).stdout.strip()
    return commit


def fetch(tag):
    with tempfile.TemporaryDirectory() as tmp:
        src = pathlib.Path(tmp) / "be-protocol"
        commit = clone(tag, src)
        if DEST.exists():
            shutil.rmtree(DEST)
        DEST.mkdir(parents=True)
        for part in PARTS:
            if (src / part).is_dir():
                shutil.copytree(src / part, DEST / part)
            else:
                shutil.copy2(src / part, DEST / part)
    (DEST / "PROTOCOL.json").write_text(json.dumps({"tag": tag, "commit": commit}, indent=2) + "\n")
    (DEST / "__init__.py").write_text('"""be-protocol\'s machine-readable parts, copied by scripts/protocol.py."""\n')
    print(f"copied {tag} ({commit})")


def differences(a, b):
    cmp = filecmp.dircmp(a, b)
    out = [f"only in {a}: {n}" for n in cmp.left_only] + [f"only in {b}: {n}" for n in cmp.right_only]
    out += [f"differs: {n}" for n in filecmp.cmpfiles(a, b, cmp.common_files, shallow=False)[1]]
    for sub in cmp.common_dirs:
        out += differences(a / sub, b / sub)
    return out


def check():
    pinned = json.loads((DEST / "PROTOCOL.json").read_text())
    with tempfile.TemporaryDirectory() as tmp:
        src = pathlib.Path(tmp) / "be-protocol"
        commit = clone(pinned["tag"], src)
        problems = [] if commit == pinned["commit"] else [f"{pinned['tag']} is now {commit}, recorded {pinned['commit']}"]
        for part in PARTS:
            if (src / part).is_dir():
                problems += differences(src / part, DEST / part)
            elif not filecmp.cmp(src / part, DEST / part, shallow=False):
                problems.append(f"differs: {part}")
    if problems:
        sys.exit("the protocol copy does not match " + pinned["tag"] + ":\n  " + "\n  ".join(problems))
    print(f"the copy matches {pinned['tag']} ({commit})")


if __name__ == "__main__":
    if sys.argv[1:2] == ["fetch"] and len(sys.argv) == 3:
        fetch(sys.argv[2])
    elif sys.argv[1:] == ["check"]:
        check()
    else:
        sys.exit(__doc__)
