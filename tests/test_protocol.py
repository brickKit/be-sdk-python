import hashlib

from besdk import protocol


def test_the_copy_matches_its_checksums():
    sums = protocol.ROOT.joinpath("vectors", "SHA256SUMS").read_text().splitlines()
    assert sums
    for line in sums:
        digest, name = line.split("  ")
        data = protocol.ROOT.joinpath("vectors", name).read_bytes()
        assert hashlib.sha256(data).hexdigest() == digest, name


def test_the_copy_names_its_tag_and_protocol_version():
    pinned = protocol.pinned()
    assert pinned["tag"] == "v" + protocol.capabilities()["protocol"]
    assert len(pinned["commit"]) == 40
    assert protocol.vector("errors")["protocol"] == protocol.capabilities()["protocol"]


def test_the_sdk_migrations_ship():
    names = sorted(f.name for f in protocol.migrations().iterdir() if f.name.endswith(".up.sql"))
    assert names[0] == "0001_doc_series.up.sql"


def test_the_go_interop_helpers_pin_the_same_protocol():
    """The Go helpers of the interoperability tests run the Go SDK of the protocol this package copies: one that
    lagged (alpha.18 against alpha.42) passed only while both protocols happened to ship the same migrations."""
    import json
    import pathlib
    import re

    root = pathlib.Path(__file__).parent
    tag = json.loads((root.parent / "besdk" / "_protocol" / "PROTOCOL.json").read_text())["tag"]
    for mod in sorted((root / "interop").glob("*/go.mod")):
        pinned = re.search(r"github.com/brickKit/be-protocol/v3 (\S+)", mod.read_text())
        assert pinned and pinned.group(1) == tag, f"{mod.parent.name} pins {pinned and pinned.group(1)}, the copy is {tag}"
