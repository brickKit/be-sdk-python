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
