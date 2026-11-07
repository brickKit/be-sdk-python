"""A unit's object keys and its manifest's canonical bytes against be-protocol's vector tier-manifest (P15.15,
P15.16); a manifest read back and written again gives the same bytes."""
import json

import pytest

from besdk import canon, protocol
from besdk.tiering import manifest


@pytest.mark.parametrize("c", protocol.vector("tier-manifest")["cases"], ids=lambda c: c["id"])
def test_the_tier_manifest_vectors(c):
    i, want = c["input"], c["expected"]
    if "key" in want:
        assert manifest.object_key(i["prefix"], i["class"], i["unit"], i["generation"], i["version"], i["table"],
                                   i["part"]) == want["key"]
    else:
        assert canon.encode(i["manifest"]).decode() == want["canonical"]
        back = manifest.parse(json.dumps(i["manifest"]).encode())
        assert back.encode().decode() == want["canonical"], "parsed and written again: the same bytes"
