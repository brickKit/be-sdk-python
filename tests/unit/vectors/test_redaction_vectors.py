"""be-protocol vectors `redaction` (P18.2): personal-data keys in log records."""
import pytest

from besdk import logs
from tests.unit.vectors._load import cases, expect


@pytest.mark.parametrize("case", cases("redaction", "redact", "redact"))
def test_redact(case):
    expect(case, lambda: {"record": logs.redact(case["input"]["record"])})


@pytest.mark.parametrize("case", cases("redaction", "redact", "protected_key"))
def test_protected_key(case):
    expect(case, lambda: {"protected": logs.protected_key(case["input"]["key"])})
