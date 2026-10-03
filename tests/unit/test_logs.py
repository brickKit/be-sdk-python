"""Structured logs (P18.2): one JSON object per line, envelope fields, 2 KiB cap that stays valid JSON."""
import io
import json
import logging

from besdk import logs


def _logger(level="info"):
    buf = io.StringIO()
    lg = logs.member_logger("conformance/widget", "1.0.0", level=level, stream=buf)
    return lg, buf


def _lines(buf):
    return [json.loads(x) for x in buf.getvalue().splitlines()]


def test_line_has_envelope_fields_and_extra_fields_redacted():
    lg, buf = _logger()
    lg.info("owner_updated", extra={"owner_id": "o1", "phone": "138"})
    (line,) = _lines(buf)
    assert line["msg"] == "owner_updated" and line["level"] == "info"
    assert line["component_id"] == "conformance/widget" and line["component_version"] == "1.0.0"
    assert line["time"].endswith("Z") and len(line["time"].split(".")[1]) == 10  # nanoseconds + Z
    assert line["owner_id"] == "o1" and line["phone"] == "[REDACTED]"


def test_level_filter_per_member():
    lg, buf = _logger("warn")
    lg.info("dropped")
    lg.warning("kept")
    assert [x["msg"] for x in _lines(buf)] == ["kept"]
    assert [x["level"] for x in _lines(buf)] == ["warn"]


def test_member_logger_does_not_touch_root_logging():
    root_handlers = list(logging.getLogger().handlers)
    lg, _ = _logger()
    assert lg.propagate is False
    assert logging.getLogger().handlers == root_handlers


def test_long_line_is_cut_and_stays_valid_json():
    lg, buf = _logger()
    lg.info("big", extra={"note": "x" * 5000, "other": "y" * 300, "small": "z"})
    raw = buf.getvalue().splitlines()[0]
    assert len(raw.encode()) <= 2048
    line = json.loads(raw)
    assert line["truncated"] is True
    assert line["note"].endswith("…[TRUNCATED]")
    assert line["small"] == "z" and line["msg"] == "big"


def test_exception_goes_to_error_field():
    lg, buf = _logger()
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        lg.exception("failed")
    (line,) = _lines(buf)
    assert line["level"] == "error" and "boom" in line["error"]


def test_context_fields_are_added(monkeypatch):
    from besdk import context

    lg, buf = _logger()
    with context.scope(request_id="r-9", sub="u1", perm="x.y.view"):
        lg.info("inside")
    (line,) = _lines(buf)
    assert (line["request_id"], line["sub"], line["perm"]) == ("r-9", "u1", "x.y.view")
