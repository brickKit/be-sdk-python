import asyncio
import logging

from besdk import errs, protocol


def level_name(level):
    return {None: "none", logging.ERROR: "error", logging.WARNING: "warn"}.get(level, "info")


def test_errors_vectors():
    reasons = 0
    for c in protocol.vector("errors")["cases"]:
        match c["kind"]:
            case "reason":
                reasons += 1
                e = errs.new(c["reason"], "x")
                assert (e.reason, e.domain, e.code, e.http) == (c["reason"], c["domain"], c["grpc_code"], c["http"]), c["id"]
                assert level_name(errs.log_level(e.code)) == c["log_level"], c["id"]
                assert errs.PLATFORM[c["reason"]].retry == c["retryable"], c["id"]
                if c["reason"] != "INTERNAL":
                    assert e.problem("0")["type"] == c["type"], c["id"]
            case "code":
                assert errs.http_status(c["grpc_code"]) == c["http"], c["id"]
                assert level_name(errs.log_level(c["grpc_code"])) == c["log_level"], c["id"]
            case "access_log":
                assert level_name(errs.access_log_level(c["http"])) == c["log_level"], c["id"]
            case _:
                raise AssertionError(c["id"])
    assert reasons == len(errs.PLATFORM), "the SDK catalog and the protocol catalog must list the same reasons"


def test_internal_never_leaks():
    e = errs.from_exception(RuntimeError('relation "secret_table" does not exist')).with_("sql", "select 1")
    p = e.problem("4bf92f3577b34da6a3ce929d0e0e4736")
    assert (p["reason"], p["domain"], p["status"]) == ("INTERNAL", "be", 500)
    assert "secret_table" not in p["detail"] and "metadata" not in p
    assert "secret_table" in str(e), "the cause stays available to the logs"


def test_unknown_reason_is_internal():
    e = errs.new("NO_SUCH_REASON", "x")
    assert e.reason == "INTERNAL" and "NO_SUCH_REASON" in str(e)


def test_cancellation_and_timeouts():
    assert errs.from_exception(asyncio.CancelledError()).reason == "REQUEST_CANCELLED"
    assert errs.from_exception(TimeoutError()).reason == "DEADLINE_EXCEEDED"
    assert errs.from_exception(None) is None


def test_problem_and_retry_after():
    e = errs.new("SERVICE_BUSY", "busy").with_("resource", "queue").with_retry_after(1.5)
    p = e.problem("t")
    assert e.http == 503 and e.retry_after_header() == "2"
    assert p["type"] == "urn:be:be:SERVICE_BUSY" and p["metadata"] == {"resource": "queue"} and "violations" not in p


def test_declared_component_reason():
    insufficient = errs.define("erp/inventory", "INSUFFICIENT_STOCK", errs.Code.FAILED_PRECONDITION, 409, errs.RETRY_NO,
                               "Not enough stock")
    e = insufficient.new("only 3 left").with_("available", "3")
    assert errs.is_(e, "erp/inventory", "INSUFFICIENT_STOCK")
    p = e.problem("0")
    assert p["type"] == "urn:be:erp/inventory:INSUFFICIENT_STOCK" and p["status"] == 409


def test_with_does_not_mutate():
    base = errs.new("NOT_FOUND", "x")
    base.with_("k", "v")
    assert base.metadata == {}
