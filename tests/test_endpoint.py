import pytest

from besdk.runtime import Config


def test_endpoint_strips_scheme():
    c = Config({"MDM_CUSTOMER_ENDPOINT": "http://be-go-core-1-0-0:8101/"})
    assert c.endpoint("mdm/customer") == ("be-go-core-1-0-0:8101", True)


def test_endpoint_ignores_process_env(monkeypatch):
    monkeypatch.setenv("MDM_CUSTOMER_ENDPOINT", "http://wrong:1")
    assert Config({}).endpoint("mdm/customer") == ("", False)


def test_endpoint_absent_and_empty_are_missing():
    assert Config({}).endpoint("infra/workflow") == ("", False)
    assert Config({"INFRA_WORKFLOW_ENDPOINT": ""}).endpoint("infra/workflow") == ("", False)


def test_must_endpoint_names_variable():
    with pytest.raises(RuntimeError, match="ERP_INVENTORY_GRPC_ENDPOINT"):
        Config({}).must_endpoint("erp/inventory", "grpc")


def test_s3_url():
    assert Config({"S3_URL": "http://rustfs:9000"}).s3_url() == ("http://rustfs:9000", True)
    assert Config({}).s3_url() == ("", False)
