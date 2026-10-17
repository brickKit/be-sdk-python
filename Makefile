.PHONY: test test-integration protocol-check lint

test:  ## unit and vector tests
	uv run pytest -q

# Needs BE_SDK_TEST_PG_ADMIN (superuser connection string of be-postgres), BE_SDK_TEST_NATS_URL (JetStream) and
# BE_SDK_TEST_S3_URL, _ACCESS_KEY and _SECRET_KEY (object storage, be-rustfs; the bucket be-sdk-test). Missing
# environment fails, never skips. The interoperability tests build small Go helpers under tests/interop (Go 1.26).
test-integration:
	uv run pytest -q -m integration

protocol-check:  ## the copy of be-protocol equals its recorded tag
	python3 -I scripts/protocol.py check
