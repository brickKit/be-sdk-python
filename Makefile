# be-sdk-python: the official Python implementation of be-protocol 1.0.
#   make venv            create .venv (CPython 3.14) with the pinned dependencies
#   make test            unit tests + protocol vectors (no containers)
#   make itest           integration tests against throwaway PostgreSQL 16 / 14 and NATS 2.12 containers
#   make sync-protocol   copy be-protocol (ddl, schemas, vectors) and the authz decision vectors from their pinned tags
#   make gen-limits      regenerate be/v1/limits_pb2.py from be-protocol's proto/be/v1/limits.proto
.PHONY: venv test vectors itest itest-up itest-down sync-protocol gen-limits dag-check import-scan all help

PY       ?= .venv/bin/python
PREFIX   ?= sdkb-py
PG16     := $(PREFIX)-pg16
PG14     := $(PREFIX)-pg14
NATS     := $(PREFIX)-nats
PROTO_TAG ?= v1.0.0-rc.1

venv: ## create .venv with Python 3.14 and the exact pins
	uv venv --clear -p python3.14 .venv
	uv pip install -p $(PY) -e '.[dev]'

test: ## unit tests and protocol vectors
	$(PY) -m pytest -q tests/unit

vectors: ## only the protocol vectors
	$(PY) -m pytest -q tests/unit/vectors

itest-up: ## start the throwaway containers (unique prefix, tmpfs, random host ports)
	@df -h / | awk 'NR==2 {print "free on /: " $$4}'
	docker run -d --rm --name $(PG16) -e POSTGRES_PASSWORD=x --tmpfs /var/lib/postgresql/data -p 127.0.0.1::5432 postgres:16-alpine >/dev/null
	docker run -d --rm --name $(PG14) -e POSTGRES_PASSWORD=x --tmpfs /var/lib/postgresql/data -p 127.0.0.1::5432 postgres:14-alpine >/dev/null
	docker run -d --rm --name $(NATS) -p 127.0.0.1::4222 nats:2.12-alpine -js >/dev/null
	@for c in $(PG16) $(PG14); do for i in $$(seq 1 60); do docker exec $$c pg_isready -U postgres -q && break; sleep 1; done; done; sleep 1

itest-down: ## remove the throwaway containers
	-docker rm -f $(PG16) $(PG14) $(NATS) >/dev/null 2>&1

itest: ## integration tests (starts and removes the containers)
	$(MAKE) itest-up
	BESDK_IT_PG16=$$(docker port $(PG16) 5432 | head -1) BESDK_IT_PG14=$$(docker port $(PG14) 5432 | head -1) \
	BESDK_IT_NATS=$$(docker port $(NATS) 4222 | head -1) $(PY) -m pytest -q tests/integration; rc=$$?; \
	$(MAKE) itest-down; exit $$rc

sync-protocol: ## copy the pinned protocol data (BE_PROTOCOL_REPO / AUTHZ_REPO may point at local clones)
	scripts/sync_protocol.sh

gen-limits: ## regenerate the shipped be.v1.limits_pb2 (needs grpcio-tools; P7.10)
	scripts/gen_limits.sh $(PROTO_TAG)

dag-check: ## the package imports without cycles
	@$(PY) -c "import besdk, be.v1.limits_pb2" && echo "besdk imports cleanly"

import-scan: ## the SDK depends on no component repository
	@bad=$$(grep -rlE "^(from|import) (mdm|erp|crm|infra|integration|hrm|prj|ana)[_.]" besdk/ 2>/dev/null || true); \
	if [ -n "$$bad" ]; then echo "be-sdk-python must not import a component: $$bad"; exit 1; fi; echo "no component imports"

all: test dag-check import-scan

help: ## list the targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'
