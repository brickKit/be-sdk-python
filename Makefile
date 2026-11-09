.PHONY: test test-integration simulation simulation-tiering protocol-check gen gen-check lint

test:  ## unit and vector tests
	uv run pytest -q

# Needs BE_SDK_TEST_PG_ADMIN (superuser connection string of be-postgres), BE_SDK_TEST_NATS_URL (JetStream) and
# BE_SDK_TEST_S3_URL, _ACCESS_KEY and _SECRET_KEY (object storage, be-rustfs; the bucket be-sdk-test). Missing
# environment fails, never skips. The interoperability tests build small Go helpers under tests/interop (Go 1.27).
test-integration:
	uv run pytest -q -m integration

# The long simulation (tests/sim): replicas of two members, one of them a shell hosting both, each ending itself at a
# crash point taken in turn, killed now and then; then the final state is checked. The environment of
# test-integration; SIM_MINUTES (default 30) is how long the crashes go on. A long run: in the background.
simulation:
	uv run pytest -q -s -m simulation tests/sim/test_simulation.py

# The tiering's long simulation (tests/sim/test_tiering_simulation.py, be-sdk-go's tiering/sim_test.go): two members on
# the object and the warm route, swapped halfway, four replicas ending themselves at every tiering crash point; then
# every document, copy and the recovery check. Also needs BE_SDK_TEST_S3_*; SIM_MINUTES as above.
simulation-tiering:
	uv run pytest -q -s -m simulation tests/sim/test_tiering_simulation.py

protocol-check:  ## the copy of be-protocol equals its recorded tag
	python3 -I scripts/protocol.py check

gen:  ## besdk/_gen from the protocol copy's proto/ (committed; after every protocol fetch)
	uv run python scripts/gen.py

gen-check:  ## the committed generated code is exactly what the pinned proto/ generates
	uv run python scripts/gen.py --check
