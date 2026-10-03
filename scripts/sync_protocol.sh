#!/bin/bash
# Copy the normative data of be-protocol (and the authz decision vectors of contract-infra-authz)
# from their pinned tags into this repository. The copies are committed: the runtime data ships in
# the wheel, the vectors let `make test` run offline.
#
#   make sync-protocol                                     # clone the pinned tags from GitHub
#   BE_PROTOCOL_REPO=../be-protocol AUTHZ_REPO=../../contracts/infra/authz make sync-protocol
#
# What lands where:
#   besdk/_protocol/ddl/*.sql             reference DDL = the platform migration (P11.3)
#   besdk/_protocol/{errors-be,config-keys}.yaml   the `be` reasons (P4) and the key catalogue (P2)
#   besdk/_protocol/VERSION               the tag the copy came from
#   tests/protocol/vectors/               be-protocol vectors/ (checked against its SHA256SUMS)
#   tests/protocol/authz-vectors/         contract-infra-authz vectors/decision (core checks, P6.2)
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PROTO_TAG=${BE_PROTOCOL_TAG:-v1.0.0-rc.1}
AUTHZ_TAG=${AUTHZ_TAG:-v2.0.0-rc.1}
PROTO_REPO=${BE_PROTOCOL_REPO:-https://github.com/brickKit/be-protocol}
AUTHZ_REPO=${AUTHZ_REPO:-https://github.com/brickKit/contract-infra-authz}
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

fetch() { # repo tag dest; a relative repo path is relative to this repository's root
  (cd "$ROOT" && git -c advice.detachedHead=false clone -q --depth 1 --branch "$2" "$1" "$3")
}
fetch "$PROTO_REPO" "$PROTO_TAG" "$TMP/p"
fetch "$AUTHZ_REPO" "$AUTHZ_TAG" "$TMP/a"

(cd "$TMP/p/vectors" && sha256sum -c --quiet SHA256SUMS)
(cd "$TMP/a/vectors" && sha256sum -c --quiet SHA256SUMS)

D="$ROOT/besdk/_protocol"
mkdir -p "$D/ddl"
cp "$TMP"/p/ddl/[0-9]*.sql "$D/ddl/"
cp "$TMP/p/schemas/errors-be.yaml" "$TMP/p/schemas/config-keys.yaml" "$D/"
echo "$PROTO_TAG" > "$D/VERSION"

V="$ROOT/tests/protocol"
rm -rf "$V/vectors" "$V/authz-vectors"
mkdir -p "$V/vectors" "$V/authz-vectors"
for area in config errors envelope redaction money idempotency calendar numbering; do
  mkdir -p "$V/vectors/$area"
  cp "$TMP"/p/vectors/$area/*.json "$V/vectors/$area/"
done
cp "$TMP/p/vectors/SHA256SUMS" "$V/vectors/"
cp -r "$TMP/a/vectors/decision" "$V/authz-vectors/"
cp "$TMP/a/vectors/SHA256SUMS" "$V/authz-vectors/"
echo "$PROTO_TAG $AUTHZ_TAG" > "$V/VERSION"
(cd "$V/vectors" && sha256sum -c --quiet --ignore-missing SHA256SUMS)
echo "synced be-protocol $PROTO_TAG and contract-infra-authz $AUTHZ_TAG"
