#!/usr/bin/env bash
# Refresh the RDS root certificate bundle shipped with the plugin from the bundle AWS publishes.
#   rds-ca-bundle.sh          replace the resource when the published bundle changed
#   rds-ca-bundle.sh check    only report whether the resource is current (exit 1 when it is not)
# AWS adds root certificates when it opens a region; the existing ones are valid until 2061 or later.
set -euo pipefail

DIR=$(cd "$(dirname "$0")" && pwd)
RESOURCE="$DIR/../src/main/resources/org/mariadb/jdbc/plugin/credential/aws/rds-global-bundle.pem"
URL=https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem

TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT
curl -sSf -o "$TMP" "$URL"

# the download must be a bundle of Amazon RDS certificate authorities, all of them parseable
subjects() { openssl crl2pkcs7 -nocrl -certfile "$1" | openssl pkcs7 -print_certs -noout | grep '^subject=' | sort; }
count=$(grep -c "BEGIN CERTIFICATE" "$TMP")
rds=$(subjects "$TMP" | grep -c "Amazon RDS")
if [ "$count" -lt 100 ] || [ "$rds" -ne "$count" ]; then
  echo "unexpected content from $URL: $count certificates, $rds of them Amazon RDS authorities" >&2
  exit 2
fi

if cmp -s "$TMP" "$RESOURCE"; then
  echo "bundle current: $count certificate authorities"
  exit 0
fi

echo "published bundle differs from $RESOURCE:"
diff <(subjects "$RESOURCE") <(subjects "$TMP") | grep '^[<>]' | sed 's/^</  removed:/; s/^>/  added:  /' || true
if [ "${1:-}" = "check" ]; then
  exit 1
fi
cp "$TMP" "$RESOURCE"
echo "updated: $count certificate authorities"
