#!/usr/bin/env bash
# Upload test objects with curl's built-in AWS SigV4 signing and check the answers.
#   ES_URL=https://<es>:443 ACCESS_KEY=AKIA... SECRET_KEY=... ./scripts/test-s3.sh
# ACCESS_KEY/SECRET_KEY: an entry in s3.access_keys. BUCKET must be allowed by s3.buckets.
set -uo pipefail
: "${ES_URL:?set ES_URL}" "${ACCESS_KEY:?set ACCESS_KEY}" "${SECRET_KEY:?set SECRET_KEY}"
BUCKET=${BUCKET:-logs}
REGION=${REGION:-us-east-1}
dir=$(mktemp -d); trap 'rm -rf "$dir"' EXIT
fails=0

# put <access key> <secret> <file> <key>: prints the HTTP status
put() {
  curl -s -o "$dir/body" -w '%{http_code}' --aws-sigv4 "aws:amz:$REGION:s3" --user "$1:$2" \
    -H "x-amz-content-sha256: $(sha256sum "$3" | cut -d' ' -f1)" -T "$3" "$ES_URL/$BUCKET/$4"
}
check() {  # check <name> <want> <got>
  if [ "$2" = "$3" ]; then echo "ok    $1 ($3)"; else echo "FAIL  $1: want $2, got $3 $(head -c 300 "$dir/body")"; fails=$((fails + 1)); fi
}

now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
cat > "$dir/lines.ndjson" <<JSON
{"@timestamp":"$now","message":"hello from test-s3.sh","host":{"name":"$(hostname)"}}
{"@timestamp":"$now","message":"second line","user":"ann","action":"login"}
JSON
gzip -k "$dir/lines.ndjson"
printf '[{"@timestamp":"%s","message":"from a JSON array"}]' "$now" > "$dir/array.json"

check "ndjson"              200 "$(put "$ACCESS_KEY" "$SECRET_KEY" "$dir/lines.ndjson"    "test-s3/lines-$now.ndjson")"
check "ndjson, gzip"        200 "$(put "$ACCESS_KEY" "$SECRET_KEY" "$dir/lines.ndjson.gz" "test-s3/lines-$now.ndjson.gz")"
check "JSON array"          200 "$(put "$ACCESS_KEY" "$SECRET_KEY" "$dir/array.json"      "test-s3/array-$now.json")"
check "wrong secret"        403 "$(put "$ACCESS_KEY" wrong-secret  "$dir/lines.ndjson"    "test-s3/bad.ndjson")"
check "unknown access key"  403 "$(put AKIAUNKNOWN0000000 "$SECRET_KEY" "$dir/lines.ndjson" "test-s3/bad.ndjson")"

echo
echo "5 documents should now be in Elasticsearch:"
echo '  FROM logs-* | WHERE s3.key LIKE "test-s3/*" | KEEP @timestamp, message, s3.key, s3.line'
exit $fails
