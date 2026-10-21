#!/usr/bin/env bash
# Compare the plugin (S3 PutObject of ndjson) with Elasticsearch's _bulk API on the same lines.
#   ES_URL=http://localhost:9200 ES_AUTH=elastic:<password> ACCESS_KEY=AKIA... SECRET_KEY=... ./scripts/benchmark.sh
# ES_AUTH: a user that can write, refresh and delete logs-bench* (for _bulk and the cleanup).
# ACCESS_KEY/SECRET_KEY: an entry in s3.access_keys; s3.target must be at its default and
# no default pipeline on the target (logs-s3@custom unset). Each run indexes OBJECTS x LINES lines at each CONCURRENCY, RUNS times (median).
# ONLY=<text> runs only the paths whose name contains it.
# s3.metadata (full or minimal) is used as it is set on the cluster.
set -euo pipefail
: "${ES_URL:?set ES_URL}" "${ES_AUTH:?set ES_AUTH}" "${ACCESS_KEY:?set ACCESS_KEY}" "${SECRET_KEY:?set SECRET_KEY}"
OBJECTS=${OBJECTS:-40} LINES=${LINES:-5000} RUNS=${RUNS:-3} CONCURRENCIES=${CONCURRENCIES:-"1 4 8"}
BUCKET=${BUCKET:-bench}
dir=$(mktemp -d); trap 'rm -rf "$dir"' EXIT
export ES_URL ES_AUTH ACCESS_KEY SECRET_KEY BUCKET dir

# The same JSON log lines as an ndjson object, and as _bulk create actions: carrying exactly the document
# the plugin builds from each line (the line in message, its ids and s3.* metadata), the line with
# the plugin's ids, or only the line.
python3 - "$dir" "$OBJECTS" "$LINES" <<'PY'
import base64, hashlib, json, random, sys, time
d, objects, lines = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
rnd = random.Random(42)
now = int(time.time())
msgs = [
    lambda: f"Built inbound TCP connection {rnd.randint(1, 10**9)} for outside:{rnd.randint(1,223)}.{rnd.randint(0,255)}.{rnd.randint(0,255)}.{rnd.randint(1,254)}/{rnd.randint(1024,65535)} to inside:10.0.{rnd.randint(0,255)}.{rnd.randint(1,254)}/443",
    lambda: f"Accepted publickey for user{rnd.randint(1,500)} from 10.1.{rnd.randint(0,255)}.{rnd.randint(1,254)} port {rnd.randint(1024,65535)} ssh2",
    lambda: f'GET /api/v1/items/{rnd.randint(1,10**6)} HTTP/1.1 {rnd.choice([200,200,200,404,500])} {rnd.randint(100,50000)} bytes',
]
for o in range(objects):
    with open(f"{d}/obj{o}.ndjson", "w") as s3, open(f"{d}/obj{o}.bulk", "w") as bulk, open(f"{d}/obj{o}.bulkid", "w") as bulkid, \
         open(f"{d}/obj{o}.bulkfull", "w") as full:
        received = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
        meta = {"bucket": "bench", "key": f"bench/r1/obj{o}.ndjson.zst", "etag": "0123456789abcdef0123456789abcdef",
                "compression": "zstd", "format": "lines",
                "access_key_name": "bench", "access_key_id": "AKIA0123456789ABCDEF",
                "request": {"id": "41a02388-80a7-4368-99a9-5436bc127907", "received": received, "node": "node-1",
                            "remote_address": "172.17.0.1",
                            "user_agent": "curl/8.18.0", "bytes": 147000}}
        for e in range(lines):
            t = now - rnd.randint(0, 3600) + rnd.random()
            line = json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + f".{int((t % 1) * 1e6):06d}Z",
                               "host": f"web{rnd.randint(1,20)}", "service": rnd.choice(["fw", "sshd", "api"]),
                               "level": rnd.choice(["info", "info", "warn"]), "msg": rnd.choice(msgs)()})
            s3.write(line + "\n")
            bulk.write('{"create":{}}\n' + json.dumps({"@timestamp": received, "message": line}) + "\n")
            create = json.dumps({"create": {"_id": base64.urlsafe_b64encode(hashlib.sha256(f"obj{o}#{e}".encode()).digest()[:15]).decode()}})
            bulkid.write(create + "\n" + json.dumps({"@timestamp": received, "message": line}) + "\n")
            full.write(create + "\n" + json.dumps({"@timestamp": received, "message": line, "event": {"module": "s3", "ingested": received},
                       "agent": {"type": "curl", "version": "8.5.0"}, "s3": dict(meta, line=e + 1)}) + "\n")
PY
for f in "$dir"/obj*.ndjson; do zstd -q "$f" -o "$f.zst"; done
for f in "$dir"/obj*.bulk "$dir"/obj*.bulkid "$dir"/obj*.bulkfull; do gzip -k "$f"; done

# curl waits for "100 Continue" before sending a body over 1 MB; "Expect:" turns that off for every path alike.
# put_one <n> <ext> <run id>: one S3 object; key unique per run, or the plugin would skip it as a retry
put_one() {
  local f="$dir/obj$1.ndjson$2"
  curl -sf -o /dev/null -H "Expect:" --aws-sigv4 "aws:amz:us-east-1:s3" --user "$ACCESS_KEY:$SECRET_KEY" \
    -H "x-amz-content-sha256: $(sha256sum "$f" | cut -d' ' -f1)" -T "$f" "$ES_URL/$BUCKET/bench/$3/obj$1.ndjson$2"
}
# bulk_one <n> <ext> <run id>: one _bulk request (ext: .gz, id.gz or full.gz); fails if any item failed
bulk_one() {
  curl -sf -H "Expect:" -u "$ES_AUTH" -H "Content-Type: application/x-ndjson" -H "Content-Encoding: gzip" \
    --data-binary "@$dir/obj$1.bulk$2" "$ES_URL/logs-benchbulk-default/_bulk?filter_path=errors" | grep -q '"errors":false'
}
export -f put_one bulk_one

es() { curl -sf -u "$ES_AUTH" -H "Content-Type: application/json" "$@"; }
count() { es -XPOST "$ES_URL/$1/_refresh" >/dev/null; es "$ES_URL/$1/_count" | python3 -c 'import json,sys;print(json.load(sys.stdin)["count"])'; }
bytes() { cat "$dir"/obj*"$1" | wc -c; }

total=$((OBJECTS * LINES))
printf '%-26s %5s %9s %9s %10s\n' "path" "conc" "sent MB" "seconds" "lines/s"
for c in $CONCURRENCIES; do
  for path in "plugin none:put_one::logs-s3.generic-default:.ndjson" "plugin zstd:put_one:.zst:logs-s3.generic-default:.ndjson.zst" \
              "_bulk gzip, plugin's docs:bulk_one:full.gz:logs-benchbulk-default:.bulkfull.gz" \
              "_bulk gzip, line + ids:bulk_one:id.gz:logs-benchbulk-default:.bulkid.gz" \
              "_bulk gzip, line only:bulk_one:.gz:logs-benchbulk-default:.bulk.gz"; do
    IFS=: read -r name fn ext ds files <<<"$path"
    [ -z "${ONLY:-}" ] || [[ "$name" == *"$ONLY"* ]] || continue
    times=()
    for r in $(seq "$RUNS"); do
      es -XDELETE "$ES_URL/_data_stream/$ds" >/dev/null 2>&1 || true
      id="r$(date +%s%N)"
      start=$(date +%s.%N)
      seq 0 $((OBJECTS - 1)) | xargs -P "$c" -I{} bash -c "$fn {} '$ext' $id"
      end=$(date +%s.%N)
      n=$(count "$ds")
      [ "$n" = "$total" ] || { echo "$name: indexed $n of $total lines" >&2; exit 1; }
      times+=("$(awk "BEGIN{print $end - $start}")")
    done
    median=$(printf '%s\n' "${times[@]}" | sort -n | sed -n "$(( (RUNS + 1) / 2 ))p")
    awk -v n="$name" -v c="$c" -v b="$(bytes "$files")" -v t="$median" -v e="$total" \
      'BEGIN{printf "%-26s %5s %9.1f %9.2f %10.0f\n", n, c, b / 1048576, t, e / t}'
  done
done
es -XDELETE "$ES_URL/_data_stream/logs-benchbulk-default" >/dev/null 2>&1 || true
