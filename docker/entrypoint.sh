#!/bin/bash
# Put S3_API_KEY in the Elasticsearch keystore, then hand off to the stock entrypoint.
set -e
[ -f config/elasticsearch.keystore ] || bin/elasticsearch-keystore create
[ -z "$S3_API_KEY" ] || printf '%s' "$S3_API_KEY" | bin/elasticsearch-keystore add -x -f s3.api_key
exec /bin/tini -- /usr/local/bin/docker-entrypoint.sh "$@"
