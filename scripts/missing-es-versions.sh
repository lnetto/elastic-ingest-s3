#!/usr/bin/env bash
# Print, as a JSON array, the Elasticsearch releases (>= minEsVersion) that the GitHub release
# for the current pluginVersion has no zip for yet. Needs gh, authenticated.
set -euo pipefail
cd "$(dirname "$0")/.."
prop() { sed -n "s/^$1=//p" plugin/gradle.properties; }
PLUGIN_VERSION=$(prop pluginVersion)
MIN=$(prop minEsVersion)

released=$(gh api --paginate 'repos/elastic/elasticsearch/releases?per_page=100' \
  --jq '.[] | select(.prerelease | not) | .tag_name' | sed -n 's/^v\([0-9]*\.[0-9]*\.[0-9]*\)$/\1/p')
# already published, e.g. ingest-s3-0.8.0-es9.5.4.zip → 9.5.4
built=$(gh release view "v${PLUGIN_VERSION}" --json assets --jq '.assets[].name' 2>/dev/null \
  | sed -n "s/^ingest-s3-${PLUGIN_VERSION}-es\(.*\)\.zip$/\1/p" || true)

for v in $released; do
  # keep v >= MIN
  [ "$(printf '%s\n%s\n' "$MIN" "$v" | sort -V | head -1)" = "$MIN" ] || continue
  grep -qxF "$v" <<<"$built" || echo "$v"
done | sort -V | jq -Rnc '[inputs]'
