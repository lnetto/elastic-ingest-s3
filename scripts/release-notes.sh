#!/usr/bin/env bash
# Print release notes for tag $1, with a table of the Elasticsearch versions it has zips for.
# Needs gh, authenticated.
set -euo pipefail
TAG="$1"
REPO="${GH_REPO:-$(gh repo view --json nameWithOwner --jq .nameWithOwner)}"
PLUGIN_VERSION="${TAG#v}"

cat <<NOTES
Download the zip whose \`-esX.Y.Z\` suffix matches your Elasticsearch version exactly; a plugin zip installs only on the version it was built for. Zips for new Elasticsearch releases are added here automatically. See the [README](https://github.com/${REPO}#install) to install.

| Elasticsearch | Download | Added |
|---|---|---|
NOTES
gh release view "$TAG" -R "$REPO" --json assets \
  --jq '.assets[] | [.name, .url, .createdAt[0:10]] | @tsv' \
  | sed -n "s/^ingest-s3-${PLUGIN_VERSION}-es\([^	]*\)\.zip	/\1	&/p" \
  | sort -V -r \
  | while IFS=$'\t' read -r es name url added; do
      echo "| ${es} | [${name}](${url}) | ${added} |"
    done
