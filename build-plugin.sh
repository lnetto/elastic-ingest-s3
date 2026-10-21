#!/usr/bin/env bash
# Build and test the plugin in a JDK 21 Gradle container (only Docker needed); the zip lands in dist/.
# Target another Elasticsearch version with: ES_VERSION=9.5.5 ./build-plugin.sh
set -euo pipefail
cd "$(dirname "$0")"
# gradle.properties has the default version; ES_VERSION overrides it for this build only
version_arg=()
[ -z "${ES_VERSION:-}" ] || version_arg=("-PesVersion=${ES_VERSION}")
rm -rf plugin/build/distributions
docker volume create ingest-s3-gradle-cache >/dev/null
# a fresh named volume is root-owned; hand it to the calling user
docker run --rm -v ingest-s3-gradle-cache:/cache --entrypoint chown gradle:8-jdk21 "$(id -u):$(id -g)" /cache
docker run --rm -u "$(id -u):$(id -g)" -e GRADLE_USER_HOME=/cache \
  -v "$PWD":/work -v ingest-s3-gradle-cache:/cache -w /work/plugin \
  gradle:8-jdk21 gradle --no-daemon -q "${version_arg[@]}" "${@:-test}" bundlePlugin
mkdir -p dist && rm -f dist/*.zip
cp plugin/build/distributions/*.zip dist/
ls -l dist/*.zip
