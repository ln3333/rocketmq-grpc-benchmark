#!/usr/bin/env bash
set -euo pipefail

ROOT="$(CDPATH= cd -- "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR="$ROOT/target/rocketmq-grpc-benchmark-all.jar"
if [[ ! -f "$JAR" ]]; then
  echo "Missing $JAR; run bin/build.sh first." >&2
  exit 1
fi

if [[ -n "${JAVA_OPTS:-}" ]]; then
  JAVA_ARGS=()
  read -r -a JAVA_ARGS <<< "$JAVA_OPTS"
  exec java "${JAVA_ARGS[@]}" -jar "$JAR" "$@"
fi
exec java -jar "$JAR" "$@"
