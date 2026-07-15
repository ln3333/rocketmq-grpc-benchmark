#!/usr/bin/env bash
set -euo pipefail

ROOT="$(CDPATH= cd -- "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mvn -f "$ROOT/pom.xml" -DskipTests clean package
java -ea -cp "$ROOT/target/test-classes:$ROOT/target/classes" \
  org.apache.rocketmq.grpc.benchmark.BenchmarkSelfTest

for script in producer push-consumer simple-consumer; do
  "$ROOT/bin/$script.sh" --help >/dev/null
done

if "$ROOT/bin/producer.sh" --topic test 2>/dev/null; then
  echo "Expected missing endpoint validation to fail" >&2
  exit 1
fi
if "$ROOT/bin/producer.sh" --endpoints localhost:8081 --topic test \
    --topic-type tx --send-mode async 2>/dev/null; then
  echo "Expected tx+async validation to fail" >&2
  exit 1
fi
echo "Smoke tests passed"
