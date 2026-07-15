#!/usr/bin/env bash
set -euo pipefail

ROOT="$(CDPATH= cd -- "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec mvn -f "$ROOT/pom.xml" -DskipTests clean package "$@"
