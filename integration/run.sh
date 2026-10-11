#!/bin/sh
# Testcontainers/Ceph dependencies live in a development-only nested module.
# Existing CEPH_MSGR_TEST_* profile inputs are interpreted by its Go runner.
set -eu
project_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
export GOWORK=off
cd "$project_root/integration/fixture"
exec go run . run -project "$project_root"
