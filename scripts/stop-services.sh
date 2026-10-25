#!/usr/bin/env bash
# Stops every `go run ./cmd/*` service started by hand in a terminal
# (api/indexer/orders/notifications/inventory/payments-sim/saga-coordinator).
#
# `go run` spawns a separately-named compiled child binary
# (.../go-build.../exe/<name> or .../go-build/.../-d/<name>) distinct from
# the "go run ./cmd/<name>" wrapper process; `pkill -f "go run ./cmd/X"`
# only matches the wrapper and leaves the child - the one actually
# binding ports and holding Kafka consumer-group membership - running.
# Verified live while debugging "address already in use" on a restart
# attempt. This script matches both forms for every binary name under
# cmd/, so a restart never has to fight a stray process left over from an
# earlier session.
set -euo pipefail

cd "$(dirname "$0")/.."

# cmd/replay is a one-shot CLI tool, not a long-running service - excluded
# so this script can never kill someone's in-progress replay run.
names=$(ls cmd | grep -v '^replay$')
pattern=$(echo "$names" | paste -sd'|' -)

echo "Looking for processes matching: $pattern"
matches="$(ps aux | grep -E "go run \./cmd/($pattern)|/(exe|go-build.*-d)/($pattern)\$" | grep -v grep || true)"

if [ -z "$matches" ]; then
  echo "Nothing running."
  exit 0
fi

echo "$matches"
pids=$(echo "$matches" | awk '{print $2}')
echo "Killing PIDs: $(echo "$pids" | tr '\n' ' ')"
echo "$pids" | xargs kill -9

sleep 1
remaining="$(ps aux | grep -E "go run \./cmd/($pattern)|/(exe|go-build.*-d)/($pattern)\$" | grep -v grep || true)"
if [ -n "$remaining" ]; then
  echo "Still running (may need a manual kill -9):"
  echo "$remaining"
  exit 1
fi
echo "All stopped."
