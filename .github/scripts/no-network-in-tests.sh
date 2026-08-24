#!/usr/bin/env bash
#
# THE ADAPTER TESTS ARE FIXTURE-BACKED AND MUST STAY THAT WAY.
#
# This is a ratchet, not a proof. It cannot observe a socket being opened; it
# refuses the CONSTRUCTORS that would be needed to open one, anywhere the test
# harness can reach. The property was verified by inspection when this landed
# (2026-08-24): the whole suite is fixture-backed, `chopsticks` and `subxt` are
# reached only from production paths (`fork_run.rs`, `source.rs`, `main.rs`,
# none of which contain a `#[cfg(test)]` module at all), and the single
# integration test file needs Postgres and nothing else. What this script buys
# is that the property cannot regress QUIETLY — a test that reaches an RPC
# would otherwise pass locally, pass in CI, and fail only when the endpoint is
# down or the chain has moved.
#
# Deliberately NOT a runtime network block: the only reliable way to enforce
# that on a hosted runner (iptables/unshare) also has to make an exception for
# the Postgres service container, and an exception-riddled firewall that breaks
# on a runner-image change is worse than a grep that always means the same
# thing.
#
# Two regions are scanned:
#   1. every file under a `tests/` directory (integration tests, in full)
#   2. every other .rs file FROM its first `#[cfg(test)]` to EOF
# (2) over-approximates on purpose — test modules sit at the end of a file by
# convention here, and a scan that reads too much fails loudly rather than
# silently letting something through.
set -uo pipefail

cd "$(dirname "$0")/../.." || exit 2

# Connection and process constructors. URLs as DATA are fine and are not
# matched: `pg_integration.rs` legitimately sets a registry `source` field to
# "wss://some-endpoint" without ever dialling it.
PAT='OnlineClient|RpcClient|LegacyRpcMethods|jsonrpsee|reqwest::|TcpStream::connect|UnixStream::connect|Command::new|tokio::process'

hits=0

while IFS= read -r f; do
  out=$(grep -nE "$PAT" "$f" || true)
  if [ -n "$out" ]; then
    echo "network/process construction in integration test $f:"
    echo "$out"
    hits=1
  fi
done < <(find crates -path '*/tests/*' -name '*.rs' | sort)

while IFS= read -r f; do
  start=$(grep -n '#\[cfg(test)\]' "$f" | head -1 | cut -d: -f1 || true)
  [ -z "${start:-}" ] && continue
  out=$(awk -v s="$start" -v pat="$PAT" 'NR>=s && $0 ~ pat { printf "  %d: %s\n", NR, $0 }' "$f")
  if [ -n "$out" ]; then
    echo "network/process construction in the test region of $f (from line $start):"
    echo "$out"
    hits=1
  fi
done < <(find crates -name '*.rs' -not -path '*/tests/*' | sort)

if [ "$hits" -ne 0 ]; then
  echo
  echo "FAIL: a test would reach outside the process."
  echo "If this is deliberate, it does not belong in the default suite — gate it"
  echo "behind #[ignore] and give it its own job, or capture a fixture instead."
  exit 1
fi

echo "OK: no network or process construction reachable from test code"
