#!/usr/bin/env bash
# Deterministically reproduce the `socket disconnected` poison loop
# (kafka-python 3.0.7 vs Redpanda v25.3.15).
#
# Boots a throwaway FRESH Redpanda that advertises 127.0.0.1:19094 — the
# address of the barrier relay run by wedge-repro.py — so every client
# connection flows through the relay. The relay releases all producers'
# InitProducerId frames simultaneously, guaranteeing the concurrent-arrival
# race that Redpanda answers with COORDINATOR_NOT_AVAILABLE, which sends
# kafka-python into its null-key FindCoordinator loop.
#
# Verifies both sides:
#   client: null-key FindCoordinator sends + ~10Hz disconnect loop
#   broker: "Asked to read a 0 byte flex string" disconnect log
#
# Usage: ./wedge-repro.sh              reproduce the bug (exit 0 = reproduced)
#        ./wedge-repro.sh --with-fix   same collision with the proposed
#                                      kafka-python fix patched in: producers
#                                      recover by plain retry, no loop
#        ./wedge-repro.sh --no-idempotence
#                                      config-only workaround demo: producers
#                                      run with enable_idempotence=False, so
#                                      no InitProducerId is sent and the loop
#                                      never starts
# See also: wedge-poison-direct.py (deterministic broker-side half only,
# works against any running Redpanda, no fresh boot needed).
set -euo pipefail
cd "$(dirname "$0")"

NAME=rp-wedge-repro
IMG=docker.redpanda.com/redpandadata/redpanda:v25.3.15

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "=== booting fresh Redpanda (advertising the relay address) ==="
cleanup
docker run -d --name "$NAME" -p 19093:19093 "$IMG" \
  redpanda start --mode=dev-container --smp=1 \
  --kafka-addr=external://0.0.0.0:19093 \
  --advertise-kafka-addr=external://127.0.0.1:19094 >/dev/null

until docker exec "$NAME" rpk cluster health 2>/dev/null | grep -qE 'Healthy:.+true'; do
  sleep 2
done
echo "broker healthy (id_allocator untouched); running probe"

if uv run python wedge-repro.py "$@"; then
  if [[ " $* " == *" --no-idempotence "* ]]; then
    echo
    echo "─── broker side: clean log with idempotency off ────────────────────────"
    N=$(docker logs "$NAME" 2>&1 | grep -c "0 byte flex string" || true)
    echo "  $N '0 byte flex string' parse-slams in the broker log (expected 0)."
    exit 0
  fi
  echo
  echo "─── STAGE 4 (broker side): Redpanda's own log of the slam ──────────────"
  echo "  Every null-key FindCoordinator made the broker throw while parsing"
  echo "  and close the connection (kafka layer, connection_context.cc):"
  docker logs "$NAME" 2>&1 | grep -m 3 "0 byte flex string" | sed 's/^/  /' || true
  N=$(docker logs "$NAME" 2>&1 | grep -c "0 byte flex string" || true)
  echo "  ...$N such parse-slams total — one per loop iteration."
  exit 0
fi

echo "NOT reproduced — unexpected; check wedge-repro.log"
exit 1
