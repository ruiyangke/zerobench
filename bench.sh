#!/usr/bin/env bash
set -euo pipefail

# NUMA-isolated benchmark: stub on node1, zerobench on node0.
# Requires: nix (for numactl), cargo build --release already done.

STUB=./target/release/zerobench-stub
BENCH=./target/release/zerobench
URL="http://127.0.0.1:8080"
DURATION=10s
RUNS=5

NUMA="nix run nixpkgs#numactl --"

echo "=== Building release ==="
cargo build --release --features mio-h1,mio-h2,sse,ws,tui 2>&1 | tail -1
cargo build --release -p zerobench-stub 2>&1 | tail -1

echo ""
echo "=== NUMA topology ==="
$NUMA --hardware 2>&1 | grep -E "node [0-9]|available"

echo ""
echo "=== Starting stub on NUMA node 1 (CPUs 8-15,24-31) ==="
$NUMA --cpunodebind=1 --membind=1 $STUB --workers 8 &
STUB_PID=$!
sleep 1

cleanup() {
    kill $STUB_PID 2>/dev/null
    wait $STUB_PID 2>/dev/null
}
trap cleanup EXIT

echo ""
echo "=== H1: 1 thread, 50 conns (single-thread client perf) ==="
for i in $(seq 1 $RUNS); do
    printf "  Run %d: " "$i"
    $NUMA --cpunodebind=0 --membind=0 $BENCH $URL -d $DURATION -c 50 -t 1 2>&1 | grep "throughput"
done

echo ""
echo "=== H1: 8 threads, 300 conns ==="
for i in $(seq 1 $RUNS); do
    printf "  Run %d: " "$i"
    $NUMA --cpunodebind=0 --membind=0 $BENCH $URL -d $DURATION -c 300 -t 8 2>&1 | grep "throughput"
done

echo ""
echo "=== wrk: 1 thread, 50 conns ==="
for i in $(seq 1 3); do
    printf "  Run %d: " "$i"
    $NUMA --cpunodebind=0 --membind=0 wrk -t 1 -c 50 -d $DURATION $URL/ 2>&1 | grep "Requests/sec"
done

echo ""
echo "=== wrk: 8 threads, 300 conns ==="
for i in $(seq 1 3); do
    printf "  Run %d: " "$i"
    $NUMA --cpunodebind=0 --membind=0 wrk -t 8 -c 300 -d $DURATION $URL/ 2>&1 | grep "Requests/sec"
done

echo ""
echo "Done."
