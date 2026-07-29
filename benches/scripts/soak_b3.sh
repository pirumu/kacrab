#!/bin/bash
# soak_b3.sh — the B3 memory soak (issue #52): a long soak_bench run against
# the 3-broker cluster with chaos churn, sampling client RSS every 10 s, plus
# an automatic RSS-trend verdict at the end.
#
# Usage:
#   benches/scripts/soak_b3.sh [hours]        # default 12; 8-24 is the B3 range
#   KEEP_CLUSTER=1 benches/scripts/soak_b3.sh # leave brokers up afterwards
#
# What it does:
#   1. Preflight: docker reachable, >= 6 GiB VM memory, >= 20 GB free disk,
#      no soak already running.
#   2. Brings up docker-compose.cluster.yml on ports 29092/29094/29096 and
#      waits for health; caps the soak topic's retention (1 h / 1 GiB per
#      partition) right after the harness creates it so disk stays bounded.
#   3. Runs soak_bench: 1,000 rec/s x 512 B over 6 partitions, idempotent
#      acks=all, 2-consumer group, broker-kill rotation every 10 min (45 s)
#      on kafka2/kafka3, consumer bounce every 15 min — the churn that
#      exercises pooled buffers, accumulator reuse, and per-topic sensors.
#   4. Post-run: an RSS verdict from soak.csv. Chaos windows spike RSS
#      transiently (producer buffering through broker kills), so a linear
#      slope misreads fault phases as growth; the verdict instead compares
#      the MEDIAN RSS of the final 10% of the run against the median of the
#      post-warm-up 25-35% window. PASS = final median <= 1.3x the baseline
#      median. Anything else prints INVESTIGATE — read SOAK-REPORT.md for
#      the analysis playbook, and keep the out dir (it is gitignored).
set -euo pipefail
cd "$(dirname "$0")/../.."

HOURS="${1:-12}"
DURATION=$(( HOURS * 3600 ))
STAMP="$(date +%Y%m%d-%H%M)"
OUT="benches/soak-out-b3-${STAMP}"
BOOTSTRAP="127.0.0.1:29092,127.0.0.1:29094,127.0.0.1:29096"

echo "== B3 memory soak: ${HOURS}h -> ${OUT}"

# --- preflight -------------------------------------------------------------
docker info >/dev/null 2>&1 || { echo "FATAL: docker daemon unreachable"; exit 1; }
mem_gib=$(docker info --format '{{.MemTotal}}' | awk '{printf "%d", $1/1073741824}')
[ "$mem_gib" -ge 6 ] || { echo "FATAL: docker VM has ${mem_gib}GiB, needs >= 6 (colima stop; colima start --memory 8)"; exit 1; }
free_gb=$(df -g . | tail -1 | awk '{print $4}')
[ "$free_gb" -ge 20 ] || { echo "FATAL: ${free_gb}GB free disk, needs >= 20"; exit 1; }
pgrep -f 'release/soak_bench' >/dev/null && { echo "FATAL: a soak_bench is already running"; exit 1; }

# --- cluster ---------------------------------------------------------------
KAFKA1_HOST_PORT=29092 KAFKA2_HOST_PORT=29094 KAFKA3_HOST_PORT=29096 \
  docker compose -f docker-compose.cluster.yml up -d --wait kafka1 kafka2 kafka3
for i in $(seq 1 60); do
  docker exec kacrab-kafka1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break
  [ "$i" = 60 ] && { echo "FATAL: cluster never became admin-ready"; exit 1; }
  sleep 2
done
echo "== cluster admin-ready"

cleanup() {
  if [ "${KEEP_CLUSTER:-0}" != "1" ]; then
    KAFKA1_HOST_PORT=29092 KAFKA2_HOST_PORT=29094 KAFKA3_HOST_PORT=29096 \
      docker compose -f docker-compose.cluster.yml down -v || true
  fi
}
trap cleanup EXIT

# --- run -------------------------------------------------------------------
cargo build -p kacrab-benches --bin soak_bench --release
mkdir -p "$OUT"
KACRAB_SOAK_BOOTSTRAP="$BOOTSTRAP" \
KACRAB_SOAK_DURATION_SECS="$DURATION" \
KACRAB_SOAK_RATE=1000 \
KACRAB_SOAK_CHAOS_INTERVAL_SECS=600 \
KACRAB_SOAK_CHAOS_DOWNTIME_SECS=45 \
KACRAB_SOAK_CHAOS_CONTAINERS="kacrab-kafka2,kacrab-kafka3" \
KACRAB_SOAK_CONSUMER_BOUNCE_SECS=900 \
KACRAB_SOAK_OUT_DIR="$OUT" \
  ./target/release/soak_bench > "$OUT/soak.log" 2>&1 &
SOAK_PID=$!
# Cap the topic's disk the moment the harness has created it.
sleep 20
docker exec kacrab-kafka1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 --alter --entity-type topics \
  --entity-name kacrab-soak \
  --add-config 'retention.ms=3600000,retention.bytes=1073741824' || true
echo "== soak running (pid ${SOAK_PID}); tail -f ${OUT}/soak.log to watch"
wait "$SOAK_PID"

# --- RSS verdict -----------------------------------------------------------
python3 - "$OUT/soak.csv" << 'PYEOF'
import csv, sys
from statistics import median
rows = list(csv.DictReader(open(sys.argv[1])))
rss = [float(r["rss_mib"]) for r in rows]
n = len(rss)
baseline = median(rss[n * 25 // 100 : n * 35 // 100])
final = median(rss[n * 90 // 100 :])
print(f"RSS start={rss[0]:.1f} MiB  peak={max(rss):.1f}  end={rss[-1]:.1f}")
print(f"post-warm-up baseline median={baseline:.1f}  final-window median={final:.1f}")
ok = final <= baseline * 1.3
print("VERDICT:", "PASS — no unbounded growth" if ok
      else "INVESTIGATE — see SOAK-REPORT.md for the analysis playbook")
PYEOF
echo "== done; raw data in ${OUT} (gitignored). Update SOAK-REPORT.md with the outcome."
