#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KACRAB_BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${KACRAB_BENCH_TOPIC:-kacrab-bench}"
KAFKA_BIN="${KAFKA_BIN:-$HOME/.local/share/kacrab-kafka/current/bin}"
PARTITIONS="${KACRAB_PARTITIONS:-3}"

RUST_BENCH=(cargo run -p kacrab-benches --release --bin producer_kafka_bench)
JAVA_PERF="$KAFKA_BIN/kafka-producer-perf-test.sh"
KAFKA_TOPICS="$KAFKA_BIN/kafka-topics.sh"

ensure_topic() {
  if [[ ! -x "$KAFKA_TOPICS" ]]; then
    echo "skip topic ensure: kafka-topics.sh not found at $KAFKA_TOPICS"
    return
  fi
  "$KAFKA_TOPICS" \
    --bootstrap-server "$BOOTSTRAP" \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor 1 >/dev/null
  "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"
}

run_rust() {
  local label="$1"
  shift
  echo
  echo "===== rust: $label ====="
  env KACRAB_BOOTSTRAP="$BOOTSTRAP" KACRAB_BENCH_TOPIC="$TOPIC" "$@" "${RUST_BENCH[@]}"
}

run_java() {
  local label="$1"
  local records="$2"
  local size="$3"
  shift 3
  if [[ ! -x "$JAVA_PERF" ]]; then
    echo "skip java $label: kafka-producer-perf-test.sh not found at $JAVA_PERF"
    return
  fi
  echo
  echo "===== java: $label ====="
  "$JAVA_PERF" \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$TOPIC" \
    --num-records "$records" \
    --record-size "$size" \
    --throughput -1 \
    --command-property "client.id=java-$label" "$@"
}

ensure_topic

run_java "default-10b" 5000000 10
run_java "default-10kib" 100000 10240
run_java "batch1m-10b" 5000000 10 batch.size=1048576
run_java "batch1m-10kib" 100000 10240 batch.size=1048576

run_rust "kafka-default" 
run_rust "kafka-default-inflight-1" KACRAB_IN_FLIGHT=1
run_rust "kafka-default-batch-64k" KACRAB_BATCH_SIZE=65536
run_rust "kafka-default-batch-256k" KACRAB_BATCH_SIZE=262144
run_rust "kafka-default-batch-1m" KACRAB_BATCH_SIZE=1048576
run_rust "relaxed" KACRAB_BENCH_PROFILE=relaxed
