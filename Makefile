.PHONY: help check build release run test test-bench-scripts test-codegen test-protocol \
        test-protocol-java test-protocol-java-matrix test-protocol-full \
        clippy fmt fmt-check doc doc-check clean audit deny udeps machete outdated \
        install-hooks commit-lint \
        kafka-start kafka-stop kafka-data-du kafka-topic-prune-delete-dirs \
        kafka-tools-check kafka-topic-describe kafka-topic-create \
        kafka-topic-delete kafka-topic-recreate bench-kafka-topic \
        bench-kafka bench-kafka-java-default \
        bench-kafka-consumer bench-kafka-consumer-java-default \
        ci ci-strict tools

KAFKA_BIN ?= $(HOME)/.local/share/kacrab-kafka/current/bin
KAFKA_TOPICS ?= $(KAFKA_BIN)/kafka-topics.sh
KAFKA_SERVER_START ?= $(KAFKA_BIN)/kafka-server-start.sh
KAFKA_SERVER_STOP ?= $(KAFKA_BIN)/kafka-server-stop.sh
KAFKA_PRODUCER_PERF ?= $(KAFKA_BIN)/kafka-producer-perf-test.sh
KAFKA_CONSUMER_PERF ?= $(KAFKA_BIN)/kafka-consumer-perf-test.sh
KAFKA_ROOT ?= $(abspath $(KAFKA_BIN)/../..)
KAFKA_SERVER_PROPERTIES ?= $(KAFKA_ROOT)/server.properties
KAFKA_DATA_DIR ?= $(KAFKA_ROOT)/data
KACRAB_BOOTSTRAP ?= 127.0.0.1:9092
KACRAB_BENCH_TOPIC ?= kacrab-bench
KACRAB_BENCH_API ?= per-record
KACRAB_PARTITIONS ?= 3
KACRAB_REPLICATION_FACTOR ?= 1

help:
	@echo "Common:"
	@echo "  check       - cargo check --all-targets"
	@echo "  build       - cargo build --all-targets"
	@echo "  release     - cargo build --release"
	@echo "  run         - cargo run"
	@echo "  test        - bench script tests + cargo test --workspace --all-features"
	@echo "  test-bench-scripts - python unit tests for benchmark helper scripts"
	@echo "  test-codegen - cargo test -p kacrab-codegen --all-features"
	@echo "  test-protocol - cargo test -p kacrab-protocol --all-features"
	@echo "  test-protocol-java - compile ignored Java interop tests"
	@echo "  test-protocol-java-matrix - run ignored Java oracle matrix"
	@echo "  clippy      - clippy with -D warnings"
	@echo "  fmt         - cargo +nightly fmt --all"
	@echo "  fmt-check   - cargo +nightly fmt --all -- --check"
	@echo "  install-hooks - use tracked git hooks from .githooks"
	@echo "  commit-lint - lint the latest commit message"
	@echo "  doc         - cargo doc --no-deps --open (with -D warnings)"
	@echo "  doc-check   - cargo doc --no-deps --all-features, no --open (CI rustdoc gate)"
	@echo "  clean       - cargo clean"
	@echo ""
	@echo "Strict (require extra tools — see 'make tools'):"
	@echo "  audit       - cargo audit (security advisories)"
	@echo "  deny        - cargo deny check (licenses, sources, bans, advisories)"
	@echo "  udeps       - cargo +nightly udeps (unused deps)"
	@echo "  machete     - cargo machete (unused deps, faster)"
	@echo "  outdated    - cargo outdated"
	@echo ""
	@echo "Pipelines:"
	@echo "  ci          - fmt-check + clippy + doc-check + test"
	@echo "  test-protocol-full - protocol tests + Java oracle matrix"
	@echo "  ci-strict   - ci + audit + deny + machete"
	@echo "  tools       - install all auxiliary cargo tools"
	@echo ""
	@echo "Kafka bench helpers:"
	@echo "  kafka-start           - start native Kafka daemon from KAFKA_SERVER_PROPERTIES"
	@echo "  kafka-stop            - stop native Kafka"
	@echo "  kafka-data-du         - show largest Kafka data dirs"
	@echo "  kafka-topic-describe  - describe KACRAB_BENCH_TOPIC on KACRAB_BOOTSTRAP"
	@echo "  kafka-topic-create    - create KACRAB_BENCH_TOPIC if missing"
	@echo "  kafka-topic-delete    - delete KACRAB_BENCH_TOPIC"
	@echo "  kafka-topic-recreate  - delete, wait, and recreate KACRAB_BENCH_TOPIC"
	@echo "  kafka-topic-prune-delete-dirs - rm stale KACRAB_BENCH_TOPIC *-delete dirs; stop Kafka first"
	@echo "  bench-kafka-topic     - alias for kafka-topic-create"
	@echo "  bench-kafka           - run Rust real-Kafka bench with fixed default scenarios"
	@echo "                          uses Java-style per-record public producer send"
	@echo "  bench-kafka-java-default - run Java default scenarios 5x with effective config snapshots"
	@echo "  bench-kafka-consumer  - run Rust real-Kafka consumer bench (ConsumerPerformance mirror)"
	@echo "                          KACRAB_BENCH_PREFILL=1 fills the topics on first use"
	@echo "  bench-kafka-consumer-java-default - run Java consumer default scenarios 5x"

check:
	cargo check --workspace --all-targets --all-features

build:
	cargo build --workspace --all-targets

release:
	cargo build --workspace --release

run:
	cargo run

test: test-bench-scripts
	cargo test --workspace --all-features

test-bench-scripts:
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest benches/scripts/test_producer_counter_metrics.py

test-codegen:
	cargo test -p kacrab-codegen --all-features

test-protocol:
	cargo test -p kacrab-protocol --all-features

test-protocol-java:
	cargo test -p kacrab-protocol --test java_interop

test-protocol-java-matrix:
	cargo test -p kacrab-protocol --test java_interop -- --ignored --nocapture

test-protocol-full: test-protocol test-protocol-java-matrix

clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

fmt:
	cargo +nightly fmt --all

fmt-check:
	cargo +nightly fmt --all -- --check

install-hooks:
	git config core.hooksPath .githooks

commit-lint:
	scripts/check-commit-message.sh --message "$$(git log -1 --format=%s)" HEAD

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --open

# CI-safe rustdoc gate: no --open, all features so the docs.rs surface
# (producer/consumer/admin + codecs) is what actually gets checked.
doc-check:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features

clean:
	cargo clean

kafka-tools-check:
	@test -x "$(KAFKA_TOPICS)" || { echo "missing kafka-topics.sh at $(KAFKA_TOPICS). Set KAFKA_BIN=/path/to/kafka/bin"; exit 1; }
	@test -x "$(KAFKA_PRODUCER_PERF)" || { echo "missing kafka-producer-perf-test.sh at $(KAFKA_PRODUCER_PERF). Set KAFKA_BIN=/path/to/kafka/bin"; exit 1; }

kafka-start: kafka-tools-check
	@test -x "$(KAFKA_SERVER_START)" || { echo "missing kafka-server-start.sh at $(KAFKA_SERVER_START). Set KAFKA_BIN=/path/to/kafka/bin"; exit 1; }
	@test -f "$(KAFKA_SERVER_PROPERTIES)" || { echo "missing Kafka server properties at $(KAFKA_SERVER_PROPERTIES). Set KAFKA_SERVER_PROPERTIES=/path/to/server.properties"; exit 1; }
	"$(KAFKA_SERVER_START)" -daemon "$(KAFKA_SERVER_PROPERTIES)"

kafka-stop: kafka-tools-check
	@test -x "$(KAFKA_SERVER_STOP)" || { echo "missing kafka-server-stop.sh at $(KAFKA_SERVER_STOP). Set KAFKA_BIN=/path/to/kafka/bin"; exit 1; }
	-"$(KAFKA_SERVER_STOP)"

kafka-data-du:
	@du -sh "$(KAFKA_DATA_DIR)"/* 2>/dev/null | sort -hr | head -40

kafka-topic-describe: kafka-tools-check
	"$(KAFKA_TOPICS)" --bootstrap-server "$(KACRAB_BOOTSTRAP)" --describe --topic "$(KACRAB_BENCH_TOPIC)"

kafka-topic-create: kafka-tools-check
	"$(KAFKA_TOPICS)" --bootstrap-server "$(KACRAB_BOOTSTRAP)" --create --if-not-exists --topic "$(KACRAB_BENCH_TOPIC)" --partitions "$(KACRAB_PARTITIONS)" --replication-factor "$(KACRAB_REPLICATION_FACTOR)"
	"$(KAFKA_TOPICS)" --bootstrap-server "$(KACRAB_BOOTSTRAP)" --describe --topic "$(KACRAB_BENCH_TOPIC)"

kafka-topic-delete: kafka-tools-check
	"$(KAFKA_TOPICS)" --bootstrap-server "$(KACRAB_BOOTSTRAP)" --delete --if-exists --topic "$(KACRAB_BENCH_TOPIC)"

kafka-topic-prune-delete-dirs:
	@echo "Pruning stale delete dirs for topic $(KACRAB_BENCH_TOPIC) under $(KAFKA_DATA_DIR)"
	@find "$(KAFKA_DATA_DIR)" -maxdepth 1 -type d -name "$(KACRAB_BENCH_TOPIC)-*.delete" -print -exec rm -rf {} +
	@find "$(KAFKA_DATA_DIR)" -maxdepth 1 -type d -name "$(KACRAB_BENCH_TOPIC)-*-delete" -print -exec rm -rf {} +

kafka-topic-recreate: kafka-tools-check
	"$(KAFKA_TOPICS)" --bootstrap-server "$(KACRAB_BOOTSTRAP)" --delete --if-exists --topic "$(KACRAB_BENCH_TOPIC)"
	@for attempt in $$(seq 1 60); do \
		if ! "$(KAFKA_TOPICS)" --bootstrap-server "$(KACRAB_BOOTSTRAP)" --describe --topic "$(KACRAB_BENCH_TOPIC)" >/dev/null 2>&1; then \
			break; \
		fi; \
		sleep 1; \
	done
	"$(KAFKA_TOPICS)" --bootstrap-server "$(KACRAB_BOOTSTRAP)" --create --if-not-exists --topic "$(KACRAB_BENCH_TOPIC)" --partitions "$(KACRAB_PARTITIONS)" --replication-factor "$(KACRAB_REPLICATION_FACTOR)"
	"$(KAFKA_TOPICS)" --bootstrap-server "$(KACRAB_BOOTSTRAP)" --describe --topic "$(KACRAB_BENCH_TOPIC)"

bench-kafka-topic: kafka-topic-create

bench-kafka: kafka-topic-create
	KACRAB_BOOTSTRAP="$(KACRAB_BOOTSTRAP)" KACRAB_BENCH_TOPIC="$(KACRAB_BENCH_TOPIC)" KACRAB_BENCH_API="$(KACRAB_BENCH_API)" cargo run -p kacrab-benches --bin producer_kafka_bench --release

bench-kafka-java-default: kafka-tools-check kafka-topic-create
	KAFKA_BIN="$(KAFKA_BIN)" KAFKA_ROOT="$(KAFKA_ROOT)" KAFKA_PRODUCER_PERF="$(KAFKA_PRODUCER_PERF)" KACRAB_BOOTSTRAP="$(KACRAB_BOOTSTRAP)" KACRAB_BENCH_TOPIC="$(KACRAB_BENCH_TOPIC)" benches/scripts/producer_default_matrix.sh

# The consumer bench manages its own per-scenario topics (kacrab-bench /
# kacrab-bench-10k unless KACRAB_BENCH_TOPIC is exported), so it does not
# force $(KACRAB_BENCH_TOPIC) the way the producer targets do.
bench-kafka-consumer:
	KACRAB_BOOTSTRAP="$(KACRAB_BOOTSTRAP)" cargo run -p kacrab-benches --bin consumer_kafka_bench --release

bench-kafka-consumer-java-default:
	@test -x "$(KAFKA_CONSUMER_PERF)" || { echo "missing kafka-consumer-perf-test.sh at $(KAFKA_CONSUMER_PERF). Set KAFKA_BIN=/path/to/kafka/bin"; exit 1; }
	KAFKA_BIN="$(KAFKA_BIN)" KAFKA_ROOT="$(KAFKA_ROOT)" KAFKA_CONSUMER_PERF="$(KAFKA_CONSUMER_PERF)" KACRAB_BOOTSTRAP="$(KACRAB_BOOTSTRAP)" benches/scripts/consumer_default_matrix.sh

audit:
	@command -v cargo-audit >/dev/null 2>&1 || { echo "cargo-audit not installed. Run: make tools"; exit 1; }
	cargo audit --deny warnings

deny:
	@command -v cargo-deny >/dev/null 2>&1 || { echo "cargo-deny not installed. Run: make tools"; exit 1; }
	cargo deny check

udeps:
	@command -v cargo-udeps >/dev/null 2>&1 || { echo "cargo-udeps not installed. Run: make tools"; exit 1; }
	cargo +nightly udeps --workspace --all-targets

machete:
	@command -v cargo-machete >/dev/null 2>&1 || { echo "cargo-machete not installed. Run: make tools"; exit 1; }
	cargo machete

outdated:
	@command -v cargo-outdated >/dev/null 2>&1 || { echo "cargo-outdated not installed. Run: make tools"; exit 1; }
	cargo outdated --workspace --root-deps-only

ci: fmt-check clippy doc-check test

ci-strict: fmt-check clippy doc-check test audit deny machete

tools:
	cargo install --locked cargo-audit cargo-deny cargo-machete cargo-outdated cargo-udeps
