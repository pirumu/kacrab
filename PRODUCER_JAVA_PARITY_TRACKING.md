# Producer Java Parity Tracking

This file tracks producer implementation parity against Apache Kafka Java
producer, pinned to `apache/kafka@4.3.0`.

Rule for this tracker: do not mark an item done without source evidence and a
fresh verification command. Benchmark numbers are not parity proof unless the
semantic and metric rows below are also satisfied.

Related local audit: `BENCHMARK_PARITY_AUDIT.md`.

## Oracle Sources

Pinned Java source:

- `KafkaProducer`: https://github.com/apache/kafka/blob/4.3.0/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java
- `RecordAccumulator`: https://github.com/apache/kafka/blob/4.3.0/clients/src/main/java/org/apache/kafka/clients/producer/internals/RecordAccumulator.java
- `ProducerBatch`: https://github.com/apache/kafka/blob/4.3.0/clients/src/main/java/org/apache/kafka/clients/producer/internals/ProducerBatch.java
- `Sender`: https://github.com/apache/kafka/blob/4.3.0/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java
- `BufferPool`: https://github.com/apache/kafka/blob/4.3.0/clients/src/main/java/org/apache/kafka/clients/producer/internals/BufferPool.java
- `BuiltInPartitioner`: https://github.com/apache/kafka/blob/4.3.0/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java
- `ProducerPerformance`: https://github.com/apache/kafka/blob/4.3.0/tools/src/main/java/org/apache/kafka/tools/ProducerPerformance.java

Java behavior evidence already checked locally:

- `ProducerPerformance` sends one `ProducerRecord` per loop and captures one
  send timestamp per record before `producer.send(record, cb)`:
  `ProducerPerformance.java:91-108`.
- Java callback latency is callback completion time minus that per-record start
  timestamp, and only successful sends are counted:
  `ProducerPerformance.java:539-560`.
- `KafkaProducer.send(record, callback)` intercepts, then calls `doSend`:
  `KafkaProducer.java:959-963`.
- `doSend` waits for metadata, serializes key/value, calculates partition,
  validates record size, appends to `RecordAccumulator`, wakes sender when the
  batch is full or new, and returns the append future:
  `KafkaProducer.java:975-1045`.
- `RecordAccumulator.append` picks sticky partition, tries existing batch,
  allocates through `BufferPool` when needed, creates `ProducerBatch`, and
  returns `RecordAppendResult`:
  `RecordAccumulator.java:275-405`, `425-445`, `1237-1251`.
- Java exposes buffer pressure metrics such as waiting threads and buffer
  available bytes:
  `RecordAccumulator.java:198-210`, `BufferPool.java:70-85`, `145-211`.
- Java adaptive partitioning updates load stats and node drain readiness:
  `RecordAccumulator.java:724-745`, `971-993`,
  `Sender.java:398-414`.
- Java sender metrics record batch size, records per request, retries, errors,
  request latency, and batch splits:
  `Sender.java:1069-1135`.
- Java `batch-split-total` is not a ProduceRequest grouping counter. It is
  recorded when a broker returns `MESSAGE_TOO_LARGE` for a multi-record
  `ProducerBatch` and Sender calls `RecordAccumulator.splitAndReenqueue`:
  `Sender.java:620-636`, `Sender.java:1133-1135`.

## Current Rust Evidence

Memory/code graph evidence used before editing this file:

- `Producer::from_parts` is the runtime producer construction point:
  `kacrab/src/producer/client.rs:167`.
- Current send APIs and dispatch path live in:
  `kacrab/src/producer/client.rs:422`, `472`, `634`,
  `kacrab/src/producer/dispatcher.rs`.
- Current default max request size is 1 MiB:
  `kacrab/src/producer/config.rs:29`.
- Existing producer metrics include produce request/record/retry/error/requeue
  counts and batch fill ratio, but the real Kafka bench currently prints them
  only behind disabled `metrics_enabled` formatting.

Fresh command evidence from this session:

- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `cargo test -p kacrab --all-features
  producer::sender::tests::sender_state_waits_for_dispatch_completion --lib`:
  first failed with missing `ProducerSenderState::wait_for_dispatch_completion`,
  then passed after adding the semantic dispatch-completion wait API and routing
  sender slot waits through it.
- `cargo test -p kacrab --all-features
  producer::client::tests::wait_for_one_helpers_process_completed_tasks --lib`:
  pass after routing `Producer::wait_for_one` and `wait_for_one_for_flush`
  through `ProducerSenderState::wait_for_dispatch_completion`.
- `cargo test -p kacrab --all-features
  producer::client::tests::wait_for_one_helpers_return_ok_when_no_task_exists
  --lib`: pass after the same routing.
- `cargo test -p kacrab --all-features
  producer::sender::tests::complete_dispatch_result_normalizes_join_errors_and_releases_partitions
  --lib`: first failed with missing `ProducerSenderState::complete_dispatch_result`,
  then passed after moving joined dispatch normalization into sender state.
- `cargo test -p kacrab --all-features
  producer::client::tests::collect_finished_consumes_successful_and_panicked_tasks
  --lib`: pass after routing production collection through
  `ProducerSenderState::complete_dispatch_result`.
- `cargo test -p kacrab --all-features
  producer::client::tests::wait_for_one_helpers_process_completed_tasks --lib`:
  pass after the same completion normalization routing.
- `cargo test -p kacrab --all-features
  producer::client::tests::dispatch_task_result_requeues_batches_or_errors_for_flush
  --lib`: pass after `Producer::dispatch_task_result` stopped accepting raw
  Tokio join results.
- `cargo test -p kacrab --all-features
  producer::sender::tests::wait_for_completed_dispatch_returns_normalized_completion
  --lib`: first failed with missing `ProducerSenderState::wait_for_completed_dispatch`,
  then passed after sender wait APIs started returning normalized dispatch
  completion results.
- `cargo test -p kacrab --all-features
  producer::sender::tests::collect_completed_dispatches_returns_normalized_completions
  --lib`: first failed with missing
  `ProducerSenderState::collect_completed_dispatches`, then passed after the
  non-blocking collection path normalized completed dispatches in sender state.
- `cargo test -p kacrab --all-features
  producer::sender::tests::sender_state_prepares_ready_dispatch_without_draining_before_completion_is_processed
  --lib`: pass after ready prepared dispatch completion stopped exposing raw
  Tokio join errors.
- `cargo test -p kacrab --all-features
  producer::sender::tests::sender_state_prepares_all_dispatch_without_draining_before_completion_is_processed
  --lib`: pass after flush prepared dispatch completion stopped exposing raw
  Tokio join errors.
- `rg -n "wait_for_completed_dispatch|collect_completed_dispatches|complete_dispatch_result|JoinError"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  `JoinError` is now confined to `producer/sender.rs`, with `client.rs` using
  completed/normalized sender APIs only.
- `cargo test -p kacrab --all-features
  producer::sender::tests::spawn_observed_drained_dispatch_records_batches_before_task_owns_them
  --lib`: first failed with missing
  `ProducerSenderState::spawn_observed_drained_dispatch`, then passed after
  sender-owned dispatch spawning gained a pre-spawn batch observation hook.
- `cargo test -p kacrab --all-features
  producer::client::tests::flush_records_local_metrics_like_java --lib`: pass
  after `Producer::spawn_dispatch` routed batch metrics through the sender
  dispatch-spawn hook.
- `cargo test -p kacrab --all-features
  producer::client::tests::poll_waits_for_one_in_flight_slot_before_spawning_ready_batch
  --lib`: pass after the same sender spawn hook routing.
- `cargo test -p kacrab --all-features
  producer::sender::tests::start_dispatch_selection_requeues_deferred_and_spawns_dispatchable_batches
  --lib`: first failed with missing
  `ProducerSenderState::start_dispatch_selection`, `DispatchSelectionStart`,
  and `DispatchStart`, then passed after sender state took ownership of
  deferred-batch requeue plus dispatchable-batch spawning for one prepared
  selection.
- `cargo test -p kacrab --all-features
  producer::client::tests::poll_waits_for_one_in_flight_slot_before_spawning_ready_batch
  --lib`: pass after `Producer::poll` routed prepared selections through
  `ProducerSenderState::start_dispatch_selection`.
- `cargo test -p kacrab --all-features
  producer::client::tests::flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout
  --lib`: pass after `Producer::flush_inner` routed prepared selections through
  the same sender API.
- `cargo test -p kacrab --all-features
  producer::client::tests::flush_records_local_metrics_like_java --lib`: pass
  after the selection-start route preserved producer batch metric accounting.
- `rg -n "selection\\.deferred|selection\\.dispatchable|start_dispatch_selection|DispatchSelectionStart|DispatchStart"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  production `client.rs` starts prepared selections through sender state
  instead of directly requeueing deferred batches or spawning dispatchable
  batches.
- `cargo test -p kacrab --all-features
  producer::sender::tests::prepare_ready_dispatch_or_requeue_restores_batches_on_prepare_error
  --lib`: first failed with missing
  `ProducerSenderState::prepare_ready_dispatch_batches_or_requeue`, then passed
  after sender state started requeueing ready-path prepare failures before
  returning `ProducerError`.
- `cargo test -p kacrab --all-features
  producer::sender::tests::prepare_all_dispatch_or_requeue_restores_batches_on_prepare_error
  --lib`: first failed with missing
  `ProducerSenderState::prepare_all_dispatch_batches_or_requeue`, then passed
  after sender state started requeueing drain-all prepare failures before
  returning `ProducerError`.
- `cargo test -p kacrab --all-features
  producer::client::tests::poll_waits_for_one_in_flight_slot_before_spawning_ready_batch
  --lib`: pass after `Producer::poll` stopped handling
  `DispatchPrepareError` directly.
- `cargo test -p kacrab --all-features
  producer::client::tests::flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout
  --lib`: pass after `Producer::flush_inner` stopped handling
  `DispatchPrepareError` directly.
- `rg -n "DispatchPrepareError|prepare_.*or_requeue|requeue_front\\(error\\.batches"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  `DispatchPrepareError` and prepare-error batch requeue are confined to
  `producer/sender.rs`, while `client.rs` uses the requeueing sender APIs.
- `cargo test -p kacrab --all-features
  producer::sender::tests::handle_completed_dispatch_records_latency_and_propagates_delivered_error
  --lib`: first failed with missing
  `ProducerSenderState::handle_completed_dispatch`, then passed after sender
  state started handling delivered dispatch completion and latency observation.
- `cargo test -p kacrab --all-features
  producer::sender::tests::handle_completed_dispatch_requeues_batches_and_reports_flush_incomplete
  --lib`: first failed with the same missing sender completion handler, then
  passed after sender state started requeueing dispatch completions and mapping
  flush-mode requeues to `FlushIncomplete`.
- `cargo test -p kacrab --all-features
  producer::client::tests::dispatch_task_result_requeues_batches_or_errors_for_flush
  --lib`: pass after `Producer::dispatch_task_result` routed requeue handling
  through `ProducerSenderState::handle_completed_dispatch`.
- `cargo test -p kacrab --all-features
  producer::client::tests::dispatch_task_result_records_latency_when_metrics_are_enabled
  --lib`: pass after latency accounting moved behind the sender completion
  handler hook.
- `rg -n "DispatchOutcome|handle_completed_dispatch|dispatch_task_result|record_dispatch_latency|record_requeue|Requeue\\("
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  dispatch completion `Delivered`/`Requeue` matching now lives in
  `producer/sender.rs`, while `client.rs` provides latency and requeue metric
  hooks.
- `cargo test -p kacrab --all-features
  producer::sender::tests::handle_finished_dispatches_collects_and_handles_completed_tasks
  --lib`: first failed with missing
  `ProducerSenderState::handle_finished_dispatches`, then passed after sender
  state started collecting completed dispatch tasks and applying latency/requeue
  handling internally.
- `cargo test -p kacrab --all-features
  producer::client::tests::collect_finished_consumes_successful_and_panicked_tasks
  --lib`: pass after `Producer::collect_finished` routed non-blocking
  completion collection through `ProducerSenderState::handle_finished_dispatches`.
- `cargo test -p kacrab --all-features
  producer::client::tests::collect_finished_for_flush_consumes_completed_tasks
  --lib`: pass after the flush collection path routed through the same
  sender-side finished-dispatch handler.
- `rg -n "collect_completed_dispatches\\(|handle_finished_dispatches|for result in self\\.sender_state\\.collect"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  `Producer` no longer loops over sender completed dispatch results directly;
  collection plus result handling is in `producer/sender.rs`.
- `cargo test -p kacrab --all-features
  producer::sender::tests::wait_for_handled_dispatch_handles_next_completed_task
  --lib`: first failed with missing
  `ProducerSenderState::wait_for_handled_dispatch`, then passed after sender
  state started waiting for one completed dispatch and applying latency/requeue
  handling internally.
- `cargo test -p kacrab --all-features
  producer::client::tests::wait_for_one_helpers_process_completed_tasks --lib`:
  pass after `Producer::wait_for_one` and `Producer::wait_for_one_for_flush`
  routed blocking completion handling through
  `ProducerSenderState::wait_for_handled_dispatch`.
- `cargo test -p kacrab --all-features
  producer::client::tests::wait_for_one_helpers_return_ok_when_no_task_exists
  --lib`: pass after the same routing preserved empty in-flight wait behavior.
- `rg -n "wait_for_handled_dispatch|wait_for_completed_dispatch\\(\\)\\.await|wait_for_one\\(|wait_for_one_for_flush"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  `Producer` wait helpers call the sender handled-wait API, while raw
  `wait_for_completed_dispatch().await` remains in `producer/sender.rs`.
- `cargo test -p kacrab --all-features
  producer::sender::tests::apply_ready_dispatch_progress_handles_completion_and_prepared_selection
  --lib`: first failed with missing
  `ProducerSenderState::apply_ready_dispatch_progress`,
  `ReadyDispatchApplication`, and `ReadyDispatchProgress`, then passed after
  sender state started applying ready-path prepared dispatch progress itself.
- `cargo test -p kacrab --all-features
  producer::client::tests::poll_waits_for_one_in_flight_slot_before_spawning_ready_batch
  --lib`: pass after `Producer::poll` routed ready-path prepared dispatch
  application through `ProducerSenderState::apply_ready_dispatch_progress`.
- `cargo test -p kacrab --all-features
  producer::client::tests::poll_waits_until_blocked_in_flight_task_completes
  --lib`: pass after the same routing preserved ready-path blocked completion
  behavior.
- `rg -n "PreparedReadyDispatch::|apply_ready_dispatch_progress|ReadyDispatchApplication|ReadyDispatchProgress"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  `Producer::poll` no longer matches `PreparedReadyDispatch` variants directly;
  ready-path progress application lives in `producer/sender.rs`.
- `cargo test -p kacrab --all-features
  producer::sender::tests::apply_all_dispatch_progress_handles_empty_completion_and_prepared_selection
  --lib`: first failed with missing `AllDispatchApplication`,
  `AllDispatchProgress`, and
  `ProducerSenderState::apply_all_dispatch_progress`, then passed after sender
  state started applying flush-path prepared dispatch progress itself.
- `cargo test -p kacrab --all-features
  producer::client::tests::flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout
  --lib`: pass after `Producer::flush_inner` routed all-batch prepared dispatch
  application through `ProducerSenderState::apply_all_dispatch_progress`.
- `cargo test -p kacrab --all-features
  producer::client::tests::collect_finished_for_flush_consumes_completed_tasks
  --lib`: pass after the same routing preserved flush completion collection
  behavior.
- `rg -n "PreparedAllDispatch::|apply_all_dispatch_progress|AllDispatchApplication|AllDispatchProgress"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  `Producer::flush_inner` no longer matches `PreparedAllDispatch` variants
  directly; flush-path progress application lives in `producer/sender.rs`.
- `cargo test -p kacrab --all-features
  producer::sender::tests::drive_ready_dispatch_progress_prepares_and_applies_ready_batches
  --lib`: first failed with missing
  `ProducerSenderState::drive_ready_dispatch_progress`, then passed after
  sender state started owning the ready-path prepare-plus-apply dispatch
  orchestration.
- `cargo test -p kacrab --all-features
  producer::client::tests::poll_waits_for_one_in_flight_slot_before_spawning_ready_batch
  --lib`: pass after `Producer::poll` routed ready dispatch progress through
  `ProducerSenderState::drive_ready_dispatch_progress`.
- `cargo test -p kacrab --all-features
  producer::client::tests::poll_waits_until_blocked_in_flight_task_completes
  --lib`: pass after the same routing preserved blocked ready-path completion
  behavior.
- `rg -n "drive_ready_dispatch_progress|prepare_ready_dispatch_batches_or_requeue|apply_ready_dispatch_progress|ReadyDispatchApplication"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  `Producer::poll` calls the sender ready-dispatch driver, while ready prepare
  and ready progress application stay in `producer/sender.rs`.
- `cargo test -p kacrab --all-features
  producer::sender::tests::drive_all_dispatch_progress_prepares_and_applies_buffered_batches
  --lib`: first failed with missing
  `ProducerSenderState::drive_all_dispatch_progress`, then passed after sender
  state started owning the flush-path prepare-plus-apply dispatch orchestration.
- `cargo test -p kacrab --all-features
  producer::client::tests::flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout
  --lib`: pass after `Producer::flush_inner` routed all dispatch progress
  through `ProducerSenderState::drive_all_dispatch_progress`.
- `cargo test -p kacrab --all-features
  producer::client::tests::collect_finished_for_flush_consumes_completed_tasks
  --lib`: pass after the same routing preserved flush completion collection
  behavior.
- `rg -n "drive_all_dispatch_progress|prepare_all_dispatch_batches_or_requeue|apply_all_dispatch_progress|AllDispatchApplication"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  `Producer::flush_inner` calls the sender all-dispatch driver, while all-batch
  prepare and all progress application stay in `producer/sender.rs`.
- `cargo test -p kacrab --all-features
  producer::sender::tests::drive_flush_dispatch_progress_maps_empty_and_spawned_steps
  --lib`: first failed with missing `FlushDispatchProgress`,
  `ProducerSenderState::drive_flush_dispatch_progress`, and
  `ProducerSenderState::apply_flush_dispatch_progress`, then passed after sender
  state started mapping all-dispatch progress to flush loop actions.
- `cargo test -p kacrab --all-features
  producer::sender::tests::apply_flush_dispatch_progress_reports_incomplete_without_in_flight_dispatch
  --lib`: pass after sender state started owning the `FlushIncomplete` decision
  for empty dispatch selections with no in-flight task to wait on.
- `cargo test -p kacrab --all-features
  producer::client::tests::flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout
  --lib`: pass after `Producer::flush_inner` routed flush loop action selection
  through `ProducerSenderState::drive_flush_dispatch_progress`.
- `cargo test -p kacrab --all-features
  producer::client::tests::collect_finished_for_flush_consumes_completed_tasks
  --lib`: pass after the same routing preserved flush completion collection
  behavior.
- `rg -n "FlushDispatchProgress|drive_flush_dispatch_progress|apply_flush_dispatch_progress|AllDispatchProgress|DispatchStart"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  `Producer::flush_inner` now matches `FlushDispatchProgress` only, while
  `AllDispatchProgress` and `DispatchStart` stay in `producer/sender.rs`.
- `cargo test -p kacrab --all-features
  producer::sender::tests::flush_completion_progress_reports_complete_or_waiting_for_in_flight_dispatch
  --lib`: first failed with missing
  `ProducerSenderState::flush_completion_progress`, then passed after sender
  state started owning the final flush completion wait decision.
- `cargo test -p kacrab --all-features
  producer::client::tests::flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout
  --lib`: pass after the final `Producer::flush_inner` wait loop routed through
  `ProducerSenderState::flush_completion_progress`.
- `cargo test -p kacrab --all-features
  producer::client::tests::collect_finished_for_flush_consumes_completed_tasks
  --lib`: pass after the same routing preserved flush completion collection
  behavior.
- `rg -n "flush_completion_progress|has_in_flight_dispatches\\(\\)"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms the
  final `flush_inner` wait loop calls the sender completion-progress API; the
  remaining direct client `has_in_flight_dispatches` use is in
  `abort_transaction`, outside this flush-tail slice.
- `cargo test -p kacrab --all-features
  producer::sender::tests::wait_for_flush_completion_handles_in_flight_dispatches_until_empty
  --lib`: first failed with missing
  `ProducerSenderState::wait_for_flush_completion`, then passed after sender
  state started owning the final flush completion wait loop and latency/requeue
  observation.
- `cargo test -p kacrab --all-features
  producer::client::tests::flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout
  --lib`: pass after `Producer::flush_inner` routed its post-drain completion
  wait through `ProducerSenderState::wait_for_flush_completion`.
- `cargo test -p kacrab --all-features
  producer::client::tests::collect_finished_for_flush_consumes_completed_tasks
  --lib`: pass after the same routing preserved flush completion collection
  behavior.
- `rg -n "wait_for_flush_completion|flush_completion_progress\\(\\)|while self\\.sender_state\\.flush_completion_progress"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms the
  final flush completion loop lives in `producer/sender.rs`, while
  `Producer::flush_inner` calls a single flush-completion helper.
- `cargo test -p kacrab --all-features
  producer::sender::tests::wait_for_abort_completion_handles_in_flight_dispatches_until_empty
  --lib`: first failed with missing
  `ProducerSenderState::wait_for_abort_completion`, then passed after sender
  state started owning the abort in-flight completion wait loop.
- `cargo test -p kacrab --all-features
  producer::client::tests::abort_transaction_drops_buffered_records_like_java
  --lib`: pass after `Producer::abort_transaction` routed its in-flight
  completion wait through `ProducerSenderState::wait_for_abort_completion`.
- `cargo test -p kacrab --all-features
  producer::client::tests::flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout
  --lib`: pass after the same sender helper reuse preserved flush wait
  behavior.
- `rg -n "wait_for_abort_completion|has_in_flight_dispatches\\(\\)"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  `Producer::abort_transaction` calls the sender abort-completion helper and no
  production `Producer` path reads sender in-flight emptiness directly.
- `cargo test -p kacrab --all-features
  producer::sender::tests::drive_flush_dispatch_step_waits_for_deferred_in_flight_partition
  --lib`: first failed with missing
  `ProducerSenderState::drive_flush_dispatch_step`, then passed after sender
  state started owning the flush-loop `WaitForCompletion` action by waiting for
  one handled dispatch and returning `Continue`.
- `cargo test -p kacrab --all-features
  producer::client::tests::flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout
  --lib`: pass after `Producer::flush_inner` routed each flush loop step
  through `ProducerSenderState::drive_flush_dispatch_step`.
- `rg -n "drive_flush_dispatch_step|wait_for_one_for_flush"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  `Producer::flush_inner` calls the sender flush step helper and no longer
  calls `wait_for_one_for_flush` from the flush loop.
- `cargo test -p kacrab --all-features
  producer::sender::tests::sender_state_owns_append_backpressure_action
  --lib`: first failed with missing `AppendBackpressureAction` and
  `ProducerSenderState::append_backpressure_action`, then passed after sender
  state started owning append/backpressure/wait decision selection.
- `cargo test -p kacrab --all-features
  producer::client::tests::send_and_batch_apis_surface_backpressure_before_dispatch
  --lib`: pass after producer append loops routed buffer-memory pressure
  through `ProducerSenderState::append_backpressure_action`.
- `cargo test -p kacrab --all-features
  producer::client::tests::send_apis_reject_records_larger_than_max_request_size
  --lib`: pass after the same routing preserved local record-size rejection.
- `cargo test -p kacrab --all-features
  producer::client::tests::poll_waits_until_blocked_in_flight_task_completes
  --lib`: pass after the same routing preserved blocked in-flight progress.
- `rg -n "has_pending_work|append_backpressure_action|AppendBackpressureAction"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  production `Producer` append paths no longer call `has_pending_work`
  directly; the pending-work decision is inside `producer/sender.rs`.
- `cargo test -p kacrab --all-features
  producer::sender::tests::drive_ready_dispatch_until_blocked_handles_completion_then_starts_ready_batch
  --lib`: first failed with missing
  `ProducerSenderState::drive_ready_dispatch_until_blocked`, then passed after
  sender state started owning the ready-dispatch poll loop that collects
  completions, handles `Continue`, and stops once idle or a dispatch starts.
- `cargo test -p kacrab --all-features
  producer::client::tests::poll_waits_for_one_in_flight_slot_before_spawning_ready_batch
  --lib`: pass after `Producer::poll` routed its loop through
  `ProducerSenderState::drive_ready_dispatch_until_blocked`.
- `cargo test -p kacrab --all-features
  producer::client::tests::poll_waits_until_blocked_in_flight_task_completes
  --lib`: pass after the same routing preserved blocked in-flight completion
  behavior.
- `rg -n "ReadyDispatchProgress|drive_ready_dispatch_until_blocked|drive_ready_dispatch_progress|collect_finished\\(\\)"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  production `Producer::poll` no longer matches `ReadyDispatchProgress`; the
  loop over `Idle` / `Continue` / `Started` lives in `producer/sender.rs`.
- `cargo test -p kacrab --all-features
  producer::sender::tests::wait_for_buffer_progress_handles_dispatch_completion
  --lib`: first failed with missing
  `ProducerSenderState::wait_for_buffer_progress`, then passed after sender
  state started owning buffer-wait action execution for in-flight completion.
- `cargo test -p kacrab --all-features
  producer::client::tests::send_and_batch_apis_surface_backpressure_before_dispatch
  --lib`: pass after `Producer::wait_for_buffer` routed through
  `ProducerSenderState::wait_for_buffer_progress`.
- `cargo test -p kacrab --all-features
  producer::client::tests::poll_waits_until_blocked_in_flight_task_completes
  --lib`: pass after the same routing preserved blocked in-flight completion.
- `rg -n "BufferWaitAction|wait_for_buffer_progress|buffer_wait_action"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  production `Producer` no longer matches `BufferWaitAction`; buffer wait
  action execution lives in `producer/sender.rs`.
- `cargo test -p kacrab --all-features
  producer::sender::tests::wait_for_append_capacity_drains_ready_buffered_batch_before_append
  --lib`: first failed with missing
  `ProducerSenderState::wait_for_append_capacity`, then passed after sender
  state started owning the append-capacity wait loop that drains ready buffered
  batches before allowing the append.
- `cargo test -p kacrab --all-features
  producer::client::tests::send_and_batch_apis_surface_backpressure_before_dispatch
  --lib`: pass after producer append loops routed through
  `ProducerSenderState::wait_for_append_capacity`.
- `cargo test -p kacrab --all-features
  producer::client::tests::send_apis_reject_records_larger_than_max_request_size
  --lib`: pass after the same routing preserved local max request validation.
- `rg -n "AppendBackpressureAction|append_backpressure_action|wait_for_append_capacity|wait_for_buffer_progress"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  production `Producer` no longer matches `AppendBackpressureAction`; append
  capacity waiting lives in `producer/sender.rs`.
- Final verification after sender completion normalization:
  `make fmt`, `make clippy`, and `make test` all pass. The full workspace test
  run includes 329 `kacrab` lib tests, 75 producer dispatcher integration tests,
  12 `producer_kafka_bench` unit tests, and the existing ignored real-Kafka /
  Java interop tests remain ignored.
- Final verification after sender dispatch-spawn observation hook:
  `make fmt`, `make clippy`, and `make test` all pass. The full workspace test
  run includes 330 `kacrab` lib tests, 75 producer dispatcher integration tests,
  12 `producer_kafka_bench` unit tests, and the existing ignored real-Kafka /
  Java interop tests remain ignored.
- Final verification after prepared-selection start moved into sender:
  `make fmt`, `make test`, and a final `make clippy` all pass. The full
  workspace test run includes 331 `kacrab` lib tests, 75 producer dispatcher
  integration tests, 12 `producer_kafka_bench` unit tests, and the existing
  ignored real-Kafka / Java interop tests remain ignored.
- Final verification after prepare-error recovery moved into sender:
  `make fmt`, `make clippy`, and `make test` all pass. The full workspace test
  run includes 333 `kacrab` lib tests, 75 producer dispatcher integration
  tests, 12 `producer_kafka_bench` unit tests, and the existing ignored
  real-Kafka / Java interop tests remain ignored.
- Final verification after completed-dispatch result handling moved into
  sender: `make fmt`, `make clippy`, and `make test` all pass. The full
  workspace test run includes 335 `kacrab` lib tests, 75 producer dispatcher
  integration tests, 12 `producer_kafka_bench` unit tests, and the existing
  ignored real-Kafka / Java interop tests remain ignored.
- Final verification after non-blocking finished-dispatch collection handling
  moved into sender: `make fmt`, `make clippy`, and `make test` all pass. The
  full workspace test run includes 336 `kacrab` lib tests, 75 producer
  dispatcher integration tests, 12 `producer_kafka_bench` unit tests, and the
  existing ignored real-Kafka / Java interop tests remain ignored.
- Final verification after blocking wait-for-one completion handling moved into
  sender: `make fmt`, `make clippy`, and `make test` all pass. The full
  workspace test run includes 337 `kacrab` lib tests, 75 producer dispatcher
  integration tests, 12 `producer_kafka_bench` unit tests, and the existing
  ignored real-Kafka / Java interop tests remain ignored.
- Final verification after ready-path prepared dispatch progress application
  moved into sender: `make fmt`, `make clippy`, and `make test` all pass. The
  full workspace test run includes 338 `kacrab` lib tests, 75 producer
  dispatcher integration tests, 12 `producer_kafka_bench` unit tests, and the
  existing ignored real-Kafka / Java interop tests remain ignored.
- Final verification after flush-path prepared dispatch progress application
  moved into sender: `make fmt`, `make clippy`, and `make test` all pass. The
  full workspace test run includes 339 `kacrab` lib tests, 75 producer
  dispatcher integration tests, 12 `producer_kafka_bench` unit tests, and the
  existing ignored real-Kafka / Java interop tests remain ignored.
- Final verification after ready-path prepare-plus-apply dispatch orchestration
  moved into sender: `make fmt`, `make clippy`, and `make test` all pass. The
  full workspace test run includes 340 `kacrab` lib tests, 75 producer
  dispatcher integration tests, 12 `producer_kafka_bench` unit tests, and the
  existing ignored real-Kafka / Java interop tests remain ignored.
- Final verification after flush-path prepare-plus-apply dispatch orchestration
  moved into sender: `make fmt`, `make clippy`, and `make test` all pass. The
  full workspace test run includes 341 `kacrab` lib tests, 75 producer
  dispatcher integration tests, 12 `producer_kafka_bench` unit tests, and the
  existing ignored real-Kafka / Java interop tests remain ignored.
- Final verification after flush loop action selection moved into sender:
  `make fmt`, `make clippy`, and `make test` all pass. The full workspace test
  run includes 343 `kacrab` lib tests, 75 producer dispatcher integration
  tests, 12 `producer_kafka_bench` unit tests, and the existing ignored
  real-Kafka / Java interop tests remain ignored.
- Final verification after final flush completion wait decision moved into
  sender: `make fmt`, `make clippy`, and `make test` all pass. The full
  workspace test run includes 344 `kacrab` lib tests, 75 producer dispatcher
  integration tests, 12 `producer_kafka_bench` unit tests, and the existing
  ignored real-Kafka / Java interop tests remain ignored.
- Final verification after final flush completion wait loop moved into sender:
  `make fmt`, `make clippy`, and `make test` all pass. The full workspace test
  run includes 345 `kacrab` lib tests, 75 producer dispatcher integration
  tests, 12 `producer_kafka_bench` unit tests, and the existing ignored
  real-Kafka / Java interop tests remain ignored.
- Final verification after abort in-flight completion wait loop moved into
  sender: `make fmt`, `make clippy`, and `make test` all pass. The full
  workspace test run includes 346 `kacrab` lib tests, 75 producer dispatcher
  integration tests, 12 `producer_kafka_bench` unit tests, and the existing
  ignored real-Kafka / Java interop tests remain ignored.
- Final verification after blocked flush-loop completion step moved into
  sender: `make fmt`, `make clippy`, and `make test` all pass. The full
  workspace test run includes 347 `kacrab` lib tests, 75 producer dispatcher
  integration tests, 12 `producer_kafka_bench` unit tests, and the existing
  ignored real-Kafka / Java interop tests remain ignored.
- Final verification after append buffer-pressure decision selection moved
  into sender: `make fmt`, `make clippy`, and `make test` all pass. The full
  workspace test run includes 348 `kacrab` lib tests, 75 producer dispatcher
  integration tests, 12 `producer_kafka_bench` unit tests, and the existing
  ignored real-Kafka / Java interop tests remain ignored.
- Final verification after ready-dispatch poll loop moved into sender:
  `make fmt`, `make clippy`, and `make test` all pass. The full workspace test
  run includes 349 `kacrab` lib tests, 75 producer dispatcher integration
  tests, 12 `producer_kafka_bench` unit tests, and the existing ignored
  real-Kafka / Java interop tests remain ignored.
- Final verification after buffer-wait action execution moved into sender:
  `make fmt`, `make clippy`, and `make test` all pass. The full workspace test
  run includes 350 `kacrab` lib tests, 75 producer dispatcher integration
  tests, 12 `producer_kafka_bench` unit tests, and the existing ignored
  real-Kafka / Java interop tests remain ignored.
- Final verification after append-capacity wait loop moved into sender:
  `make fmt`, `make clippy`, and `make test` all pass. The full workspace test
  run includes 351 `kacrab` lib tests, 75 producer dispatcher integration
  tests, 12 `producer_kafka_bench` unit tests, and the existing ignored
  real-Kafka / Java interop tests remain ignored.
- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `make bench-kafka`: pass after `make kafka-topic-create`.
  Earlier Rust baseline printed:
  - `5,000,000 x 10B`: 2,486,798 msg/s, 23.716 MB/s average over 5 runs.
  - `100,000 x 10 KiB`: 28,250 msg/s, 275.879 MB/s average over 5 runs.
- `bash -n benches/scripts/producer_default_matrix.sh`: pass.
- `make bench-kafka-java-default`: pass with 5 Java runs per scenario,
  effective `ProducerConfig` snapshot before each run, and compact metric lines
  parsed from `--print-metrics`.
  - `5,000,000 x 10B`: 3,548,252 msg/s, 33.838 MB/s average over 5 runs.
  - `100,000 x 10 KiB`: 39,513 msg/s, 385.866 MB/s average over 5 runs.
- `make bench-kafka`: pass after adding Rust benchmark counter output.
  - `5,000,000 x 10B`: 2,309,110 msg/s, 22.021 MB/s average over 5 runs.
  - `100,000 x 10 KiB`: 28,391 msg/s, 277.253 MB/s average over 5 runs.
  - Rust 10 KiB run emitted about 100,000 produce requests and 1.000
    records/request; Java public metrics emitted about 33,333 derived produce
    requests and 3.000 records/request. This is a batching parity gap, not a
    completed parity result.
- `make bench-kafka`: pass after callback-path request coalescing and
  request-size split guard.
  - `5,000,000 x 10B`: 2,370,041 msg/s, 22.602 MB/s average over 5 runs.
  - `100,000 x 10 KiB`: 32,027 msg/s, 312.768 MB/s average over 5 runs.
  - Rust 10 KiB emitted `produce_requests=35306`, `record_batches=100000`,
    `records_per_batch_avg=1.000`, `records_per_request_avg=2.832`,
    `record_batch_payload_bytes_per_request_avg=29207.500`, `retries=0`,
    `errors=0`, `in_flight_stalls=0`, `requeues=0`.
  - A `CALLBACK_READY_BATCH_POLL_THRESHOLD=128` experiment improved packing to
    `records_per_request_avg=2.898` but reduced throughput to 32,154 msg/s and
    doubled latency; reverted to threshold 64.
  - A lazy `send_with_callback` completion-collection experiment reduced 10 KiB
    throughput to 28,054 msg/s; reverted.
- `cargo test -p kacrab-benches --bin producer_kafka_bench
  tracked_result_metrics_use_parity_counter_schema -- --nocapture`: pass after
  adding Rust `request_size_avg` from generated ProduceRequest encoded length.
- `cargo test -p kacrab-benches --bin producer_kafka_bench -- --nocapture`:
  pass, 11 tests.
- `cargo test -p kacrab --features producer --test producer_dispatcher
  kafka_producer_metrics_snapshot_reports_queue_and_dispatch_counters --
  --nocapture`: pass.
- `cargo test -p kacrab --features producer --test producer_dispatcher
  kafka_producer_single_send_budget_coalesces_ready_partitions -- --nocapture`:
  pass.
- `bash -n benches/scripts/producer_default_matrix.sh`: pass after renaming the
  Java derived request count field to the shared `produce_requests` name.
- `make bench-kafka`: pass after adding `request_size_avg` output.
  - `5,000,000 x 10B`: 2,349,904 msg/s, 22.410 MB/s average over 5 runs.
  - `100,000 x 10 KiB`: 31,779 msg/s, 310.345 MB/s average over 5 runs.
  - Rust 10 KiB emitted `produce_requests=35306`, `record_batches=100000`,
    `records_per_request_avg=2.832`, `request_size_avg=29254.327`,
    `record_batch_payload_bytes_per_request_avg=29207.500`, `retries=0`,
    `errors=0`, `in_flight_stalls=0`, `requeues=0`.
- `make bench-kafka-java-default`: pass after Java counter field rename.
  - `5,000,000 x 10B`: 3,535,024 msg/s, 33.714 MB/s average over 5 runs.
  - `100,000 x 10 KiB`: 32,211 msg/s, 314.558 MB/s average over 5 runs.
  - Java 10 KiB emitted `produce_requests=33333.333`,
    `records_per_request_avg=3`, `request_size_avg` around `31016-31018`,
    `retries=0`, `errors=0`, `batch_splits=0`; exact record-batch count and
    in-flight stalls remain `not_exposed_by_producer_perf`.
- `cargo test -p kacrab-benches --bin producer_kafka_bench
  tracked_result_metrics_use_parity_counter_schema -- --nocapture`: pass after
  keeping Rust `batch_splits=not_tracked` and adding separate
  `request_splits=0`.
- `cargo test -p kacrab --features producer
  producer::dispatcher::tests::broker_request_index_splits_when_next_partition_would_exceed_max_request_size
  -- --nocapture`: pass; the placement helper now distinguishes a new request
  opened by `max.request.size` from a new request opened by duplicate route
  conflict.
- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `make bench-kafka`: pass during the brief numeric `batch_splits` experiment.
  - `5,000,000 x 10B`: 2,290,388 msg/s, 21.843 MB/s average over 5 runs.
  - `100,000 x 10 KiB`: 19,537 msg/s, 190.788 MB/s average over 5 runs.
  - This run is retained as negative/diagnostic evidence only. The numeric Rust
    `batch_splits` label was reverted because Java's `batch-split-total` tracks
    broker-triggered ProducerBatch splitting, not ProduceRequest grouping.
- `cargo test -p kacrab-benches --bin producer_kafka_bench
  tracked_result_metrics_use_parity_counter_schema -- --nocapture`: pass after
  restoring Rust `batch_splits=not_tracked` and adding separate
  `request_splits`.
- `cargo test -p kacrab --features producer
  producer::dispatcher::tests::broker_request_index_splits_when_next_partition_would_exceed_max_request_size
  -- --nocapture`: pass.
- `bash -n benches/scripts/producer_default_matrix.sh`: pass after adding Java
  `request_splits=not_exposed_by_producer_perf`.
- `make fmt`: pass after the counter semantic correction.
- `make clippy`: pass after the counter semantic correction.
- `make test`: pass after the counter semantic correction.
- `make bench-kafka`: pass after restoring honest Rust `batch_splits` semantics.
  - `5,000,000 x 10B`: 2,379,732 msg/s, 22.695 MB/s average over 5 runs.
  - `100,000 x 10 KiB`: 35,768 msg/s, 349.295 MB/s average over 5 runs.
  - Rust output now emits `batch_splits=not_tracked` and `request_splits=0`.
  - This run did not rerun Java, so it is not Java comparison evidence.
- `cargo test -p kacrab-benches --bin producer_kafka_bench -- --nocapture`:
  pass, 12 tests, after extracting benchmark callback accounting. The test
  `tracked_callback_accounting_counts_successes_without_consuming_failed_start_time`
  proves failed callbacks do not consume per-record start timestamps and only
  successful callbacks with a matching start timestamp update Java-style
  latency/count accounting; extra success callbacks beyond the timestamp list
  are ignored instead of being counted with a fallback timestamp.
- `make fmt && make clippy && make test`: pass after the benchmark callback
  accounting extraction.
- `make bench-kafka`: pass after the benchmark callback accounting extraction.
  - `5,000,000 x 10B`: 2,370,854 msg/s, 22.610 MB/s average over 5 runs.
  - `100,000 x 10 KiB`: 26,473 msg/s, 258.524 MB/s average over 5 runs.
  - The 10 KiB run remained at `produce_requests=35306`,
    `records_per_request_avg=2.832`, `request_size_avg=29254.327`,
    `retries=0`, `errors=0`, `in_flight_stalls=0`, `request_splits=0`.
    The lower 10 KiB average is retained as fresh Rust evidence only; Java was
    not rerun in this step, and this is not parity evidence.
- A callback-path linger-ready sender-tick experiment was tested and reverted.
  The experiment added a cached linger deadline so `send_with_callback` could
  poll an older linger-expired batch even when the current append did not seal a
  batch. Unit tests and `make fmt && make clippy && make test` passed, but real
  Kafka benchmark output regressed the current hot path:
  - First version scanned/checked on every non-ready append:
    `5,000,000 x 10B`: 2,194,185 msg/s, 20.925 MB/s;
    `100,000 x 10 KiB`: 35,581 msg/s, 347.471 MB/s.
  - Cached-deadline version:
    `5,000,000 x 10B`: 2,245,426 msg/s, 21.414 MB/s;
    `100,000 x 10 KiB`: 31,849 msg/s, 311.029 MB/s.
  - Conclusion: API-call-driven linger polling is not the right next production
    shape. The next implementation attempt should move this responsibility to a
    real sender task / IO-owner loop instead of adding checks to every
    `send_with_callback` call. These Rust-only runs are negative evidence, not
    Java parity evidence.
- `cargo test -p kacrab --features producer --test producer_dispatcher
  kafka_producer_10kib_records_keep_observed_requests_under_max_request_size --
  --nocapture`: pass after adding a mock-broker observable test for 120
  partitions of 10 KiB records with `max.request.size=1048576`. The mock broker
  captures raw ProduceRequest frame lengths and partition groups, proving the
  flushed request set is split into two ProduceRequests, each observed request
  stays under the configured max request size, every partition is sent once, and
  dispatcher metrics report `produce_request_count=2`,
  `produce_request_split_count=1`, `produce_retry_count=0`, and
  `produce_error_count=0`.
- `make fmt && make clippy && make test`: pass after the observable
  `max.request.size` proof test.
- `cargo test -p kacrab --features producer
  producer::accumulator::tests::next_ready_at_reports_earliest_linger_deadline
  -- --nocapture`: first failed with `no method named next_ready_at`, then
  passed after adding `RecordAccumulator::next_ready_at`. This is a sender-loop
  primitive only: it reports the earliest buffered batch linger deadline or
  `now` when a batch is already size/seal/linger ready. It is wired into
  producer buffer-wait sleeping so the code path has real use, but it is not a
  background sender task and it is not benchmark parity evidence.
- `cargo test -p kacrab --features producer
  producer::client::tests::flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout
  -- --nocapture`: pass after wiring `next_ready_at` into buffer waits.
- `cargo test -p kacrab --features producer
  producer::client::tests::poll_waits_for_one_in_flight_slot_before_spawning_ready_batch
  -- --nocapture`: pass after wiring `next_ready_at` into buffer waits.
- `make fmt && make clippy && make test`: pass after adding the sender-loop
  linger wake primitive and wiring it into buffer waits.
- `cargo test -p kacrab --features producer
  producer::sender::tests::idempotent_sender_state_defers_partitions_already_in_flight
  -- --nocapture`: first failed with unresolved `ProducerSenderState`, then
  passed after adding `kacrab/src/producer/sender.rs`. The new
  `ProducerSenderState` owns max in-flight configuration and in-flight
  partition reservation/selection state. This is an architecture step toward a
  Java-like sender owner, not a completed background sender loop.
- `cargo test -p kacrab --features producer
  producer::client::tests::idempotent_selection_defers_batches_for_in_flight_partitions_when_order_is_guaranteed
  -- --nocapture`: pass after wiring `Producer` batch selection through
  `ProducerSenderState`.
- `make fmt && make clippy && make test`: pass after extracting sender
  dispatch scheduling state into `producer/sender.rs`.
- `cargo test -p kacrab --features producer
  producer::sender::tests::sender_state_owns_in_flight_task_slots --
  --nocapture`: first failed because `TimedDispatchOutcome`,
  `spawn_in_flight`, `in_flight_len`, and `join_next` did not exist in
  `producer/sender.rs`, then passed after moving `JoinSet<TimedDispatchOutcome>`
  ownership from `Producer` into `ProducerSenderState`.
- `cargo test -p kacrab --features producer
  producer::client::tests::wait_for_one_helpers_process_completed_tasks --
  --nocapture`: pass after moving in-flight task ownership into
  `ProducerSenderState`.
- `cargo test -p kacrab --features producer
  producer::client::tests::poll_waits_for_one_in_flight_slot_before_spawning_ready_batch
  -- --nocapture`: pass after moving in-flight task ownership into
  `ProducerSenderState`.
- `make fmt && make clippy && make test`: pass after moving the producer
  dispatch task `JoinSet` and `TimedDispatchOutcome` into `producer/sender.rs`.
- `cargo test -p kacrab --features producer
  producer::sender::tests::completing_joined_dispatch_releases_reserved_partitions
  -- --nocapture`: first failed because `ProducerSenderState` did not expose
  `complete_joined_dispatch`, then passed after moving joined-dispatch
  partition cleanup into `producer/sender.rs`.
- `cargo test -p kacrab --features producer
  producer::client::tests::dispatch_task_result_requeues_batches_or_errors_for_flush
  -- --nocapture`: pass after `Producer::dispatch_task_result` started routing
  joined results through `ProducerSenderState::complete_joined_dispatch`.
- `cargo test -p kacrab --features producer
  producer::client::tests::dispatch_task_result_records_latency_when_metrics_are_enabled
  -- --nocapture`: pass after routing joined result cleanup through
  `ProducerSenderState`.
- `make fmt && make clippy && make test`: pass after moving joined-dispatch
  partition cleanup into `producer/sender.rs`.
- `cargo test -p kacrab --features producer
  producer::sender::tests::spawn_dispatch_task_reserves_partitions_until_completion
  -- --nocapture`: first failed with `no method named spawn_dispatch_task`,
  then passed after adding `ProducerSenderState::spawn_dispatch_task`. The
  sender state now reserves dispatch partitions at task-spawn time and keeps
  them reserved until `complete_joined_dispatch` releases the joined outcome.
- `cargo test -p kacrab --features producer
  producer::sender::tests::completing_joined_dispatch_releases_reserved_partitions
  -- --nocapture`: pass after moving spawn-time reservation behind
  `ProducerSenderState::spawn_dispatch_task`.
- `cargo test -p kacrab --features producer
  producer::client::tests::poll_waits_for_one_in_flight_slot_before_spawning_ready_batch
  -- --nocapture`: pass after `Producer::spawn_dispatch` stopped reserving
  partitions directly and uses `ProducerSenderState::spawn_dispatch_task`.
- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `git diff --check`: pass.
- `cargo test -p kacrab --features producer
  producer::sender::tests::sender_state_waits_for_dispatch_slot_only_when_limit_is_reached
  -- --nocapture`: first failed with `no method named wait_for_dispatch_slot`,
  then passed after adding `ProducerSenderState::wait_for_dispatch_slot`.
  The sender state now owns the bounded in-flight slot availability check and
  joins one completed dispatch task only when the configured limit is reached.
- `cargo test -p kacrab --features producer
  producer::client::tests::poll_waits_for_one_in_flight_slot_before_spawning_ready_batch
  -- --nocapture`: pass after `Producer::poll` started using
  `ProducerSenderState::wait_for_dispatch_slot` instead of checking the
  in-flight length/limit directly.
- `cargo test -p kacrab --features producer
  producer::client::tests::flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout
  -- --nocapture`: pass after `Producer::flush_inner` started using
  `ProducerSenderState::wait_for_dispatch_slot`.
- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `cargo test -p kacrab --features producer
  producer::sender::tests::sender_state_collects_finished_dispatch_tasks_without_blocking
  -- --nocapture`: first failed with `no method named
  collect_finished_dispatches`, then passed after adding
  `ProducerSenderState::collect_finished_dispatches`. The sender state now owns
  the non-blocking `JoinSet::try_join_next` drain loop for completed dispatch
  tasks.
- `cargo test -p kacrab --features producer
  producer::client::tests::collect_finished_consumes_successful_and_panicked_tasks
  -- --nocapture`: pass after `Producer::collect_finished` started consuming
  completed dispatch task results from `ProducerSenderState`.
- `cargo test -p kacrab --features producer
  producer::client::tests::collect_finished_for_flush_consumes_completed_tasks
  -- --nocapture`: pass after `Producer::collect_finished_for_flush` started
  consuming completed dispatch task results from `ProducerSenderState`.
- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `cargo test -p kacrab --features producer
  producer::sender::tests::sender_state_waits_for_next_dispatch_task --
  --nocapture`: first failed with `no method named wait_for_next_dispatch`,
  then passed after adding `ProducerSenderState::wait_for_next_dispatch`. The
  sender state now owns the blocking wait-for-one-dispatch-completion API, and
  the raw `JoinSet::join_next` wrapper is private to `producer/sender.rs`.
- `cargo test -p kacrab --features producer
  producer::client::tests::wait_for_one_helpers_process_completed_tasks --
  --nocapture`: pass after `Producer::wait_for_one` and
  `Producer::wait_for_one_for_flush` started using
  `ProducerSenderState::wait_for_next_dispatch`.
- `cargo test -p kacrab --features producer
  producer::client::tests::wait_for_one_helpers_return_ok_when_no_task_exists
  -- --nocapture`: pass after routing blocking completion waits through
  `ProducerSenderState`.
- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `cargo test -p kacrab --features producer
  producer::sender::tests::spawn_drained_dispatch_owns_dispatch_task_body_and_partition_reservation
  -- --nocapture`: first failed with `no method named spawn_drained_dispatch`,
  then passed after adding `ProducerSenderState::spawn_drained_dispatch`. The
  sender state now owns dispatch task body construction: partition reservation,
  earliest append latency start, `ProducerDispatcher::dispatch_drained`, and
  `TimedDispatchOutcome` assembly.
- `cargo test -p kacrab --features producer
  producer::client::tests::poll_waits_for_one_in_flight_slot_before_spawning_ready_batch
  -- --nocapture`: pass after `Producer::spawn_dispatch` stopped constructing
  the async dispatch task body and delegates to
  `ProducerSenderState::spawn_drained_dispatch`.
- `cargo test -p kacrab --features producer
  producer::client::tests::dispatch_task_result_records_latency_when_metrics_are_enabled
  -- --nocapture`: pass after moving dispatch task latency capture into
  `ProducerSenderState::spawn_drained_dispatch`.
- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `cargo test -p kacrab --features producer
  producer::sender::tests::prepare_dispatch_batches_selects_and_prepares_dispatchable_batches
  -- --nocapture`: first failed with `no method named prepare_dispatch_batches`,
  then passed after adding `ProducerSenderState::prepare_dispatch_batches` and
  `DispatchPrepareError`. The sender state now owns select-and-prepare for
  drained batches, including idempotent partition deferral and dispatcher
  `prepare_drained_batches` before spawn.
- `cargo test -p kacrab --features producer
  producer::client::tests::poll_waits_for_one_in_flight_slot_before_spawning_ready_batch
  -- --nocapture`: pass after `Producer::poll` started routing select+prepare
  through `ProducerSenderState::prepare_dispatch_batches`.
- `cargo test -p kacrab --features producer
  producer::client::tests::flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout
  -- --nocapture`: pass after `Producer::flush_inner` started routing
  select+prepare through `ProducerSenderState::prepare_dispatch_batches`.
- `cargo test -p kacrab --features producer
  producer::client::tests::dispatch_task_result_requeues_batches_or_errors_for_flush
  -- --nocapture`: pass after preserving prepare-error requeue behavior through
  the sender error carrier.
- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `cargo test -p kacrab --features producer
  producer::client::tests::poll_waits_for_dispatch_slot_before_preparing_ready_batches
  -- --nocapture`: first failed because `Producer::poll` could finish before
  waiting for a full dispatch slot, then passed after adding
  `ProducerSenderState::has_ready_dispatch_batches` and changing `poll` to
  check readiness, wait for a bounded dispatch slot, and only then drain and
  prepare ready batches. This preserves bounded backpressure ordering before
  metadata/idempotence preparation.
- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `cargo test -p kacrab --features producer
  producer::sender::tests::sender_state_waits_for_ready_dispatch_slot_after_readiness
  -- --nocapture`: first failed with missing `ReadyDispatchSlot` and
  `wait_for_ready_dispatch_slot`, then passed after adding a sender-owned gate
  that returns `Idle` for no ready batches and returns `Ready { completed }`
  after any required bounded dispatch-slot wait.
- `cargo test -p kacrab --features producer
  producer::client::tests::poll_waits_for_dispatch_slot_before_preparing_ready_batches
  -- --nocapture`: pass after routing `Producer::poll` through
  `ProducerSenderState::wait_for_ready_dispatch_slot`.
- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `cargo test -p kacrab --features producer
  producer::sender::tests::sender_state_prepares_ready_dispatch_without_draining_before_completion_is_processed
  -- --nocapture`: first failed with missing `PreparedReadyDispatch` and
  `prepare_ready_dispatch_batches`, then passed after adding a sender-owned
  prepare-ready API. The API returns `Idle`, `PendingCompletion`, or a prepared
  dispatch selection, and intentionally avoids draining the accumulator before
  `Producer` processes a dispatch completion that could return an error.
- `cargo test -p kacrab --features producer
  producer::client::tests::poll_waits_for_dispatch_slot_before_preparing_ready_batches
  -- --nocapture`: pass after routing `Producer::poll` through
  `ProducerSenderState::prepare_ready_dispatch_batches`.
- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `cargo test -p kacrab --features producer
  producer::sender::tests::sender_state_prepares_all_dispatch_without_draining_before_completion_is_processed
  -- --nocapture`: first failed with missing `PreparedAllDispatch` and
  `prepare_all_dispatch_batches`, then passed after adding a sender-owned
  flush/all path. The API returns `Empty`, `PendingCompletion`, or a prepared
  dispatch selection, and avoids draining all buffered batches before `Producer`
  processes a dispatch completion that may return an error.
- `cargo test -p kacrab --features producer
  producer::client::tests::flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout
  -- --nocapture`: pass after routing `Producer::flush_inner` through
  `ProducerSenderState::prepare_all_dispatch_batches`.
- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `cargo test -p kacrab --features producer
  producer::sender::tests::sender_state_reports_pending_work_from_buffer_or_in_flight_dispatch
  -- --nocapture`: first failed with missing `has_pending_work`, then passed
  after moving the buffered-or-in-flight predicate into `ProducerSenderState`.
- `cargo test -p kacrab --features producer
  producer::client::tests::send_and_batch_apis_surface_backpressure_before_dispatch
  -- --nocapture`: pass after append backpressure paths started using
  `ProducerSenderState::has_pending_work`.
- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `cargo test -p kacrab --all-features
  producer::sender::tests::sender_state_caps_buffer_sleep_to_one_millisecond
  --lib`: first failed with missing `BufferWaitAction` and
  `ProducerSenderState::buffer_wait_action`, then passed as part of
  `cargo test -p kacrab --all-features producer::sender::tests::sender_state_
  --lib` after moving the buffer-wait decision into sender state.
- `cargo test -p kacrab --all-features
  send_and_batch_apis_surface_backpressure_before_dispatch --lib`: pass after
  routing `Producer::wait_for_buffer` through `ProducerSenderState`.
- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `cargo test -p kacrab --all-features
  producer::sender::tests::sender_state_reports_in_flight_dispatches_for_flush_waits
  --lib`: first failed with missing
  `ProducerSenderState::has_in_flight_dispatches`, then passed after moving the
  flush/abort in-flight predicate into sender state and removing the old
  raw `in_flight_is_empty` accessor.
- `cargo test -p kacrab --all-features
  producer::client::tests::flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout
  --lib`: pass after routing flush through
  `ProducerSenderState::has_in_flight_dispatches`.
- `cargo test -p kacrab --all-features
  producer::client::tests::abort_transaction_drops_buffered_records_like_java
  --lib`: pass after routing abort in-flight waits through
  `ProducerSenderState::has_in_flight_dispatches`.
- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `cargo test -p kacrab --all-features
  producer::sender::tests::sender_state_creates_append_poll_budget_from_in_flight_limit
  --lib`: first failed with missing
  `ProducerSenderState::append_poll_budget`,
  `ProducerSenderState::callback_append_poll_budget`, and sender-local callback
  threshold constant, then passed as part of
  `cargo test -p kacrab --all-features producer::sender::tests::sender_state_creates_
  --lib` after moving `AppendPollBudget` and callback/dense ready-batch poll
  thresholds into `producer/sender.rs`.
- `cargo test -p kacrab --all-features
  producer::client::tests::idempotent_producer_keeps_configured_dispatch_task_concurrency
  --lib`: pass after routing client construction through
  `ProducerSenderState::callback_append_poll_budget`.
- `cargo test -p kacrab --all-features
  kafka_producer_single_send_budget_coalesces_ready_partitions`: pass after
  routing batch send paths through `ProducerSenderState::append_poll_budget`.
- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `cargo test -p kacrab --all-features
  producer::sender::tests::sender_state_owns_callback_append_poll_budget_across_records
  --lib`: first failed with missing
  `ProducerSenderState::observe_callback_append_status`, then passed after
  moving the stateful callback append poll budget field from `Producer` into
  `ProducerSenderState`.
- `cargo test -p kacrab --all-features
  producer::client::tests::send_with_callback_invokes_callback_on_local_api_error_like_java
  --lib`: pass after routing `send_with_callback` through
  `ProducerSenderState::observe_callback_append_status`.
- `cargo test -p kacrab --all-features
  kafka_producer_single_send_budget_coalesces_ready_partitions`: pass after
  moving callback budget state into sender.
- `make fmt`: pass.
- `make clippy`: first failed on `missing_const_for_fn` for
  `observe_callback_append_status`, then passed after making that method const.
- `make test`: pass.
- `cargo test -p kacrab --all-features
  producer::sender::tests::sender_state_reports_in_flight_dispatch_count_for_metrics
  --lib`: first failed with missing
  `ProducerSenderState::in_flight_dispatch_count`, then failed once because the
  completed test tasks had not yielded before collection, then passed after
  adding the sender-owned metric count API and yielding before non-blocking
  collection in the test.
- `cargo test -p kacrab --all-features
  producer::client::tests::metrics_registry_exposes_named_snapshot_values_like_java_metrics_map
  --lib`: pass after routing `Producer::metrics` through
  `ProducerSenderState::in_flight_dispatch_count`.
- `cargo test -p kacrab --all-features
  kafka_producer_metrics_snapshot_reports_queue_and_dispatch_counters`: pass
  after routing producer metrics through the sender metric count API.
- `make fmt`: pass.
- `make clippy`: pass.
- `make test`: pass.
- `cargo test -p kacrab --all-features
  producer::sender::tests::drive_flush_until_complete_drains_in_flight_completion_after_empty_step
  --lib`: first failed with missing
  `ProducerSenderState::drive_flush_until_complete`, then passed after moving
  the full flush loop into sender state.
- `cargo test -p kacrab --all-features
  producer::client::tests::flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout
  --lib`: pass after `Producer::flush_inner` routed through
  `ProducerSenderState::drive_flush_until_complete`.
- `cargo test -p kacrab --all-features
  producer::client::tests::collect_finished_for_flush_consumes_completed_tasks
  --lib`: pass after the same flush-loop routing preserved flush-mode
  completion collection.
- `rg -n "drive_flush_until_complete|drive_flush_dispatch_step|FlushDispatchProgress|collect_finished_for_flush|wait_for_flush_completion"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  production `client.rs` calls sender-owned `drive_flush_until_complete`, while
  raw `FlushDispatchProgress` matching and `drive_flush_dispatch_step` remain
  in `producer/sender.rs`.
- `make fmt`: pass after moving the full flush loop into sender state.
- `make clippy`: first failed on dead `Producer::collect_finished_for_flush`
  after the production flush loop no longer called it, then passed after
  marking that helper test-only.
- `make test`: pass after the flush-loop move; `kacrab` lib now runs 352 unit
  tests, `producer_dispatcher` integration still runs 75 tests, and
  `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --all-features
  producer::sender::tests::sender_state_discards_buffered_batches_for_abort_lifecycle
  --lib`: first failed with missing
  `ProducerSenderState::discard_buffered_batches`, then passed after moving the
  abort buffered-batch discard operation behind sender state.
- `cargo test -p kacrab --all-features
  producer::client::tests::abort_transaction_drops_buffered_records_like_java
  --lib`: pass after `Producer::abort_transaction` routed buffered-batch drop
  through `ProducerSenderState::discard_buffered_batches`.
- `rg -n "drain_all\\(|discard_buffered_batches|abort_transaction"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  production `abort_transaction` no longer calls accumulator `drain_all`
  directly; remaining `client.rs` `drain_all` matches are tests.
- `make fmt`: pass after moving abort buffered-batch discard behind sender state.
- `make clippy`: pass after the abort discard move.
- `make test`: pass after the abort discard move; `kacrab` lib now runs 353
  unit tests, `producer_dispatcher` integration still runs 75 tests, and
  `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --all-features
  producer::sender::tests::sender_state_reports_queue_snapshot_for_metrics
  --lib`: first failed with missing `ProducerSenderState::queue_snapshot`,
  then passed after sender state started reporting buffered bytes, buffered
  records, and in-flight dispatches as one metrics snapshot.
- `cargo test -p kacrab --all-features
  producer::client::tests::metrics_registry_exposes_named_snapshot_values_like_java_metrics_map
  --lib`: pass after `Producer::metrics` routed queue and in-flight gauges
  through `ProducerSenderState::queue_snapshot`.
- `rg -n "queue_snapshot|in_flight_dispatch_count|buffered_records\\(\\)|buffered_bytes\\(\\)"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  production `Producer::metrics` uses `queue_snapshot`; direct
  `in_flight_dispatch_count` use is confined to sender/tests.
- `make fmt`: pass after moving metrics queue/in-flight gauge collection
  behind sender state.
- `make clippy`: pass after the metrics queue snapshot move.
- `make test`: pass after the metrics queue snapshot move; `kacrab` lib now
  runs 354 unit tests, `producer_dispatcher` integration still runs 75 tests,
  and `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --all-features
  producer::sender::tests::append_untracked_waits_for_capacity_then_appends_record
  --lib`: first failed with missing
  `ProducerSenderState::append_untracked_with_capacity_wait`, then passed after
  sender state started owning the untracked append capacity wait plus
  accumulator append step.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_untracked_skips_delivery_handles`: pass after
  `send_batch_untracked` routed untracked appends through sender state.
- `cargo test -p kacrab --all-features kafka_producer_send_buffers_until_flush`:
  pass after the same untracked append routing preserved buffered-send
  semantics.
- `rg -n "append_untracked_with_capacity_wait|append_untracked_with_max_block|append_with_status_at\\("
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs
  kacrab/src/producer/accumulator.rs`: confirms production direct
  `append_with_status_at` use now lives in `producer/sender.rs`, while
  `client.rs` calls `append_untracked_with_capacity_wait` for the untracked
  append path.
- `make fmt`: pass after moving untracked append behind sender state.
- `make clippy`: first failed on `too_many_arguments` for
  `append_untracked_with_capacity_wait`, then passed after grouping the
  append inputs in `AppendUntracked`.
- `make test`: pass after the untracked append move; `kacrab` lib now runs
  355 unit tests, `producer_dispatcher` integration still runs 75 tests, and
  `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --all-features
  producer::sender::tests::append_for_delivery_waits_for_capacity_then_returns_delivery
  --lib`: first failed with missing `AppendDelivery` and
  `ProducerSenderState::append_for_delivery_with_capacity_wait`, then passed
  after sender state started owning the delivery append capacity wait plus
  accumulator append step used by `send` / `send_with_callback`.
- `cargo test -p kacrab --all-features
  producer::client::tests::send_with_callback_invokes_callback_on_local_api_error_like_java
  --lib`: pass after the default callback append path routed through sender
  state.
- `cargo test -p kacrab --all-features
  kafka_producer_send_with_callback_invokes_callback_and_returns_delivery`:
  pass after the same delivery append routing preserved callback delivery
  behavior.
- `rg -n "append_for_delivery_with_capacity_wait|AppendDelivery|append_for_delivery_with_status_at\\("
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs
  kacrab/src/producer/accumulator.rs`: confirms production direct
  `append_for_delivery_with_status_at` use now lives in `producer/sender.rs`,
  while `client.rs` calls `append_for_delivery_with_capacity_wait` for the
  per-record delivery append path.
- `make fmt`: pass after moving per-record delivery append behind sender state.
- `make clippy`: pass after the per-record delivery append move.
- `make test`: pass after the per-record delivery append move; `kacrab` lib now
  runs 356 unit tests, `producer_dispatcher` integration still runs 75 tests,
  and `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --all-features
  producer::sender::tests::append_for_batch_delivery_waits_for_capacity_then_returns_optional_delivery
  --lib`: first failed with missing `AppendBatchDelivery` and
  `ProducerSenderState::append_for_batch_delivery_with_capacity_wait`, then
  passed after sender state started owning the batch-delivery append capacity
  wait plus accumulator append step used by `send_batch` /
  `send_batch_with_callback`.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_returns_delivery_handles`: pass after routing the
  public tracked batch append path through sender state.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_untracked_skips_delivery_handles`: pass after the
  same refactor kept the existing untracked batch integration path green.
- `rg -n "append_for_batch_delivery_with_capacity_wait|AppendBatchDelivery|append_for_batch_delivery_with_status_at_capacity|wait_for_append_capacity\\("
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs
  kacrab/src/producer/accumulator.rs`: confirms production direct
  `append_for_batch_delivery_with_status_at_capacity` use now lives in
  `producer/sender.rs`, while `client.rs` calls
  `append_for_batch_delivery_with_capacity_wait` for the tracked batch append
  path.
- `make fmt`: pass after moving tracked batch delivery append behind sender
  state.
- `make clippy`: pass after the tracked batch delivery append move.
- `make test`: pass after the tracked batch delivery append move; `kacrab` lib
  now runs 357 unit tests, `producer_dispatcher` integration still runs 75
  tests, and `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --all-features
  producer::sender::tests::sender_state_observes_batch_append_poll_budget
  --lib`: first failed with missing
  `ProducerSenderState::observe_batch_append_status`, then passed after sender
  state exposed the batch append poll-budget observation API.
- `cargo test -p kacrab --all-features
  producer::client::tests::send_and_batch_apis_surface_backpressure_before_dispatch
  --lib`: pass after batch public API loops routed append poll-budget decisions
  through `ProducerSenderState::observe_batch_append_status`.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_returns_delivery_handles`: pass after routing the
  tracked batch post-append poll decision through sender state.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_untracked_skips_delivery_handles`: pass after
  routing the untracked batch post-append poll decision through sender state.
- `cargo test -p kacrab --all-features
  kafka_producer_single_send_budget_coalesces_ready_partitions`: pass after the
  same sender-state budget API preserved the current callback coalescing
  behavior.
- `rg -n "poll_budget\\.observe|\\.observe_batch_append_status|ProducerSenderState::observe_batch_append_status"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  production client no longer calls `AppendPollBudget::observe` directly; batch
  loops call the sender-state API and callback cadence remains sender-state
  owned.
- `make fmt`: pass after routing batch append poll-budget observation through
  sender state.
- `make clippy`: first failed on `unused_self` for
  `observe_batch_append_status`, then passed after making it an associated
  sender-state function.
- `make test`: pass after routing batch append poll-budget observation through
  sender state; `kacrab` lib now runs 358 unit tests, `producer_dispatcher`
  integration still runs 75 tests, and `producer_kafka_bench` still runs 12 unit
  tests.
- `cargo test -p kacrab --all-features
  producer::sender::tests::sender_state_reports_append_dispatch_decisions
  --lib`: first failed with missing `AppendDispatchDecision`,
  `single_append_dispatch_decision`, `batch_append_dispatch_decision`, and
  `callback_append_dispatch_decision`, then passed after sender state started
  normalizing append outcomes into `Idle` / `MarkBatchReady` / `DriveReady`.
- `cargo test -p kacrab --all-features
  kafka_producer_send_with_callback_invokes_callback_and_returns_delivery`: pass
  after callback post-append handling routed through sender dispatch decisions.
- `cargo test -p kacrab --all-features
  kafka_producer_single_send_budget_coalesces_ready_partitions`: pass after
  callback coalescing kept the same external behavior through the decision API.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_returns_delivery_handles`: pass after tracked batch
  post-append handling routed through sender dispatch decisions.
- `cargo test -p kacrab --all-features
  producer::client::tests::send_and_batch_apis_surface_backpressure_before_dispatch
  --lib`: pass after single and batch append post-dispatch decisions moved
  behind sender state.
- `rg -n "status\\.batch_ready|observe_callback_append_status|observe_batch_append_status|single_append_dispatch_decision|batch_append_dispatch_decision|callback_append_dispatch_decision|AppendDispatchDecision"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  production client routes append post-dispatch policy through sender decision
  APIs; direct `status.batch_ready` checks now live in sender logic and tests.
- `make fmt`: pass after introducing append dispatch decisions.
- `make clippy`: first failed on collapsible nested sticky-topic `if` blocks,
  then passed after collapsing them.
- `make test`: pass after append dispatch decision routing; `kacrab` lib now
  runs 359 unit tests, `producer_dispatcher` integration still runs 75 tests,
  and `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --all-features
  producer::sender::tests::sender_state_applies_append_dispatch_decision
  --lib`: first failed with missing
  `ProducerSenderState::apply_append_dispatch_decision`, then passed after
  sender state started applying `AppendDispatchDecision` by marking sticky
  batches and driving ready dispatch itself.
- `cargo test -p kacrab --all-features
  producer::client::tests::send_and_batch_apis_surface_backpressure_before_dispatch
  --lib`: pass after public append paths routed decision application through
  sender state.
- `cargo test -p kacrab --all-features
  kafka_producer_send_with_callback_invokes_callback_and_returns_delivery`: pass
  after callback post-append decision application moved behind sender state.
- `cargo test -p kacrab --all-features
  kafka_producer_single_send_budget_coalesces_ready_partitions`: pass after
  callback coalescing kept the same behavior through sender decision
  application.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_returns_delivery_handles`: pass after tracked batch
  decision application moved behind sender state.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_untracked_skips_delivery_handles`: pass after
  untracked batch decision application moved behind sender state.
- `make fmt`: pass after moving append decision application behind sender state.
- `make clippy`: first failed on `too_many_arguments` for
  `apply_append_dispatch_decision`, then passed after grouping inputs in
  `AppendDispatchApplication`.
- `make test`: pass after append decision application moved behind sender state;
  `kacrab` lib now runs 360 unit tests, `producer_dispatcher` integration still
  runs 75 tests, and `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --features producer
  producer::sender::tests::sender_state_applies_append_dispatch_decision_then_collects_finished_dispatches
  --lib`: first failed with missing
  `ProducerSenderState::apply_append_dispatch_decision_then_collect_finished`,
  then passed after sender state started applying append dispatch decisions and
  collecting completed dispatch tasks in one sender-owned step.
- `cargo test -p kacrab --features producer
  collect_finished_consumes_successful_and_panicked_tasks --lib`: pass after
  `send` / `send_with_callback` routed post-append completion collection
  through the sender-owned helper.
- `make fmt`: pass after moving single-record post-append completion collection
  behind sender state.
- `make clippy`: first failed because `Producer::collect_finished` and
  `Producer::handle_finished_dispatches` became test-only dead code, then
  passed after gating both helpers with `#[cfg(test)]`.
- `make test`: pass after sender-owned post-append completion collection;
  `kacrab` lib now runs 361 unit tests, `producer_dispatcher` integration still
  runs 75 tests, and `producer_kafka_bench` still runs 12 unit tests.
- `git diff --check`: pass after the sender-owned post-append completion
  collection changes.
- `cargo test -p kacrab --features producer
  sender_state_finishes_batch_append_by_driving_ready_dispatch --lib`: first
  failed with missing `ProducerSenderState::finish_batch_append_dispatch`, then
  passed after sender state started owning the final ready-dispatch drive for
  batch append APIs.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_returns_delivery_handles`: pass after tracked batch
  final dispatch moved through the sender-owned helper.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_untracked_skips_delivery_handles`: pass after
  untracked batch final dispatch moved through the sender-owned helper.
- `cargo test -p kacrab --all-features
  kafka_producer_send_with_callback_invokes_callback_and_returns_delivery`:
  pass as a callback delivery guard after batch final dispatch helper changes.
- `rg -n "self\\.poll\\(\\)\\.await|finish_batch_append_dispatch|drive_ready_dispatch_until_blocked\\(\\)\\.await"
  kacrab/src/producer/client.rs kacrab/src/producer/sender.rs`: confirms
  `send_batch`, `send_batch_with_callback`, and `send_batch_untracked` now end
  through `finish_batch_append_dispatch`; the explicit public `poll()` API still
  routes to `drive_ready_dispatch_until_blocked`.
- `make fmt`: pass after moving batch final ready-dispatch drive behind sender
  state.
- `make clippy`: pass after moving batch final ready-dispatch drive behind
  sender state.
- `make test`: pass after sender-owned batch final ready-dispatch drive;
  `kacrab` lib now runs 362 unit tests, `producer_dispatcher` integration still
  runs 75 tests, and `producer_kafka_bench` still runs 12 unit tests.
- `git diff --check`: pass after the sender-owned batch final ready-dispatch
  changes.
- `cargo test -p kacrab --features producer
  sender_state_applies_batch_append_status_with_poll_budget --lib`: first
  failed with missing `ProducerSenderState::apply_batch_append_status`, then
  passed after sender state started owning batch append status interpretation,
  poll-budget observation, sticky readiness marking, and ready dispatch drive
  application.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_returns_delivery_handles`: pass after tracked batch
  append loops routed status application through sender state.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_untracked_skips_delivery_handles`: pass after
  untracked batch append loops routed status application through sender state.
- `cargo test -p kacrab --all-features
  kafka_producer_send_with_callback_invokes_callback_and_returns_delivery`:
  pass as a callback delivery guard after batch status application moved behind
  sender state.
- `make fmt`: pass after moving batch append status application behind sender
  state.
- `make clippy`: first failed on `too_many_arguments` for
  `apply_batch_append_status`, then passed after grouping dispatcher/status/topic
  in `BatchAppendStatusApplication`.
- `make test`: pass after sender-owned batch append status application;
  `kacrab` lib now runs 363 unit tests, `producer_dispatcher` integration still
  runs 75 tests, and `producer_kafka_bench` still runs 12 unit tests.
- `git diff --check`: pass after the sender-owned batch append status
  application changes.
- `cargo test -p kacrab --features producer
  producer_sender_owns_accumulator_and_state_queue_snapshot --lib`: first
  failed with unresolved `ProducerSender`, then passed after introducing a
  sender wrapper that owns both `RecordAccumulator` and `ProducerSenderState`.
- `cargo test -p kacrab --features producer
  metrics_registry_exposes_named_snapshot_values_like_java_metrics_map --lib`:
  pass after `Producer::metrics` routed queue snapshot collection through
  `ProducerSender`.
- `cargo test -p kacrab --features producer
  poll_waits_for_dispatch_slot_before_preparing_ready_batches --lib`: pass
  after `Producer` moved its accumulator and sender state fields under
  `ProducerSender`.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_returns_delivery_handles`: pass after the field
  ownership move kept batch dispatch behavior unchanged.
- `make fmt`: pass after moving accumulator and sender state ownership under
  `ProducerSender`.
- `make clippy`: pass after moving accumulator and sender state ownership under
  `ProducerSender`.
- `make test`: pass after the sender ownership wrapper change; `kacrab` lib now
  runs 364 unit tests, `producer_dispatcher` integration still runs 75 tests,
  and `producer_kafka_bench` still runs 12 unit tests.
- `git diff --check`: pass after the sender ownership wrapper changes.
- `cargo test -p kacrab --features producer
  producer_sender_append_for_delivery_owns_capacity_wait_and_append --lib`:
  first failed with missing
  `ProducerSender::append_for_delivery_with_capacity_wait`, then passed after
  the wrapper owned per-record delivery append capacity wait plus accumulator
  append.
- `cargo test -p kacrab --all-features
  kafka_producer_send_with_callback_invokes_callback_and_returns_delivery`:
  pass after `send` / `send_with_callback` routed per-record delivery append
  through `ProducerSender`.
- `cargo test -p kacrab --features producer
  send_and_batch_apis_surface_backpressure_before_dispatch --lib`: pass after
  the wrapper route preserved local backpressure behavior.
- `make fmt`: pass after routing per-record delivery append through
  `ProducerSender`.
- `make clippy`: pass after routing per-record delivery append through
  `ProducerSender`.
- `make test`: pass after routing per-record delivery append through
  `ProducerSender`; `kacrab` lib now runs 365 unit tests and
  `producer_kafka_bench` still runs 12 unit tests.
- `git diff --check`: pass after routing per-record delivery append through
  `ProducerSender`.
- `cargo test -p kacrab --features producer
  producer_sender_append_for_batch_delivery_owns_capacity_wait_and_append
  --lib`: first failed with missing
  `ProducerSender::append_for_batch_delivery_with_capacity_wait`, then passed
  after the wrapper owned tracked batch delivery append capacity wait plus
  accumulator append.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_returns_delivery_handles`: pass after `send_batch`
  routed tracked batch delivery append through `ProducerSender`.
- `cargo test -p kacrab --features producer
  producer_sender_append_untracked_owns_capacity_wait_and_append --lib`: first
  failed with missing `ProducerSender::append_untracked_with_capacity_wait`,
  then passed after the wrapper owned untracked append capacity wait plus
  accumulator append.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_untracked_skips_delivery_handles`: pass after
  `send_batch_untracked` routed untracked append through `ProducerSender`.
- `make fmt`: pass after routing tracked batch delivery and untracked append
  through `ProducerSender`.
- `make clippy`: pass after routing tracked batch delivery and untracked append
  through `ProducerSender`.
- `make test`: pass after routing tracked batch delivery and untracked append
  through `ProducerSender`; `kacrab` lib now runs 367 unit tests,
  `producer_dispatcher` integration still runs 75 tests, and
  `producer_kafka_bench` still runs 12 unit tests.
- `git diff --check`: pass after routing tracked batch delivery and untracked
  append through `ProducerSender`.
- `cargo test -p kacrab --features producer
  producer_sender_applies_append_dispatch_decision_then_collects_finished_dispatches
  --lib`: first failed with missing
  `ProducerSender::apply_append_dispatch_decision_then_collect_finished`, then
  passed after the wrapper owned single-record post-append dispatch decision
  application plus completed dispatch collection.
- `cargo test -p kacrab --all-features
  kafka_producer_send_with_callback_invokes_callback_and_returns_delivery`:
  pass after `send` / `send_with_callback` routed single-record post-append
  dispatch through `ProducerSender`.
- `cargo test -p kacrab --features producer
  producer_sender_finishes_batch_append_by_driving_ready_dispatch --lib`: first
  failed with missing `ProducerSender::finish_batch_append_dispatch`, then
  passed after the wrapper owned final batch append ready-dispatch drive.
- `cargo test -p kacrab --features producer
  producer_sender_applies_batch_append_status_with_poll_budget --lib`: first
  failed with missing `ProducerSender::apply_batch_append_status`, then passed
  after the wrapper owned batch append status application.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_returns_delivery_handles`: pass after `send_batch`
  routed batch post-append dispatch through `ProducerSender`.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_untracked_skips_delivery_handles`: pass after
  `send_batch_untracked` routed batch post-append dispatch through
  `ProducerSender`.
- `make fmt`: pass after routing post-append dispatch helpers through
  `ProducerSender`.
- `make clippy`: pass after routing post-append dispatch helpers through
  `ProducerSender`.
- `make test`: pass after routing post-append dispatch helpers through
  `ProducerSender`; `kacrab` lib now runs 370 unit tests,
  `producer_dispatcher` integration still runs 75 tests, and
  `producer_kafka_bench` still runs 12 unit tests.
- `git diff --check`: pass after routing post-append dispatch helpers through
  `ProducerSender`.
- `cargo test -p kacrab --features producer
  producer_sender_drives_ready_dispatch_until_blocked --lib`: first failed
  with missing `ProducerSender::drive_ready_dispatch_until_blocked`, then
  passed after the wrapper owned the ready-dispatch loop over its accumulator.
- `cargo test -p kacrab --features producer
  producer_sender_drives_flush_until_complete --lib`: first failed with
  missing `ProducerSender::drive_flush_until_complete`, then passed after the
  wrapper owned the flush-dispatch loop over its accumulator.
- `cargo test -p kacrab --all-features
  poll_waits_for_one_in_flight_slot_before_spawning_ready_batch --lib`: pass
  after `poll` routed the ready-dispatch loop through `ProducerSender`.
- `cargo test -p kacrab --all-features
  flush_records_local_metrics_like_java --lib`: pass after `flush` routed the
  flush-dispatch loop through `ProducerSender`.
- `make fmt`: pass after routing ready and flush dispatch loops through
  `ProducerSender`.
- `make clippy`: pass after routing ready and flush dispatch loops through
  `ProducerSender`.
- `make test`: pass after routing ready and flush dispatch loops through
  `ProducerSender`; `kacrab` lib now runs 372 unit tests,
  `producer_dispatcher` integration still runs 75 tests, and
  `producer_kafka_bench` still runs 12 unit tests.
- `git diff --check`: pass after routing ready and flush dispatch loops through
  `ProducerSender`.
- `cargo test -p kacrab --features producer
  producer_sender_waits_for_abort_completion_until_empty --lib`: first failed
  with missing `ProducerSender::wait_for_abort_completion`, then passed after
  the wrapper owned abort completion waiting over its accumulator.
- `cargo test -p kacrab --features producer
  producer_sender_discards_buffered_batches_for_abort_lifecycle --lib`: first
  failed with missing `ProducerSender::discard_buffered_batches`, then passed
  after the wrapper owned abort buffered-batch discard.
- `cargo test -p kacrab --all-features
  abort_transaction_drops_buffered_records_like_java --lib`: pass after
  `abort_transaction` routed buffered discard and abort completion through
  `ProducerSender`.
- `make fmt`: pass after routing abort buffered discard and abort completion
  through `ProducerSender`.
- `make clippy`: pass after routing abort buffered discard and abort completion
  through `ProducerSender`.
- `make test`: pass after routing abort buffered discard and abort completion
  through `ProducerSender`; `kacrab` lib now runs 374 unit tests,
  `producer_dispatcher` integration still runs 75 tests, and
  `producer_kafka_bench` still runs 12 unit tests.
- `git diff --check`: pass after routing abort buffered discard and abort
  completion through `ProducerSender`.
- `cargo test -p kacrab --features producer
  producer_sender_creates_append_poll_budget_from_in_flight_limit --lib`:
  first failed with missing `ProducerSender::append_poll_budget` and missing
  `ProducerSender::callback_append_dispatch_decision`, then passed after the
  wrapper exposed append poll-budget and callback append-decision policy.
- `cargo test -p kacrab --features producer
  producer_sender_reports_callback_append_dispatch_decision --lib`: first
  failed with missing `ProducerSender::single_append_dispatch_decision`, then
  passed after the single-send append decision moved behind `ProducerSender`.
- `cargo test -p kacrab --features producer
  producer_sender_handles_finished_dispatches_without_exposing_accumulator
  --lib`: pass after the wrapper owned finished dispatch completion handling
  over its accumulator for test helper call paths.
- `cargo test -p kacrab --features producer
  producer_sender_waits_for_handled_dispatch_without_exposing_accumulator
  --lib`: pass after the wrapper owned one-dispatch completion waiting over its
  accumulator for test helper call paths.
- `cargo test -p kacrab --features producer
  producer_sender_handles_completed_dispatch_without_exposing_accumulator
  --lib`: pass after the wrapper owned direct completed-dispatch handling over
  its accumulator for test helper call paths.
- `make fmt`: pass after moving append poll-budget and append-decision helpers
  through `ProducerSender`.
- `make clippy`: pass after moving append poll-budget and append-decision
  helpers through `ProducerSender`.
- `make test`: pass after moving append poll-budget and append-decision helpers
  through `ProducerSender`; `kacrab` lib now runs 379 unit tests,
  `producer_dispatcher` integration still runs 75 tests, and
  `producer_kafka_bench` still runs 12 unit tests.
- `git diff --check`: pass after moving append poll-budget and append-decision
  helpers through `ProducerSender`.
- `cargo test -p kacrab --features producer
  producer_sender_assigns_partition_with_accumulator_without_exposing_accumulator
  --lib`: first failed with missing
  `ProducerSender::assign_partition_with_accumulator`, missing
  `ProducerSender::refresh_partition_load_stats_with_metadata`, and missing
  `ProducerSender::refresh_topic_load_stats_with_metadata`, then passed after
  the wrapper owned read-only accumulator access for default partition
  assignment.
- `cargo test -p kacrab --features producer
  producer_sender_refreshes_empty_partition_load_stats_without_exposing_accumulator
  --lib`: first failed with the same missing `ProducerSender` accumulator-load
  wrapper methods, then passed after the wrapper owned read-only accumulator
  access for partition load-stat refresh.
- `cargo test -p kacrab --features producer
  producer_sender_refreshes_topic_load_stats_without_exposing_accumulator
  --lib`: first failed with the same missing `ProducerSender` accumulator-load
  wrapper methods, then passed after the wrapper owned read-only accumulator
  access for topic load-stat refresh.
- `make fmt`: pass after routing read-only accumulator access for partition
  assignment and load-stat refresh through `ProducerSender`.
- `make clippy`: pass after routing read-only accumulator access for partition
  assignment and load-stat refresh through `ProducerSender`.
- `make test`: pass after routing read-only accumulator access for partition
  assignment and load-stat refresh through `ProducerSender`; `kacrab` lib now
  runs 382 unit tests, `producer_dispatcher` integration still runs 75 tests,
  and `producer_kafka_bench` still runs 12 unit tests.
- `git diff --check`: pass after routing read-only accumulator access for
  partition assignment and load-stat refresh through `ProducerSender`.
- `cargo test -p kacrab --features producer
  producer_sender_drives_ready_dispatch_until_blocked --lib`: first failed
  because `ProducerSender::drive_ready_dispatch_until_blocked` still required a
  caller-provided `&ProducerDispatcher`, then passed after `ProducerSender`
  started carrying a dispatcher clone for the ready-dispatch wrapper.
- `cargo test -p kacrab --all-features
  poll_waits_for_one_in_flight_slot_before_spawning_ready_batch --lib`: pass
  after `Producer::poll` routed ready dispatch through the sender-owned
  dispatcher clone.
- `cargo test -p kacrab --all-features
  poll_waits_until_blocked_in_flight_task_completes --lib`: pass after
  `Producer::poll` routed completion handling plus ready dispatch through the
  sender-owned dispatcher clone.
- `cargo test -p kacrab --all-features
  kafka_producer_single_send_budget_coalesces_ready_partitions --test
  producer_dispatcher`: pass after the ready-dispatch wrapper used the
  sender-owned dispatcher clone.
- `make fmt`: pass after moving the ready-dispatch wrapper's dispatcher access
  into `ProducerSender`.
- `make clippy`: pass after moving the ready-dispatch wrapper's dispatcher
  access into `ProducerSender`.
- `make test`: pass after moving the ready-dispatch wrapper's dispatcher access
  into `ProducerSender`; `kacrab` lib still runs 382 unit tests,
  `producer_dispatcher` integration still runs 75 tests, and
  `producer_kafka_bench` still runs 12 unit tests.
- `git diff --check`: pass after moving the ready-dispatch wrapper's dispatcher
  access into `ProducerSender`.
- `cargo test -p kacrab --features producer
  producer_sender_finishes_batch_append_by_driving_ready_dispatch --lib`: first
  failed because `ProducerSender::finish_batch_append_dispatch` still required a
  caller-provided `&ProducerDispatcher`, then passed after the wrapper used the
  dispatcher clone owned by `ProducerSender`.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_returns_delivery_handles --test producer_dispatcher`:
  pass after final batch append dispatch used the sender-owned dispatcher clone.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_untracked_skips_delivery_handles --test
  producer_dispatcher`: pass after final batch append dispatch used the
  sender-owned dispatcher clone.
- `make fmt`: pass after moving final batch append dispatcher's access into
  `ProducerSender`.
- `make clippy`: pass after moving final batch append dispatcher's access into
  `ProducerSender`.
- `make test`: pass after moving final batch append dispatcher's access into
  `ProducerSender`; `kacrab` lib still runs 382 unit tests,
  `producer_dispatcher` integration still runs 75 tests, and
  `producer_kafka_bench` still runs 12 unit tests.
- `git diff --check`: pass after moving final batch append dispatcher's access
  into `ProducerSender`.
- `cargo test -p kacrab --features producer
  producer_sender_drives_flush_until_complete --lib`: first failed because
  `ProducerSender::drive_flush_until_complete` still required a
  caller-provided `&ProducerDispatcher`, then passed after the wrapper used the
  dispatcher clone owned by `ProducerSender`.
- `cargo test -p kacrab --all-features
  flush_records_local_metrics_like_java --lib`: pass after flush dispatch used
  the sender-owned dispatcher clone.
- `cargo test -p kacrab --all-features
  flush_waits_for_in_flight_slot_and_reports_local_delivery_timeout --lib`:
  pass after flush dispatch used the sender-owned dispatcher clone.
- Initial `make test` after the flush-dispatch wrapper change failed three
  producer dispatcher metric/counter tests because `Producer::enable_metrics`
  enabled only the original dispatcher, not the clone owned by
  `ProducerSender`. This confirmed the wrapper clone had to stay configuration
  synchronized before the change could be accepted.
- `cargo test -p kacrab --all-features
  kafka_producer_10kib_records_keep_observed_requests_under_max_request_size
  --test producer_dispatcher`: pass after adding `ProducerSender::enable_metrics`
  and routing `Producer::enable_metrics` through it.
- `cargo test -p kacrab --all-features
  kafka_producer_metrics_snapshot_reports_queue_and_dispatch_counters --test
  producer_dispatcher`: pass after sender-owned dispatcher metrics were
  enabled with the producer dispatcher.
- `cargo test -p kacrab --all-features
  kafka_producer_single_send_budget_coalesces_ready_partitions --test
  producer_dispatcher`: pass after sender-owned dispatcher metrics were
  enabled with the producer dispatcher.
- `make clippy`: pass after moving flush-dispatch wrapper dispatcher access
  into `ProducerSender` and synchronizing metrics enablement for the
  sender-owned dispatcher clone.
- `make test`: pass after moving flush-dispatch wrapper dispatcher access into
  `ProducerSender` and synchronizing metrics enablement for the sender-owned
  dispatcher clone.
- `cargo test -p kacrab --features producer
  producer_sender_append_for_delivery_owns_capacity_wait_and_append --lib`:
  first failed with missing
  `ProducerSender::append_delivery_record_with_capacity_wait`, then passed after
  the wrapper used its sender-owned dispatcher clone for per-record delivery
  append capacity waits.
- `cargo test -p kacrab --features producer
  producer_sender_append_untracked_owns_capacity_wait_and_append --lib`: first
  failed with missing
  `ProducerSender::append_untracked_record_with_capacity_wait`, then passed
  after the wrapper used its sender-owned dispatcher clone for untracked append
  capacity waits.
- `cargo test -p kacrab --features producer
  producer_sender_append_for_batch_delivery_owns_capacity_wait_and_append
  --lib`: first failed with missing
  `ProducerSender::append_batch_delivery_record_with_capacity_wait`, then
  passed after the wrapper used its sender-owned dispatcher clone for tracked
  batch delivery append capacity waits. A following `make clippy` failed on too
  many method arguments, so the batch-delivery wrapper now accepts
  `AppendBatchDeliveryRecord`, which carries batch append input without carrying
  a dispatcher reference.
- `cargo test -p kacrab --features producer
  producer_sender_applies_append_dispatch_decision_then_collects_finished_dispatches
  --lib`: first failed because the wrapper still expected
  `AppendDispatchApplication` with a caller-provided dispatcher, then passed
  after `ProducerSender` constructed that application from its owned dispatcher
  clone.
- `cargo test -p kacrab --features producer
  producer_sender_applies_batch_append_status_with_poll_budget --lib`: first
  failed because the wrapper still expected `BatchAppendStatusApplication` with
  a caller-provided dispatcher, then passed after `ProducerSender` constructed
  that application from its owned dispatcher clone.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_returns_delivery_handles --test producer_dispatcher`:
  pass after append capacity and post-append dispatch application wrappers used
  sender-owned dispatcher access.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_untracked_skips_delivery_handles --test
  producer_dispatcher`: pass after untracked append capacity used sender-owned
  dispatcher access.
- `cargo test -p kacrab --all-features
  kafka_producer_send_with_callback_invokes_callback_and_returns_delivery
  --test producer_dispatcher`: pass after per-record delivery append capacity
  used sender-owned dispatcher access.
- `cargo test -p kacrab --all-features
  kafka_producer_single_send_budget_coalesces_ready_partitions --test
  producer_dispatcher`: pass after post-append dispatch application used
  sender-owned dispatcher access.
- `make fmt`: pass after moving append capacity and post-append dispatch
  application dispatcher access into `ProducerSender`.
- `make clippy`: pass after moving append capacity and post-append dispatch
  application dispatcher access into `ProducerSender`.
- `make test`: pass after moving append capacity and post-append dispatch
  application dispatcher access into `ProducerSender`; `kacrab` lib still runs
  382 unit tests, `producer_dispatcher` integration still runs 75 tests, and
  `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --features producer
  producer_sender_assigns_partition_with_accumulator_without_exposing_accumulator
  --lib`: first failed because `ProducerSender::assign_partition_with_accumulator`
  still required a caller-provided `&ProducerDispatcher`, then passed after the
  wrapper used its sender-owned dispatcher clone.
- `cargo test -p kacrab --features producer
  producer_sender_refreshes_empty_partition_load_stats_without_exposing_accumulator
  --lib`: first failed because
  `ProducerSender::refresh_partition_load_stats_with_metadata` still required a
  caller-provided `&ProducerDispatcher`, then passed after the wrapper used its
  sender-owned dispatcher clone.
- `cargo test -p kacrab --features producer
  producer_sender_refreshes_topic_load_stats_without_exposing_accumulator
  --lib`: first failed because
  `ProducerSender::refresh_topic_load_stats_with_metadata` still required a
  caller-provided `&ProducerDispatcher`, then passed after the wrapper used its
  sender-owned dispatcher clone.
- `cargo test -p kacrab --features producer
  producer_sender_reports_default_sticky_partitioner_policy --lib`: first
  failed with missing `ProducerSender::uses_sticky_partitioner`, then passed
  after sticky-partitioner policy lookup moved behind the sender-owned
  dispatcher clone.
- `cargo test -p kacrab --all-features
  kafka_producer_builder_uses_native_partitioner_instead_of_jvm_class_loading
  --test producer_dispatcher`: pass after default partition assignment and
  load-stat refresh wrappers used sender-owned dispatcher access.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_returns_delivery_handles --test producer_dispatcher`:
  pass after topic planning and batch default partition refresh wrappers used
  sender-owned dispatcher access.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_untracked_skips_delivery_handles --test
  producer_dispatcher`: pass after topic planning and untracked batch default
  partition refresh wrappers used sender-owned dispatcher access.
- `cargo test -p kacrab --all-features
  kafka_producer_single_send_budget_coalesces_ready_partitions --test
  producer_dispatcher`: pass after sticky-partitioner policy lookup used
  sender-owned dispatcher access.
- `make fmt`: pass after moving read-only accumulator/default partition helper
  dispatcher access and sticky-partitioner policy lookup into `ProducerSender`.
- `make clippy`: pass after moving read-only accumulator/default partition
  helper dispatcher access and sticky-partitioner policy lookup into
  `ProducerSender`.
- `make test`: pass after moving read-only accumulator/default partition helper
  dispatcher access and sticky-partitioner policy lookup into `ProducerSender`;
  `kacrab` lib now runs 383 unit tests, `producer_dispatcher` integration still
  runs 75 tests, and `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --features producer
  producer_sender_refreshes_and_assigns_topic_partitions_with_metadata --lib`:
  first failed with missing
  `ProducerSender::refresh_and_assign_topic_partitions_with_metadata`, then
  passed after `ProducerSender` wrapped load-stat refresh plus topic partition
  assignment through its owned dispatcher clone.
- `cargo test -p kacrab --features producer
  producer_sender_refreshes_and_assigns_multiple_topics_with_metadata --lib`:
  first failed with missing
  `ProducerSender::refresh_and_assign_partitions_with_metadata`, then passed
  after `ProducerSender` wrapped multi-topic load-stat refresh plus partition
  assignment through its owned dispatcher clone.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_returns_delivery_handles --test producer_dispatcher`:
  pass after tracked batch default partition assignment used the sender-owned
  refresh-and-assign wrapper.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_untracked_skips_delivery_handles --test
  producer_dispatcher`: pass after untracked batch default partition assignment
  used the sender-owned refresh-and-assign wrapper.
- `make fmt`: pass after moving batch default partition refresh-and-assign
  coordination into `ProducerSender`.
- `make clippy`: pass after moving batch default partition refresh-and-assign
  coordination into `ProducerSender`.
- `make test`: pass after moving batch default partition refresh-and-assign
  coordination into `ProducerSender`; `kacrab` lib now runs 385 unit tests,
  `producer_dispatcher` integration still runs 75 tests, and
  `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --features producer
  producer_sender_fetches_metadata_through_owned_dispatcher --lib`: first
  failed with missing `ProducerSender::metadata_for_topics`, then passed after
  `ProducerSender` wrapped metadata fetch through its owned dispatcher clone.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_returns_delivery_handles --test producer_dispatcher`:
  pass after tracked batch send metadata fetch used the sender-owned wrapper.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_untracked_skips_delivery_handles --test
  producer_dispatcher`: pass after untracked batch send metadata fetch used the
  sender-owned wrapper.
- `cargo test -p kacrab --all-features
  kafka_producer_send_with_callback_invokes_callback_and_returns_delivery --test
  producer_dispatcher`: pass after per-record callback send custom-partitioner
  metadata fetch used the sender-owned wrapper.
- `cargo test -p kacrab --all-features
  kafka_producer_builder_uses_native_partitioner_instead_of_jvm_class_loading
  --test producer_dispatcher`: pass after custom partition assignment metadata
  fetch used the sender-owned wrapper.
- `make fmt`: pass after moving send-path metadata fetches into
  `ProducerSender`.
- `make clippy`: pass with `-D warnings` after moving send-path metadata
  fetches into `ProducerSender`.
- `make test`: pass after moving send-path metadata fetches into
  `ProducerSender`; `kacrab` lib now runs 386 unit tests,
  `producer_dispatcher` integration still runs 75 tests, and
  `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --features producer
  producer_sender_checks_transaction_error_through_owned_dispatcher --lib`:
  first failed with missing `ProducerSender::fail_if_transaction_error`, then
  passed after `ProducerSender` wrapped transaction-error guard checks through
  its owned dispatcher clone.
- `cargo test -p kacrab --all-features
  kafka_producer_send_with_callback_invokes_callback_and_returns_delivery --test
  producer_dispatcher`: pass after per-record callback send used the
  sender-owned transaction-error guard.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_returns_delivery_handles --test producer_dispatcher`:
  pass after tracked batch send used the sender-owned transaction-error guard.
- `cargo test -p kacrab --all-features
  kafka_producer_send_batch_untracked_skips_delivery_handles --test
  producer_dispatcher`: pass after untracked batch send used the sender-owned
  transaction-error guard.
- `cargo test -p kacrab --features producer
  send_with_callback_local_api_error_runs_user_callback_before_interceptor_like_java
  --lib`: pass after per-record callback send used the sender-owned
  transaction-error guard.
- `cargo test -p kacrab --features producer
  transactional_send_rejects_previous_pending_operation_like_java --lib`: pass
  after send-family transaction-error guard checks used `ProducerSender`.
- `make fmt`: pass after moving send-family transaction-error guard checks into
  `ProducerSender`.
- `make clippy`: pass with `-D warnings` after moving send-family
  transaction-error guard checks into `ProducerSender`.
- `make test`: pass after moving send-family transaction-error guard checks
  into `ProducerSender`; `kacrab` lib now runs 387 unit tests,
  `producer_dispatcher` integration still runs 75 tests, and
  `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --features producer
  producer_sender_assigns_partition_with_metadata_through_owned_dispatcher --lib`:
  first failed with missing `ProducerSender::assign_partition_with_metadata`,
  then passed after `ProducerSender` wrapped metadata-backed default partition
  assignment through its owned dispatcher clone.
- `cargo test -p kacrab --all-features
  kafka_producer_builder_uses_native_partitioner_instead_of_jvm_class_loading
  --test producer_dispatcher`: pass after custom partitioner fallback
  assignment used the sender-owned wrapper.
- `cargo test -p kacrab --all-features
  kafka_producer_single_send_budget_coalesces_ready_partitions --test
  producer_dispatcher`: pass after metadata-backed default partition assignment
  fallback used the sender-owned wrapper.
- `make fmt`: pass after moving metadata-backed default partition assignment
  into `ProducerSender`.
- `make clippy`: pass with `-D warnings` after moving metadata-backed default
  partition assignment into `ProducerSender`.
- `make test`: pass after moving metadata-backed default partition assignment
  into `ProducerSender`; `kacrab` lib now runs 388 unit tests,
  `producer_dispatcher` integration still runs 75 tests, and
  `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --features producer
  producer_sender_exposes_metrics_handle_from_owned_dispatcher --lib`: first
  failed with missing `ProducerSender::metrics_handle`, then passed after
  `ProducerSender` wrapped the metrics handle from its owned dispatcher clone.
- `cargo test -p kacrab --features producer
  metrics_registry_exposes_named_snapshot_values_like_java_metrics_map --lib`:
  pass after `Producer::from_parts` obtained the producer metrics handle through
  `ProducerSender`.
- `cargo test -p kacrab --all-features
  kafka_producer_metrics_snapshot_reports_queue_and_dispatch_counters --test
  producer_dispatcher`: pass after `Producer::from_parts` obtained the producer
  metrics handle through `ProducerSender`.
- `make fmt`: pass after moving producer construction metrics handle setup into
  `ProducerSender`.
- `make clippy`: pass with `-D warnings` after moving producer construction
  metrics handle setup into `ProducerSender`.
- `make test`: pass after moving producer construction metrics handle setup into
  `ProducerSender`; `kacrab` lib now runs 389 unit tests,
  `producer_dispatcher` integration still runs 75 tests, and
  `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --features producer
  producer_sender_reports_next_ready_at_for_linger_scheduling --lib`: first
  failed with missing `ProducerSender::next_ready_at`, then passed after
  `ProducerSender` exposed the accumulator's next linger readiness deadline for
  future sender-loop scheduling.
- `cargo test -p kacrab --features producer
  next_ready_at_reports_earliest_linger_deadline --lib`: pass after
  `ProducerSender::next_ready_at` delegated to the accumulator readiness
  primitive.
- `cargo test -p kacrab --features producer
  poll_waits_for_dispatch_slot_before_preparing_ready_batches --lib`: pass after
  `ProducerSender::next_ready_at` was added for sender-loop readiness
  scheduling.
- `cargo test -p kacrab --features producer
  producer_sender_append_untracked_owns_capacity_wait_and_append --lib`: pass
  after the untracked append capacity wait loop moved into `ProducerSender` and
  used sender-owned readiness scheduling.
- `cargo test -p kacrab --features producer
  producer_sender_append_for_delivery_owns_capacity_wait_and_append --lib`:
  pass after the per-record delivery append capacity wait loop moved into
  `ProducerSender`.
- `cargo test -p kacrab --features producer
  producer_sender_append_for_batch_delivery_owns_capacity_wait_and_append
  --lib`: pass after the tracked batch delivery append capacity wait loop moved
  into `ProducerSender`.
- `make fmt`: pass after moving append capacity wait scheduling into
  `ProducerSender`.
- `make clippy`: pass with `-D warnings` after moving append capacity wait
  scheduling into `ProducerSender`.
- `make test`: pass after moving append capacity wait scheduling into
  `ProducerSender`; `kacrab` lib now runs 390 unit tests,
  `producer_dispatcher` integration still runs 75 tests, and
  `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --features producer
  producer_sender_reports_next_wake_action_for_sender_loop --lib`: first failed
  with missing `SenderWakeAction` / `ProducerSender::next_wake_action`, then
  passed after adding sender-owned wake decisions for parked, dispatch-ready,
  sleep-until-linger, and wait-for-dispatch states.
- `cargo test -p kacrab --features producer buffer_wait --lib`: pass after
  `ProducerSender::buffer_wait_action` was routed through
  `ProducerSender::next_wake_action`.
- `make fmt`: pass after adding the sender wake-decision primitive.
- `make clippy`: pass with `-D warnings` after adding the sender wake-decision
  primitive.
- `make test`: pass after adding the sender wake-decision primitive; `kacrab`
  lib now runs 391 unit tests, `producer_dispatcher` integration still runs 75
  tests, and `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --features producer
  producer_sender_drives_one_wake_step_for_sender_loop --lib`: first failed
  with missing `SenderWakeStep` / `ProducerSender::drive_wake_step`, then
  passed after adding a sender-owned single-step loop primitive for completion,
  ready-dispatch, sleep-until-linger, and parked states.
- `cargo test -p kacrab --features producer wait_for_buffer_progress --lib`:
  pass after routing `ProducerSender::wait_for_buffer_progress` through the
  new `ProducerSender::drive_wake_step` immediate-work path.
- `make fmt`: pass after adding the sender single-step primitive.
- `make clippy`: pass with `-D warnings` after adding the sender single-step
  primitive.
- `make test`: pass after adding the sender single-step primitive; `kacrab` lib
  now runs 392 unit tests, `producer_dispatcher` integration still runs 75
  tests, and `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --features producer
  producer_sender_drives_available_wake_work_until_waiting --lib`: first
  failed with missing `SenderLoopWait` /
  `ProducerSender::drive_wake_until_waiting`, then passed after adding a
  sender-owned loop primitive that handles completed dispatches, dispatches
  ready batches, and returns the next wait state without awaiting newly spawned
  dispatch work.
- `cargo test -p kacrab --features producer wait_for_buffer_progress --lib`:
  pass after routing the `PollReady` buffer-progress branch through
  `ProducerSender::drive_wake_until_waiting`.
- `make fmt`: pass after adding the sender drive-until-waiting primitive.
- `make clippy`: pass with `-D warnings` after adding the sender
  drive-until-waiting primitive.
- `make test`: pass after adding the sender drive-until-waiting primitive;
  `kacrab` lib now runs 393 unit tests, `producer_dispatcher` integration still
  runs 75 tests, and `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --features producer
  producer_sender_notifies_sender_loop_after_untracked_append --lib`: first
  failed with missing `ProducerSender::sender_loop_notifier`, then passed after
  adding sender-loop wake notification and notifying after successful untracked
  append.
- `cargo test -p kacrab --features producer producer_sender_append --lib`: pass
  after adding sender-loop wake notification to untracked, per-record delivery,
  and tracked batch delivery append wrappers.
- `make fmt`: pass after adding sender-loop wake notification.
- `make clippy`: pass with `-D warnings` after adding sender-loop wake
  notification.
- `make test`: pass after adding sender-loop wake notification; `kacrab` lib
  now runs 394 unit tests, `producer_dispatcher` integration still runs 75
  tests, and `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --features producer
  producer_sender_delay_wait_wakes_on_sender_loop_notification --lib`: first
  failed with missing `SenderWaitSignal` /
  `ProducerSender::wait_for_sender_loop_delay`, then passed after adding a
  sender-loop delay wait primitive that can wake on the sender notifier.
- `cargo test -p kacrab --features producer wait_for_buffer_progress --lib`:
  pass after routing the buffer-progress sleep branch through
  `ProducerSender::wait_for_sender_loop_delay`.
- `make fmt`: pass after adding sender-loop delay waiting.
- `make clippy`: pass with `-D warnings` after adding sender-loop delay
  waiting.
- `make test`: pass after adding sender-loop delay waiting; `kacrab` lib now
  runs 395 unit tests, `producer_dispatcher` integration still runs 75 tests,
  and `producer_kafka_bench` still runs 12 unit tests.
- `cargo test -p kacrab --features producer
  producer_sender_loop_wait_handles_dispatch_completion --lib`: first failed
  with missing `ProducerSender::wait_for_sender_loop_wait` and
  `SenderWaitSignal::DispatchCompleted`, then passed after adding a sender-loop
  wait-state primitive for dispatch completion, linger sleep, and parked
  notification waits.
- `cargo test -p kacrab --features producer wait_for_buffer_progress --lib`:
  pass after routing the sender wake-step dispatch-completion branch through
  `ProducerSender::wait_for_sender_loop_wait`.
- `make fmt`: pass after adding sender-loop wait-state handling.
- `make clippy`: pass with `-D warnings` after adding sender-loop wait-state
  handling.
- `make test`: pass after adding sender-loop wait-state handling; `kacrab` lib
  now runs 396 unit tests, `producer_dispatcher` integration still runs 75
  tests, and `producer_kafka_bench` still runs 12 unit tests.
- `git diff --check`: pass after adding sender-loop wait-state handling.
- `cargo test -p kacrab --features producer
  producer_sender_loop_tick_waits_after_reaching_dispatch_completion --lib`:
  first failed with missing `ProducerSender::drive_sender_loop_once`, then
  passed after adding a sender-owned loop-tick primitive that drives immediate
  work to the next wait state and awaits that wait state.
- `cargo test -p kacrab --features producer producer_sender_loop --lib`: pass
  after adding the loop-tick primitive.
- `cargo test -p kacrab --features producer wait_for_buffer_progress --lib`:
  pass after routing the in-flight buffer-progress branch through
  `ProducerSender::drive_sender_loop_once`.
- `make fmt`: pass after adding sender-loop tick handling.
- `make clippy`: first failed because `drive_sender_loop_once` was not used in
  non-test code; after routing the in-flight buffer-progress branch through it
  and scoping the older single-step helper to tests, `make clippy` passed with
  `-D warnings`.
- `make test`: pass after adding sender-loop tick handling; `kacrab` lib now
  runs 397 unit tests, `producer_dispatcher` integration still runs 75 tests,
  and `producer_kafka_bench` still runs 12 unit tests.
- `git diff --check`: pass after adding sender-loop tick handling.
- `cargo test -p kacrab --features producer
  producer_sender_buffer_progress_dispatches_after_linger_sleep --lib`: first
  failed because `ProducerSender::wait_for_buffer_progress` returned after a
  single sleep tick without dispatching the newly ready batch, then passed after
  looping the sleep branch until buffer progress is made or the deadline elapses.
- `cargo test -p kacrab --features producer wait_for_buffer_progress --lib`:
  pass after changing buffer progress sleep handling.
- `make fmt`: pass after changing buffer progress sleep handling.
- `make clippy`: pass with `-D warnings` after changing buffer progress sleep
  handling.
- `make test`: pass after changing buffer progress sleep handling; `kacrab` lib
  now runs 398 unit tests, `producer_dispatcher` integration still runs 75
  tests, and `producer_kafka_bench` still runs 12 unit tests.
- `git diff --check`: pass after changing buffer progress sleep handling.
- `make bench-kafka`: pass against local Kafka at `127.0.0.1:9092` with
  `KACRAB_BENCH_API=per-record`. Current 5-run Rust averages:
  - `5,000,000 messages x 10 bytes`: `2,083,701 messages/s`, `19.872 MB/s`.
    Per-run counters stayed at zero retries/errors/requeues/splits; records per
    request were about `909.6-909.9`.
  - `100,000 messages x 10 KiB`: `32,328 messages/s`, `315.700 MB/s`.
    Per-run counters stayed at zero retries/errors/requeues/splits; records per
    request were about `2.828-2.845`.
- `make bench-kafka-java-default`: pass against the same local Kafka broker and
  topic. Current Java 5-run averages:
  - `5,000,000 messages x 10 bytes`: `3,579,483 messages/s`, `34.136 MB/s`.
    `records_per_request_avg` ranged about `949.668-975.991`.
  - `100,000 messages x 10 KiB`: `40,113 messages/s`, `391.726 MB/s`.
    `records_per_request_avg=3` for all five runs.
- This fresh same-machine benchmark still shows Rust below Java in both default
  scenarios. Do not report parity.

Latest session update, 2026-06-19:

- Added a first production `Producer` background sender loop for non-zero
  linger configs. It shares `ProducerSender` state behind an async mutex, wakes
  from append notifications and linger deadlines, and aborts on producer drop.
  It is deliberately gated to `accumulator.linger > 0` so existing zero-linger
  manual-dispatch semantics and deterministic requeue tests remain stable.
- Added integration coverage that failed before this change:
  `kafka_producer_background_sender_dispatches_after_linger_without_flush`.
  The test verifies a `send_with_callback` record dispatches after linger
  without an explicit `flush`.
- Preserved local transaction-error behavior by checking producer send paths
  directly through `ProducerDispatcher::fail_if_transaction_error`, so public
  sends do not wait behind the sender mutex when a fatal transaction error is
  already known. Abort uses the dispatcher rule that allows abortable errors
  but rejects fatal transaction errors before waiting for buffered dispatches.
- Fresh verification after these changes:
  - `make fmt`: pass.
  - `make clippy`: pass with `-D warnings`.
  - `make test`: pass; `kacrab` lib ran 400 tests,
    `producer_dispatcher` ran 76 tests, and `producer_kafka_bench` ran 12 unit
    tests.
- Fresh Rust benchmark, local Kafka `127.0.0.1:9092`, topic `kacrab-bench`,
  default per-record API:
  - `5,000,000 messages x 10 bytes`: `1,076,823 messages/s`, `10.269 MB/s`
    average over 5 runs. Counters showed high batch fill
    (`batch_fill ~= 0.964-0.994`) and about `902-909` records/request, but
    throughput regressed substantially versus the previous Rust snapshot.
  - `100,000 messages x 10 KiB`: `35,887 messages/s`, `350.456 MB/s` average
    over 5 runs. Counters stayed at zero retries/errors/requeues, with
    `records_per_request_avg ~= 2.817-2.839`.
- Fresh Java benchmark, same local Kafka/topic/default wrapper:
  - `5,000,000 messages x 10 bytes`: `3,920,599 messages/s`, `37.390 MB/s`
    average over 5 runs. `records_per_request_avg ~= 954.563-957.854`.
  - `100,000 messages x 10 KiB`: `40,782 messages/s`, `398.260 MB/s` average
    over 5 runs. `records_per_request_avg=3` for all five runs.
- Current conclusion: Rust is not parity. The background sender step improves
  Java-like linger behavior and raises the fresh 10 KiB average versus the last
  accepted Rust number, but it regresses the 10B scenario badly. The next
  performance fix should reduce sender mutex/loop contention and avoid making
  small-record throughput pay for linger-driven dispatch.

Follow-up session update, 2026-06-19:

- Added `AppendStatus::starts_new_batch` so sender wakeups can distinguish the
  first append that starts a linger timer from later pending appends into the
  same non-ready batch.
- Changed `ProducerSender` append wake policy to notify the background sender
  only when an append starts a new batch or makes a batch ready. Focused tests:
  `append_status_reports_new_batch_for_linger_wakeup_policy`,
  `producer_sender_notifies_sender_loop_after_untracked_append`, and
  `producer_sender_skips_redundant_pending_append_notify`.
- Reduced callback hot-path sender mutex round trips by adding
  `append_callback_delivery_record_with_capacity_wait`, which appends the
  tracked record and returns the callback append dispatch decision under one
  sender lock. Focused test:
  `producer_sender_appends_callback_delivery_and_returns_dispatch_decision`.
- Fresh verification after these changes:
  - `make fmt`: pass.
  - `make clippy`: pass with `-D warnings`.
  - `make test`: pass; `kacrab` lib ran 403 tests,
    `producer_dispatcher` ran 76 tests, and `producer_kafka_bench` ran 12 unit
    tests.
- Fresh Rust benchmark, local Kafka `127.0.0.1:9092`, topic `kacrab-bench`,
  default per-record API:
  - `5,000,000 messages x 10 bytes`: `1,232,629 messages/s`, `11.755 MB/s`
    average over 5 runs. This improves over the immediate background-sender
    regression snapshot (`1,076,823 messages/s`) but remains far below the
    pre-background accepted Rust snapshot and Java.
  - `100,000 messages x 10 KiB`: `33,624 messages/s`, `328.356 MB/s` average
    over 5 runs, with zero retries/errors/requeues and
    `records_per_request_avg ~= 2.825-2.831`.
- Fresh Java benchmark, same local Kafka/topic/default wrapper:
  - `5,000,000 messages x 10 bytes`: `3,820,732 messages/s`, `36.436 MB/s`
    average over 5 runs.
  - `100,000 messages x 10 KiB`: `38,295 messages/s`, `373.976 MB/s` average
    over 5 runs, with `records_per_request_avg=3`.
- Current conclusion remains: Rust is not parity. Notify throttling and one
  fewer callback lock improve 10B somewhat, but the main gap is still the
  sender hot path sitting behind `tokio::Mutex` plus public-call-driven append
  work. Next work should split the append accumulator fast path from the
  async dispatch owner instead of adding more callback thresholds.

This benchmark output is not Java parity proof yet.

## Status Matrix

| Area | Java oracle | Rust status | Required evidence before done |
| --- | --- | --- | --- |
| Effective config snapshot, Rust | Kafka producer defaults plus only bootstrap/client id overridden | Partially present in `producer_kafka_bench` output | Unit test covering all audit keys; one real run output showing snapshot |
| Effective config snapshot, Java | Same config table from `ProducerConfig` effective values | Present in Java wrapper output | Java runner or wrapper prints same key list for each of 5 runs |
| ProducerPerformance send API | One `producer.send(record, cb)` per record | Rust bench now has `per-record` path | Unit test locks default bench API; real run output shows `delivery_mode=per-record` |
| Latency timestamp semantics | One start timestamp per record; callback uses that timestamp | Rust per-record and batched benchmark paths use one start timestamp per record and successful-only accounting; batch delivery callback order is covered by accumulator/record tests, and benchmark accounting now has focused unit coverage | Keep test coverage green when changing callback delivery or benchmark send loops |
| Five-run Java averaging | Java must run 5 times per scenario and average with same formula | Present in `bench-kafka-java-default` | Make target/script runs 5x each scenario and prints same average formula |
| Produce request counters | Java sender records records/request, batch size, retry, error, latency, split | Partial: Rust emits exact request/batch counters, generated encoded `request_size_avg`, and separate `request_splits`; Java producer-perf exposes records/request, request-size, retries/errors/splits, but not exact record batch count or in-flight stall count. Rust `batch_splits` remains `not_tracked` until Java-style ProducerBatch split-on-`MESSAGE_TOO_LARGE` exists. | Rust and Java output include produce requests, record batches, records/batch, request size, retries, errors, in-flight stalls |
| Buffer memory / max.block.ms | Java `BufferPool.allocate` blocks and records wait/exhaustion | Rust has buffer.memory backpressure, parity not proven | Tests cover wait, timeout, and counters against Java behavior |
| RecordAccumulator batching | Java appends into existing per-partition ProducerBatch before allocating new batch | Rust has RecordAccumulator and a new `next_ready_at` primitive for sender-loop linger scheduling, but parity is not proven at Java edge cases | Oracle tests for existing-batch append, new-batch creation, full-batch wakeup, linger readiness |
| Sender ownership shape | Java has a dedicated Sender loop that owns drain/readiness/network progression | Rust now has `producer/sender.rs` owning sender dispatch scheduling state plus a `ProducerSender` wrapper that owns the accumulator and state together. Sender owns the ready-batch plus bounded-slot plus drain-and-prepare path for `poll`, the drain-all plus bounded-slot plus prepare path for `flush`, append poll budget policy and state, per-record delivery append, tracked batch delivery append, and untracked append capacity wait plus accumulator append through the wrapper, batch append status application, append post-dispatch decision application plus single-record completion collection, batch final ready-dispatch drive, the pending buffered-or-in-flight backpressure predicate, buffer-wait poll/sleep/dispatch-completion decisions, flush/abort in-flight wait predicate, in-flight dispatch count for metrics, dispatch task body construction, blocking and non-blocking dispatch completion waits, bounded in-flight slot waiting policy, spawn-time partition reservation, the dispatch task `JoinSet`, and joined-dispatch partition cleanup. `ProducerSender` now wraps the public send-loop append poll-budget and append-decision helpers, post-append dispatch helpers, ready/flush dispatch loops, abort buffered discard/completion wait, test-only completed-dispatch helpers, and read-only accumulator access for default partition assignment plus adaptive load-stat refresh. The ready-dispatch, final batch append dispatch, flush-dispatch, append capacity, post-append dispatch application, read-only accumulator/default partition helper, sticky-partitioner policy, batch metadata refresh-and-assign, send-path metadata fetch, send-path transaction-error guard, metadata-backed default partition assignment, constructor metrics-handle, and linger readiness-deadline wrappers now use state owned by `ProducerSender`, so `Producer::poll`, `Producer::flush`, public send append paths, public batch send endings, default partition assignment, adaptive load-stat refresh, topic-plan sticky checks, batch default partition assignment, batch send metadata fetch, custom partition assignment metadata fetch, send-family transaction-error checks, custom-partitioner fallback assignment, producer construction metrics handle setup, and future sender-loop sleep scheduling no longer need direct dispatcher/accumulator/state access for those paths; public metadata APIs such as `partitions_for` still call dispatcher directly, and public API calls still drive drain/dispatch | Move accumulator draining into a bounded sender owner/loop with backpressure and lifecycle tests |
| Sticky/adaptive partitioning | Java `BuiltInPartitioner` plus load stats from accumulator/sender | Rust has tests around adaptive sticky, but bench counters missing | Metric output proves distribution/load behavior under benchmark |
| Idempotence ordering/retry | Java TransactionManager/Sender preserve sequence rules and unresolved retry gaps | Rust has idempotence state tests, stress parity not proven | Mock broker tests for retry, leadership errors, out-of-order sequence, producer id/epoch recovery |
| 10 KiB max.request.size split | Java request/batch split observable via sender metrics | Partially fixed: Rust now sends ~2.832 records/request with zero retries/errors and has request-size split tests plus a mock-broker observable proof that captured ProduceRequest frame lengths stay under `max.request.size`; Java still shows ~3.000 records/request and higher throughput | Real 10 KiB benchmark comparison must still report zero errors/retries and same-machine Java 5-run evidence before any parity claim |
| Bench shape guard | Exactly two scenarios; no hidden env tuning | Partially guarded by tests; current Makefile exposes only bootstrap/topic/API | Test asserts scenario list and allowed env knobs; README matches code |
| Diff hygiene | Avoid degrading existing bench docs/scripts without approval | Current worktree has large README reduction and deleted matrix script | Review current diff before commit; either justify with tracker evidence or restore intentionally |

## Execution Plan

### P0: Stabilize The Tracking Baseline

- [x] Review current dirty diff and decide whether the README reduction and
  deleted `benches/scripts/producer_default_matrix.sh` should remain.
- [x] Keep or restore those changes explicitly; do not bury them inside producer
  parity work.
- [x] Verification: `git diff --name-status` plus user-visible summary.

### P1: Java Benchmark Parity Harness

- [x] Add a Java default benchmark wrapper that runs each scenario 5 times.
- [x] Print effective Java config snapshot at the start of every run, using the
  same key list as Rust.
- [x] Average records/sec and MB/sec with the same formula as Rust.
- [x] Verification:
  - `make bench-kafka-java-default` prints 5 runs per scenario.
  - Output includes effective config snapshot for each run.
  - No extra producer properties except required bootstrap/topic/client id.

### P2: Counter Parity Output

- [x] Define a shared output schema for Rust and Java:
  - `produce_requests`
  - `record_batches`
  - `records_per_batch_avg`
  - `records_per_request_avg`
  - `request_size_avg`
  - `retries`
  - `errors`
  - `in_flight_stalls`
  - `batch_splits`
  - `request_splits`
- [ ] Map remaining Java metrics from `Sender` and accumulator/buffer pool
  sensors; Java producer-perf still does not expose exact record-batch count or
  in-flight stall count.
- [x] Map Rust metrics from producer/dispatcher/accumulator counters; request
  size uses generated `ProduceRequestData` encoded length. ProduceRequest
  grouping splits forced by `max.request.size` are exposed as `request_splits`,
  not Java `batch_splits`.
- [ ] Implement Java-style `batch_splits` only when Rust can split and reenqueue
  a multi-record ProducerBatch after broker `MESSAGE_TOO_LARGE`.
- [ ] Verification:
  - Rust and Java benchmark outputs both include the schema.
  - Unit tests cover formatting and counter deltas.

### P3: 10 KiB Split Proof

- [x] Add a dispatcher unit test proving request construction opens a new
  ProduceRequest when the next partition would exceed `max.request.size`, using
  generated `ProduceRequestData::encoded_len`.
- [x] Add a mock or real-broker observable test for 10 KiB records under
  `max.request.size=1048576`.
- [x] Emit actual ProduceRequest count, record batch count, records/batch, and
  bytes/request for the 100k x 10 KiB scenario.
- [x] Verification:
  - Unit tests prove no silent oversize request.
  - Real run reports zero errors/retries or explains them.

### P4: Implementation Gap Closure

- [ ] Compare Java `RecordAccumulator.append` branch-by-branch against Rust
  `RecordAccumulator` and `Producer::send`.
- [ ] Compare Java `Sender.sendProducerData` readiness/backpressure/retry loop
  against Rust `ProducerDispatcher`.
- [x] Add a sender-loop primitive for predicting the next linger wake without
  scanning from the public callback send hot path.
- [x] Extract sender dispatch scheduling state into `producer/sender.rs` so the
  public `Producer` facade no longer owns in-flight partition selection state
  directly.
- [x] Move dispatch task `JoinSet` and `TimedDispatchOutcome` into
  `producer/sender.rs` so the sender state owns in-flight task slots.
- [x] Move joined-dispatch partition cleanup into `producer/sender.rs` so the
  sender state releases its own partition reservations on successful task
  completion.
- [x] Move dispatch spawn-time partition reservation into `producer/sender.rs`
  so `Producer::spawn_dispatch` no longer reserves in-flight partitions
  directly.
- [x] Move bounded in-flight slot waiting policy into `producer/sender.rs` so
  `Producer::poll` and `Producer::flush_inner` no longer compare the sender's
  in-flight task count against the configured limit directly.
- [x] Move non-blocking completed dispatch task collection into
  `producer/sender.rs` so `Producer::collect_finished` and
  `Producer::collect_finished_for_flush` no longer loop over `JoinSet`
  completion directly.
- [x] Move blocking wait-for-one dispatch completion behind
  `ProducerSenderState::wait_for_next_dispatch` so `Producer` no longer calls
  the sender state's raw `JoinSet::join_next` wrapper directly.
- [x] Move drained dispatch task body construction into `producer/sender.rs` so
  `Producer::spawn_dispatch` no longer builds the async
  `dispatch_drained`/latency/`TimedDispatchOutcome` task directly.
- [x] Move select-and-prepare for drained batches into `producer/sender.rs` so
  `Producer::poll` and `Producer::flush_inner` no longer directly combine
  `select_dispatchable_batches` with dispatcher `prepare_drained_batches`.
- [x] Add sender-side ready-batch detection before bounded slot waiting so
  `Producer::poll` does not drain or prepare ready batches while the dispatch
  slot is still full.
- [x] Move the `poll` ready-batch plus bounded dispatch-slot gate behind
  `ProducerSenderState::wait_for_ready_dispatch_slot`, returning any completed
  dispatch task to `Producer` for existing error/requeue handling.
- [x] Move the `poll` ready-batch drain and prepare path behind
  `ProducerSenderState::prepare_ready_dispatch_batches`, while preserving the
  old behavior that a dispatch completion is processed before any accumulator
  drain that could otherwise lose ready batches on error.
- [x] Move the `flush` drain-all plus bounded dispatch-slot wait and prepare
  path behind `ProducerSenderState::prepare_all_dispatch_batches`, while
  preserving the old behavior that a dispatch completion is processed before
  draining all buffered batches.
- [x] Move the append-path pending-work/backpressure wait predicate behind
  `ProducerSenderState::has_pending_work` so `Producer` no longer computes
  buffered-or-in-flight sender work directly.
- [x] Move the append backpressure buffer-wait decision behind
  `ProducerSenderState::buffer_wait_action` so `Producer::wait_for_buffer`
  no longer computes dispatch-completion, poll-ready, deadline, or capped sleep
  policy directly.
- [x] Move the flush/abort in-flight wait predicate behind
  `ProducerSenderState::has_in_flight_dispatches` so `Producer::flush_inner`
  and `Producer::abort_transaction` no longer inspect raw sender in-flight
  emptiness directly.
- [x] Move append poll budget policy behind
  `ProducerSenderState::append_poll_budget` so `Producer` no longer reads
  sender max-in-flight configuration to decide batch API polling cadence.
- [x] Move stateful callback append poll budget into `ProducerSenderState` so
  `Producer::send_with_callback` no longer owns mutable sender polling cadence.
- [x] Move the in-flight dispatch count used by producer metrics behind
  `ProducerSenderState::in_flight_dispatch_count` so `Producer::metrics` no
  longer reads raw sender task length directly.
- [x] Move blocking dispatch completion waits behind
  `ProducerSenderState::wait_for_dispatch_completion` so `Producer` no longer
  calls the sender's raw next-dispatch wait API in production paths.
- [x] Move joined dispatch completion normalization behind
  `ProducerSenderState::complete_dispatch_result` so production `Producer`
  paths no longer accept raw Tokio join results or release sender partition
  reservations directly.
- [x] Move blocking and non-blocking completed dispatch collection behind
  `ProducerSenderState::wait_for_completed_dispatch` and
  `ProducerSenderState::collect_completed_dispatches`, and make prepared
  dispatch states carry normalized producer errors instead of raw Tokio join
  errors.
- [x] Move pre-spawn batch observation for producer batch metrics behind
  `ProducerSenderState::spawn_observed_drained_dispatch` so dispatch task
  spawning owns the point where batches are observed before sender task
  ownership moves them.
- [x] Move prepared-selection start behind
  `ProducerSenderState::start_dispatch_selection` so `Producer::poll` and
  `Producer::flush_inner` no longer directly requeue deferred batches or spawn
  dispatchable batches from a `DispatchSelection`.
- [x] Move prepare-error recovery behind
  `ProducerSenderState::prepare_ready_dispatch_batches_or_requeue` and
  `ProducerSenderState::prepare_all_dispatch_batches_or_requeue` so
  `Producer::poll` and `Producer::flush_inner` no longer directly handle
  `DispatchPrepareError` or requeue failed prepare batches.
- [x] Move completed-dispatch result handling behind
  `ProducerSenderState::handle_completed_dispatch` so `Producer` no longer
  matches `DispatchOutcome::Delivered` / `DispatchOutcome::Requeue` directly
  and only supplies latency/requeue metric hooks.
- [x] Move non-blocking finished-dispatch collection plus result handling
  behind `ProducerSenderState::handle_finished_dispatches` so
  `Producer::collect_finished` and `Producer::collect_finished_for_flush` no
  longer loop over sender completion results directly.
- [x] Move blocking wait-for-one dispatch completion plus result handling
  behind `ProducerSenderState::wait_for_handled_dispatch` so
  `Producer::wait_for_one` and `Producer::wait_for_one_for_flush` no longer
  await raw sender completions and then call `dispatch_task_result` directly.
- [x] Move ready-path prepared dispatch progress application behind
  `ProducerSenderState::apply_ready_dispatch_progress` so `Producer::poll` no
  longer matches `PreparedReadyDispatch` variants directly.
- [x] Move flush-path prepared dispatch progress application behind
  `ProducerSenderState::apply_all_dispatch_progress` so
  `Producer::flush_inner` no longer matches `PreparedAllDispatch` variants
  directly.
- [x] Move ready-path prepare-plus-apply dispatch orchestration behind
  `ProducerSenderState::drive_ready_dispatch_progress` so `Producer::poll`
  no longer calls ready prepare and ready progress application as separate
  steps.
- [x] Move flush-path prepare-plus-apply dispatch orchestration behind
  `ProducerSenderState::drive_all_dispatch_progress` so `Producer::flush_inner`
  no longer calls all-batch prepare and all progress application as separate
  steps.
- [x] Move flush loop action selection behind
  `ProducerSenderState::drive_flush_dispatch_progress` so
  `Producer::flush_inner` no longer matches raw `AllDispatchProgress` or
  `DispatchStart` variants directly.
- [x] Move final flush completion wait decision behind
  `ProducerSenderState::flush_completion_progress` so `Producer::flush_inner`
  no longer reads raw sender in-flight state for its post-drain wait loop.
- [x] Move final flush completion wait loop behind
  `ProducerSenderState::wait_for_flush_completion` so `Producer::flush_inner`
  no longer loops over sender in-flight completion itself after draining all
  buffered batches.
- [x] Move abort in-flight completion wait loop behind
  `ProducerSenderState::wait_for_abort_completion` so
  `Producer::abort_transaction` no longer loops over sender in-flight state
  directly.
- [x] Move blocked flush-loop completion step behind
  `ProducerSenderState::drive_flush_dispatch_step` so `Producer::flush_inner`
  no longer calls `wait_for_one_for_flush` directly when dispatch is deferred by
  an in-flight partition.
- [x] Move append buffer-pressure decision selection behind
  `ProducerSenderState::append_backpressure_action` so producer append loops no
  longer inspect sender pending-work state directly before deciding whether to
  append, wait for buffer, or return local backpressure.
- [x] Move ready-dispatch poll loop behind
  `ProducerSenderState::drive_ready_dispatch_until_blocked` so
  `Producer::poll` no longer loops over `ReadyDispatchProgress` directly.
- [x] Move buffer-wait action execution behind
  `ProducerSenderState::wait_for_buffer_progress` so `Producer::wait_for_buffer`
  no longer matches `BufferWaitAction` directly.
- [x] Move append-capacity wait loop behind
  `ProducerSenderState::wait_for_append_capacity` so producer append loops no
  longer match `AppendBackpressureAction` directly.
- [x] Move the full flush loop behind
  `ProducerSenderState::drive_flush_until_complete` so
  `Producer::flush_inner` no longer loops over `FlushDispatchProgress`
  directly.
- [x] Move abort buffered-batch discard behind
  `ProducerSenderState::discard_buffered_batches` so
  `Producer::abort_transaction` no longer calls accumulator `drain_all`
  directly in production.
- [x] Move metrics queue/in-flight gauge collection behind
  `ProducerSenderState::queue_snapshot` so `Producer::metrics` no longer
  directly composes accumulator queue depth with sender in-flight count.
- [x] Move untracked append capacity wait plus accumulator append behind
  `ProducerSenderState::append_untracked_with_capacity_wait` so the
  `send_batch_untracked` hot path no longer appends directly into the
  accumulator from `Producer` after waiting for capacity.
- [x] Move per-record delivery append capacity wait plus accumulator append
  behind `ProducerSenderState::append_for_delivery_with_capacity_wait` so
  `send` / `send_with_callback` no longer append directly into the accumulator
  from `Producer` after waiting for capacity.
- [x] Move tracked batch delivery append capacity wait plus accumulator append
  behind `ProducerSenderState::append_for_batch_delivery_with_capacity_wait` so
  `send_batch` / `send_batch_with_callback` no longer append directly into the
  accumulator from `Producer` after waiting for capacity.
- [x] Move batch append poll-budget observation behind
  `ProducerSenderState::observe_batch_append_status` so public batch send loops
  no longer call `AppendPollBudget::observe` directly when deciding whether to
  drive ready dispatch after an append.
- [x] Move batch append status application behind
  `ProducerSenderState::apply_batch_append_status` so public batch send loops no
  longer compute batch append dispatch decisions directly after each append.
- [x] Move append post-dispatch interpretation behind
  `AppendDispatchDecision` plus sender-state decision helpers so public send
  loops no longer inspect `AppendStatus::batch_ready` directly when deciding
  whether to mark a sticky batch ready or drive ready dispatch.
- [x] Move append post-dispatch action application behind
  `ProducerSenderState::apply_append_dispatch_decision` so public send loops no
  longer directly mark sticky batches or call `poll` after append decisions.
- [x] Move single-record post-append completion collection behind
  `ProducerSenderState::apply_append_dispatch_decision_then_collect_finished`
  so `send` / `send_with_callback` no longer call `collect_finished` directly
  after append decisions in production.
- [x] Move batch final ready-dispatch drive behind
  `ProducerSenderState::finish_batch_append_dispatch` so
  `send_batch`, `send_batch_with_callback`, and `send_batch_untracked` no
  longer call `poll` directly at the end of append loops.
- [x] Move accumulator and sender state ownership under `ProducerSender` so
  `Producer` no longer stores `RecordAccumulator` and `ProducerSenderState` as
  independent top-level fields.
- [x] Move per-record delivery append capacity wait plus accumulator append
  through `ProducerSender` so `send` / `send_with_callback` no longer pass the
  wrapper's accumulator and state separately for that append path.
- [x] Move tracked batch delivery append capacity wait plus accumulator append
  through `ProducerSender` so `send_batch` / `send_batch_with_callback` no
  longer pass the wrapper's accumulator and state separately for that append
  path.
- [x] Move untracked append capacity wait plus accumulator append through
  `ProducerSender` so `send_batch_untracked` no longer passes the wrapper's
  accumulator and state separately for that append path.
- [x] Move single-record post-append dispatch decision application plus finished
  dispatch collection through `ProducerSender` so `send` / `send_with_callback`
  no longer pass the wrapper's accumulator and state separately for that
  post-append path.
- [x] Move batch append status application and final batch ready-dispatch drive
  through `ProducerSender` so `send_batch`, `send_batch_with_callback`, and
  `send_batch_untracked` no longer pass the wrapper's accumulator and state
  separately for those post-append paths.
- [x] Move ready-dispatch and flush-dispatch loops through `ProducerSender` so
  `poll` and `flush` no longer pass the wrapper's accumulator and state
  separately for those dispatch loops.
- [x] Move abort buffered-batch discard and abort completion wait through
  `ProducerSender` so `abort_transaction` no longer passes the wrapper's
  accumulator and state separately for abort lifecycle cleanup.
- [x] Move append poll-budget and append-decision helpers through
  `ProducerSender` so public send loops no longer read `sender.state` directly
  for those post-append scheduling decisions.
- [x] Move test-only completed-dispatch handling helpers through
  `ProducerSender` so producer tests no longer pass the wrapper's accumulator
  and state separately for completion handling.
- [x] Move read-only accumulator access for default partition assignment and
  adaptive partition load-stat refresh through `ProducerSender` so `Producer`
  no longer passes the wrapper's accumulator directly into `ProducerDispatcher`.
- [x] Move ready-dispatch wrapper dispatcher access into `ProducerSender` so
  `Producer::poll` no longer passes a dispatcher reference into the sender for
  that hot dispatch path.
- [x] Move final batch append dispatcher's access into `ProducerSender` so
  public batch send endings no longer pass a dispatcher reference into the
  sender for that hot dispatch path.
- [x] Move flush-dispatch wrapper dispatcher access into `ProducerSender` so
  `Producer::flush` no longer passes a dispatcher reference into the sender for
  that lifecycle dispatch path.
- [x] Move append capacity wait dispatcher access into `ProducerSender` so
  per-record delivery, tracked batch delivery, and untracked append wrappers no
  longer require `Producer` to pass a dispatcher reference into sender.
- [x] Move post-append dispatch application dispatcher access into
  `ProducerSender` so append decision and batch status application wrappers no
  longer require `Producer` to pass dispatcher-bearing application structs.
- [x] Move read-only accumulator/default partition helper dispatcher access into
  `ProducerSender` so default partition assignment and adaptive load-stat
  refresh wrappers no longer require `Producer` to pass a dispatcher reference.
- [x] Move sticky-partitioner policy lookup into `ProducerSender` so batch topic
  planning no longer calls `ProducerDispatcher` directly for that send-loop
  predicate.
- [x] Move batch default partition refresh-and-assign coordination into
  `ProducerSender` so tracked and untracked batch send paths no longer combine
  sender load-stat refresh with direct dispatcher assignment in `Producer`.
- [x] Move send-path metadata fetches behind `ProducerSender` so batch sends and
  custom partition assignment no longer call `ProducerDispatcher::metadata_for_topics`
  directly from `Producer`; public metadata APIs remain outside this send-path
  slice.
- [x] Move send-family transaction-error guard checks behind `ProducerSender` so
  `send`, `send_with_callback`, `send_batch`, `send_batch_with_callback`, and
  `send_batch_untracked` no longer call `ProducerDispatcher::fail_if_transaction_error`
  directly from `Producer`.
- [x] Move metadata-backed default partition assignment behind `ProducerSender`
  so custom partitioner fallback no longer calls
  `ProducerDispatcher::assign_partition_with_metadata` directly from
  `Producer`.
- [x] Move producer construction metrics handle setup behind `ProducerSender`
  so `Producer::from_parts` no longer calls `ProducerDispatcher::metrics_handle`
  directly.
- [x] Expose next linger readiness deadline through `ProducerSender` so a future
  sender loop can sleep until accumulator readiness without reaching into the
  accumulator directly.
- [x] Move append capacity wait scheduling into `ProducerSender` so untracked,
  per-record delivery, and tracked batch delivery append paths use the
  sender-owned linger readiness deadline instead of state-level accumulator
  access.
- [x] Add a sender-loop wake-decision primitive through
  `ProducerSender::next_wake_action` so a future background sender task can
  distinguish parked, linger-sleep, dispatch-ready, and in-flight-completion
  waits without public API calls driving every decision.
- [x] Add a sender-loop single-step primitive through
  `ProducerSender::drive_wake_step` so a future background sender task can
  execute immediate completion or ready-dispatch work while returning
  sleep/park states to the outer loop.
- [x] Add a sender-loop drive-until-waiting primitive through
  `ProducerSender::drive_wake_until_waiting` so a future background sender task
  can drain completed dispatches, start ready dispatch work, and stop at the
  next completion/sleep/park wait state without blocking on newly spawned
  dispatches.
- [x] Add sender-loop wake notification owned by `ProducerSender` so successful
  untracked, per-record delivery, and tracked batch delivery appends can wake a
  future background sender task without relying on later public API polling.
- [x] Add sender-loop delay waiting through
  `ProducerSender::wait_for_sender_loop_delay` so sleep branches can wake on
  append notification instead of only waiting for the timeout tick.
- [x] Add sender-loop wait-state handling through
  `ProducerSender::wait_for_sender_loop_wait` so the future outer sender task
  can await dispatch completion, linger sleep, or parked notification through
  one sender-owned primitive.
- [x] Add a sender-loop tick primitive through
  `ProducerSender::drive_sender_loop_once` so a future background sender task can
  drive available work to a wait state and then await dispatch completion,
  linger sleep, or append notification without relying on public API polling.
- [ ] Add failing tests first for every semantic gap.
- [ ] Verification:
  - Each gap has an oracle citation.
  - Each closed gap has red/green test output.
  - `make fmt`, `make clippy`, and `make test` pass.

## 2026-06-19 Same-Partition Request Packing Check

- [x] Added RED/GREEN coverage for dispatcher placement and send-batch request
  shape:
  - RED: `cargo test -p kacrab --features producer
    broker_request_index_reuses_same_partition_request_when_combined_records_fit
    --lib` failed with `index: 1` vs expected `index: 0`.
  - GREEN/safety: after real broker rejected same-partition multi-batch packing,
    the test was corrected to
    `broker_request_index_starts_fresh_request_for_same_partition_batch`, and
    `cargo test -p kacrab --features producer
    broker_request_index_starts_fresh_request_for_same_partition_batch --lib`
    passed.
  - Integration mock broker now decodes produce request RecordBatch base offsets
    for `kafka_producer_send_buffers_until_flush`,
    `kafka_producer_send_batch_returns_delivery_handles`, and
    `kafka_producer_send_batch_untracked_skips_delivery_handles`.
- [x] Real Kafka invalidated the attempted same topic-partition packing:
  `make bench-kafka` failed twice with broker `InvalidRecord` when multiple
  same-partition RecordBatches were concatenated into one `PartitionProduceData`
  records field. That optimization was not kept as a production claim.
- [x] Fresh verification after reverting to safe same-partition request
  separation:
  - `make fmt && make clippy && make test && git diff --check`: pass.
  - `make bench-kafka`: pass.
  - `make bench-kafka-java-default`: pass.
- [x] Fresh Rust benchmark, local Kafka `127.0.0.1:9092`, topic
  `kacrab-bench`, default per-record API:
  - 5,000,000 x 10B: 2,101,125 msg/s, 20.038 MB/s average over 5.
  - 100,000 x 10KiB: 34,446 msg/s, 336.383 MB/s average over 5.
  - 10KiB counters remained around `produce_requests ~= 35.2k-35.5k`,
    `record_batches=100000`, `records_per_request_avg ~= 2.819-2.842`,
    `retries=0`, `errors=0`, `in_flight_stalls=0`, `request_splits=0`,
    `requeues=0`.
- [x] Fresh Java benchmark, same local Kafka/topic/config wrapper:
  - 5,000,000 x 10B: 3,898,423 msg/s, 37.178 MB/s average over 5.
  - 100,000 x 10KiB: 38,806 msg/s, 378.966 MB/s average over 5.
  - Java 10KiB counters: `produce_requests=33333.333`,
    `records_per_request_avg=3`, `retries=0`, `errors=0`, `batch_splits=0`.
- [ ] Parity status: not parity. Rust remains below Java on both fresh default
  scenarios; same-partition multi-batch request packing is not a valid shortcut
  for closing the remaining 10KiB records/request gap.

## 2026-06-19 Callback Append Try-Lock Fast Path Check

- [x] Added sender fast-path coverage before wiring it into the public callback
  send path:
  - `producer_sender_fast_appends_callback_delivery_when_capacity_available`
    proves an uncontended sender can append callback delivery work without the
    async lock wait path when accumulator memory is available.
  - `producer_sender_fast_callback_append_returns_record_when_capacity_missing`
    proves the fast path returns the original `ProducerRecord` for the existing
    max.block/backpressure path instead of dropping delivery state.
- [x] Implemented a conservative callback append fast path:
  - `Producer::append_callback_for_delivery_with_max_block` now tries
    `sender.try_lock()` first.
  - `ProducerSender::try_append_callback_delivery_record` appends only when
    `RecordAccumulator::has_available_memory_for` is true.
  - Capacity misses fall back to the previous async capacity-wait path.
- [x] Fresh verification:
  - `make fmt`: pass.
  - `make clippy`: pass.
  - `make test`: pass.
  - `git diff --check`: pass.
- [x] Fresh Rust benchmark, local Kafka `127.0.0.1:9092`, topic
  `kacrab-bench`, default per-record API:
  - `5,000,000 messages x 10 bytes`: `1,418,216 messages/s`, `13.525 MB/s`
    average over 5.
  - `100,000 messages x 10 KiB`: `30,081 messages/s`, `293.759 MB/s`
    average over 5.
  - 10KiB counters remained around `produce_requests ~= 35.3k`,
    `record_batches=100000`, `records_per_request_avg ~= 2.831-2.836`,
    `retries=0`, `errors=0`, `in_flight_stalls=0`, `request_splits=0`,
    `requeues=0`, `batch_fill=0.629`.
- [x] Fresh Java benchmark, same local Kafka/topic/config wrapper:
  - `5,000,000 messages x 10 bytes`: `3,551,430 messages/s`, `33.868 MB/s`
    average over 5.
  - `100,000 messages x 10 KiB`: `35,339 messages/s`, `345.112 MB/s`
    average over 5.
  - Java 10KiB counters stayed at `produce_requests=33333.333`,
    `records_per_request_avg=3`, `retries=0`, `errors=0`, `batch_splits=0`.
- [ ] Parity status: not parity. The try-lock append path is not enough to
  close the sender-ownership gap. It improved the latest 10B snapshot versus
  the prior post-notify run, but 10KiB regressed versus the previous Rust
  snapshot in this same tracker. Do not report Java parity or overall producer
  performance completion from this change.

## 2026-06-19 Sender Completion Wakeup Check

- [x] Added RED/GREEN coverage for sender-loop completion wakeup:
  - RED: `cargo test -p kacrab --features producer
    producer_sender_notifies_sender_loop_when_dispatch_completes --lib` failed
    by timing out because completed dispatch tasks did not wake the sender-loop
    `Notify`.
  - GREEN: `ProducerSenderState` now can notify the owning `ProducerSender`
    loop when a spawned dispatch future completes; `ProducerSender::with_dispatcher`
    wires the completion notifier to the existing sender-loop notifier.
  - The background sender loop now waits on that notifier for both parked and
    dispatch-completion states instead of sleeping/polling one millisecond.
- [x] Stabilized manual-poll tests that intentionally bypass the background
  loop:
  - `poll_waits_for_one_in_flight_slot_before_spawning_ready_batch`
  - `poll_waits_until_blocked_in_flight_task_completes`
  both abort the background sender loop before directly injecting in-flight
  tasks.
- [x] Tried and rejected a stronger "background sender owns callback dispatch"
  experiment:
  - Downgrading callback append `DriveReady` to `MarkBatchReady` while a
    background sender loop exists raised the 10KiB request packing to Java-like
    `records_per_request_avg ~= 2.997`.
  - It regressed 10KiB throughput badly because callback latency rose into the
    hundreds of milliseconds even after completion notify:
    `100,000 messages x 10 KiB`: `26,186 messages/s`, `255.727 MB/s`, with
    avg latency around `220-234 ms`.
  - That downgrade was reverted and is not part of the current code.
- [x] Tried and rejected a narrower callback wake-suppression experiment:
  - RED/GREEN test `producer_sender_callback_mark_ready_append_does_not_wake_sender_loop`
    proved callback `MarkBatchReady` appends could avoid waking the background
    sender loop while keeping public callback dispatch budget behavior.
  - Fresh Rust-only benchmark regressed badly, so the test and code change were
    reverted before running Java parity comparison:
    - `5,000,000 messages x 10 bytes`: `1,415,077 messages/s`, `13.495 MB/s`
      average over 5.
    - `100,000 messages x 10 KiB`: `12,226 messages/s`, `119.395 MB/s`
      average over 5.
  - Counters stayed similar (`records_per_request_avg ~= 2.83` for 10KiB), so
    the loss was not from request packing; it is retained as negative evidence
    that suppressing callback ready notifications alone worsens progress.
- [x] Fresh verification for the current code:
  - `make fmt`: pass.
  - `make clippy`: pass.
  - `make test`: pass.
- [x] Fresh Rust benchmark for the current code, local Kafka
  `127.0.0.1:9092`, topic `kacrab-bench`, default per-record API:
  - `5,000,000 messages x 10 bytes`: `1,397,853 messages/s`, `13.331 MB/s`
    average over 5.
  - `100,000 messages x 10 KiB`: `30,856 messages/s`, `301.323 MB/s`
    average over 5.
  - 10KiB counters stayed around `produce_requests ~= 35.2k-35.4k`,
    `record_batches=100000`, `records_per_request_avg ~= 2.826-2.841`,
    `retries=0`, `errors=0`, `in_flight_stalls=0`, `request_splits=0`,
    `requeues=0`, `batch_fill=0.629`.
- [x] Fresh Java benchmark, same local Kafka/topic/config wrapper:
  - `5,000,000 messages x 10 bytes`: `3,631,968 messages/s`, `34.638 MB/s`
    average over 5.
  - `100,000 messages x 10 KiB`: `31,875 messages/s`, `311.278 MB/s`
    average over 5.
  - Java 10KiB counters stayed at `produce_requests=33333.333`,
    `records_per_request_avg=3`, `retries=0`, `errors=0`, `batch_splits=0`.
- [ ] Parity status: not parity. The current code keeps the completion wakeup
  primitive because it is needed for a real sender loop, but Rust remains below
  Java on both fresh scenarios. The 10KiB gap is narrow in this run, but 10B is
  still far behind and the Java-like request-packing experiment is not
  throughput-safe yet.
- [x] Tried and rejected a metrics-hot-path bulk counter experiment:
  - Changed ready-batch observers to aggregate `record_produce_batch` counters
    for the observed slice and added a snapshot-equivalence unit test.
  - Verification passed during the experiment: `make fmt`, `make clippy`, and
    `make test`.
  - Fresh Rust-only benchmark did not prove improvement and produced worse
    10KiB average under the same default per-record harness:
    - `5,000,000 messages x 10 bytes`: `1,404,605 messages/s`, `13.395 MB/s`
      average over 5.
    - `100,000 messages x 10 KiB`: `21,485 messages/s`, `209.813 MB/s`
      average over 5.
  - 10KiB packing counters stayed around `records_per_request_avg ~= 2.83`,
    so the experiment did not address the remaining architecture gap. The
    metrics bulk change and its test were reverted.
- [x] Fresh verification after reverting the metrics bulk experiment:
  - `make fmt`: pass.
  - `make clippy`: pass.
  - `make test`: pass.
- [x] Tried and rejected a callback idle-defer experiment:
  - RED/GREEN test proved a possible Java-like behavior: when a background
    sender loop exists, a callback append whose dispatch decision is `Idle`
    can defer completed-dispatch collection to the sender loop instead of
    re-entering sender state on the API thread.
  - A guard test kept the no-background-sender (`linger.ms=0`) path collecting
    completed dispatches on the API thread.
  - Fresh verification passed during the experiment: targeted callback tests,
    `make fmt`, `make clippy`, and `make test`.
  - Fresh Rust-only benchmark improved 10-byte throughput but regressed 10KiB
    badly, so the code and tests were reverted:
    - Before topic reset: `5,000,000 messages x 10 bytes`:
      `1,719,177 messages/s`, `16.395 MB/s`; `100,000 messages x 10 KiB`:
      `16,971 messages/s`, `165.731 MB/s`.
    - After `make kafka-topic-recreate`: `5,000,000 messages x 10 bytes`:
      `1,717,500 messages/s`, `16.379 MB/s`; `100,000 messages x 10 KiB`:
      `14,605 messages/s`, `142.629 MB/s`.
  - The 10KiB counters stayed around `records_per_request_avg ~= 2.83`,
    `retries=0`, `errors=0`, `in_flight_stalls=0`, `request_splits=0`,
    so the regression was not a request-packing improvement tradeoff worth
    keeping.
- [x] Fresh verification after reverting the callback idle-defer experiment:
  - `make fmt`: pass.
  - `make clippy`: pass.
  - `make test`: pass.
- [x] Tried and rejected a completion-aware callback idle-skip experiment:
  - Added an advisory pending-completion counter on sender dispatch tasks so
    callback `Idle` appends could skip API-thread collection only when no
    dispatch completion was known to be pending.
  - RED/GREEN tests covered both the pending-completion collection path and
    normalized dispatch completion accounting. Full verification passed during
    the experiment.
  - Fresh Rust-only benchmark after `make kafka-topic-recreate` regressed the
    10KiB case versus the accepted current-code baseline, so the code and tests
    were reverted:
    - `5,000,000 messages x 10 bytes`: `1,651,144 messages/s`, `15.747 MB/s`
      average over 5.
    - `100,000 messages x 10 KiB`: `24,176 messages/s`, `236.095 MB/s`
      average over 5.
  - 10KiB counters stayed around `records_per_request_avg ~= 2.83`,
    `retries=0`, `errors=0`, `in_flight_stalls=0`, so the change did not
    solve the architecture gap and was not kept.
- [x] Fresh verification after reverting the completion-aware idle-skip
  experiment:
  - `make fmt`: pass.
  - `make clippy`: pass.
  - `make test`: pass.

## 2026-06-19 No-Interceptor Error Snapshot Clone Reduction

- [x] Added RED/GREEN coverage for the default callback send hot path:
  - RED: `cargo test -p kacrab --features producer
    send_with_callback_without_interceptors_does_not_clone_successful_record
    --lib -- --nocapture` failed with clone count `2` versus expected `0`.
  - GREEN: `Producer::send` and `Producer::send_with_callback` now only clone
    an error-record snapshot when native interceptors are installed. The
    default benchmark path has no interceptors, so successful callback sends no
    longer clone `ProducerRecord` solely for interceptor error reporting.
  - Existing error-ordering tests still prove local callback errors run before
    interceptor error hooks and that interceptor error metadata remains intact.
- [x] Fresh verification:
  - Targeted callback no-clone test: pass.
  - Targeted callback/interceptor error tests: pass.
  - `make fmt`: pass.
  - `make clippy`: pass.
  - `make test`: pass.
- [x] Fresh Rust benchmark, local Kafka `127.0.0.1:9092`, topic
  `kacrab-bench`, default per-record API:
  - `5,000,000 messages x 10 bytes`: `1,469,495 messages/s`, `14.014 MB/s`
    average over 5.
  - `100,000 messages x 10 KiB`: `35,256 messages/s`, `344.296 MB/s`
    average over 5.
  - 10KiB counters stayed around `produce_requests ~= 35.1k-35.4k`,
    `record_batches=100000`, `records_per_request_avg ~= 2.828-2.848`,
    `retries=0`, `errors=0`, `in_flight_stalls=0`, `request_splits=0`,
    `requeues=0`, `batch_fill=0.629`.
- [x] Fresh Java benchmark, same local Kafka/topic/default wrapper:
  - `5,000,000 messages x 10 bytes`: `3,792,139 messages/s`, `36.166 MB/s`
    average over 5.
  - `100,000 messages x 10 KiB`: `39,473 messages/s`, `385.482 MB/s`
    average over 5.
  - Java 10KiB counters stayed at `produce_requests=33333.333`,
    `records_per_request_avg=3`, `retries=0`, `errors=0`, `batch_splits=0`.
- [ ] Parity status: not parity. The clone reduction improves the fresh Rust
  snapshot versus the prior accepted current-code run, but Rust remains below
  Java on both same-machine default scenarios. The remaining gap is still
  sender/accumulator architecture, especially 10B API-thread overhead and
  10KiB records/request/request-size efficiency.
- [x] Tried and rejected a zero-copy sticky-topic snapshot experiment:
  - RED/GREEN test proved `Producer` could snapshot the sticky topic with an
    `Arc<str>` clone instead of allocating `record.topic.to_string()` on
    `send`/`send_with_callback`.
  - Fresh verification passed during the experiment: targeted sticky snapshot
    test, targeted callback no-clone test, `make fmt`, `make clippy`, and
    `make test`.
  - Fresh Rust-only benchmark was mixed and widened the 10KiB gap versus the
    accepted current-code snapshot, so the sticky snapshot code/test was
    reverted:
    - `5,000,000 messages x 10 bytes`: `1,508,150 messages/s`, `14.383 MB/s`
      average over 5.
    - `100,000 messages x 10 KiB`: `33,199 messages/s`, `324.206 MB/s`
      average over 5.
  - 10KiB counters stayed around `produce_requests ~= 35.2k-35.4k`,
    `records_per_request_avg ~= 2.825-2.837`, `retries=0`, `errors=0`,
    `in_flight_stalls=0`, so the experiment did not improve the batching or
    request-efficiency gap.
- [x] Fresh verification after reverting the sticky-topic snapshot experiment:
  - `make fmt`: pass.
  - `make clippy`: pass.
  - `make test`: pass.
- [x] Tried and rejected a delivery metadata lock-avoidance experiment:
  - RED/GREEN test proved `DeliverySender::delivery_for_record` could append
    per-record metadata through the exclusive `Arc::get_mut` path after the
    previous delivery handle was dropped, matching the existing
    `record_for_batch_callback` optimization.
  - Fresh verification passed during the experiment: targeted delivery metadata
    tests, `make fmt`, `make clippy`, and `make test`.
  - Fresh Rust-only benchmark was mixed and regressed the 10KiB scenario versus
    the accepted current-code snapshot, so the metadata-lock code/test was
    reverted:
    - `5,000,000 messages x 10 bytes`: `1,491,392 messages/s`, `14.223 MB/s`
      average over 5.
    - `100,000 messages x 10 KiB`: `34,412 messages/s`, `336.055 MB/s`
      average over 5.
  - 10KiB counters stayed around `produce_requests ~= 35.3k-35.5k`,
    `records_per_request_avg ~= 2.818-2.833`, `retries=0`, `errors=0`,
    `in_flight_stalls=0`, `request_splits=0`, `requeues=0`, so the experiment
    did not improve the batching or request-efficiency gap.
- [x] Fresh verification after reverting the delivery metadata lock-avoidance
  experiment:
  - `make fmt`: pass.
  - `make clippy`: pass.
  - `make test`: pass.
- [x] Tried and rejected a sender-loop capacity wake experiment:
  - RED/GREEN test changed `ProducerSender::next_wake_action` so a ready batch
    could dispatch while another request was in flight if `in_flight <
    max.in.flight.requests.per.connection`, instead of waiting for any
    in-flight dispatch to complete.
  - Fresh verification passed during the experiment: targeted sender wake test,
    sender unit test filter, background sender integration test, `make fmt`,
    `make clippy`, and `make test`.
  - Fresh Rust-only benchmark regressed the 10KiB scenario versus the accepted
    current-code snapshot, so the scheduler code/test was reverted:
    - `5,000,000 messages x 10 bytes`: `1,470,985 messages/s`, `14.028 MB/s`
      average over 5.
    - `100,000 messages x 10 KiB`: `33,016 messages/s`, `322.422 MB/s`
      average over 5.
  - 10KiB counters stayed around `produce_requests ~= 35.3k-35.5k`,
    `records_per_request_avg ~= 2.819-2.830`, `retries=0`, `errors=0`,
    `in_flight_stalls=0`, `request_splits=0`, `requeues=0`, so simply letting
    the sender loop dispatch with a partially used in-flight window did not
    improve the request-efficiency gap.
- [x] Tried and rejected a cached-sticky fit-limit experiment:
  - RED/GREEN test changed `ProducerPartitionerState::try_assign_cached_sticky_partition`
    to stop reusing the cached sticky partition when `sticky.bytes +
    next_record_bytes > batch.size`, matching the accumulator's per-batch fit
    rule more closely than the previous `2 * batch.size` guard.
  - Fresh verification passed during the experiment: targeted cached-sticky
    tests, `kafka_producer_` integration filter, `make fmt`, `make clippy`,
    and `make test`.
  - Fresh Rust-only benchmark regressed both default scenarios versus the
    accepted current-code snapshot, so the cached-sticky code/test was
    reverted:
    - `5,000,000 messages x 10 bytes`: `1,169,123 messages/s`, `11.150 MB/s`
      average over 5.
    - `100,000 messages x 10 KiB`: `32,304 messages/s`, `315.468 MB/s`
      average over 5.
  - 10KiB counters stayed around `produce_requests ~= 35.2k-35.3k`,
    `records_per_request_avg ~= 2.830-2.836`, `retries=0`, `errors=0`,
    `in_flight_stalls=0`, `request_splits=0`, `requeues=0`, so switching the
    cached sticky partition earlier did not improve request packing and hurt
    the API-thread path.
- [x] Tried and rejected a fused callback assign+append experiment:
  - RED/GREEN test added a sender primitive that assigned a sticky partition and
    fast-appended the callback delivery record while holding one sender lock,
    then the client used that primitive only for the default no-interceptor,
    no-native-partitioner callback path.
  - Fresh verification passed during the experiment: targeted sender/client/mock
    broker tests, `make fmt`, `make clippy`, and `make test`.
  - Fresh Rust-only benchmark regressed both default scenarios versus the
    accepted current-code snapshot, so the fused callback assign+append
    code/test was reverted:
    - `5,000,000 messages x 10 bytes`: `1,782,698 messages/s`, `17.001 MB/s`
      average over 5.
    - `100,000 messages x 10 KiB`: `31,565 messages/s`, `308.250 MB/s`
      average over 5.
  - 10KiB counters stayed around `produce_requests ~= 35.2k-35.4k`,
    `records_per_request_avg ~= 2.828-2.838`, `retries=0`, `errors=0`,
    `in_flight_stalls=0`, `request_splits=0`, `requeues=0`, so removing one
    API-thread lock boundary did not address the remaining throughput gap.
- [x] Tried and rejected a single-record inline delivery metadata store:
  - RED/GREEN test replaced the per-delivery `Vec<RecordDeliveryMetadata>` with
    an inline-first store so the common per-record callback path did not reserve
    metadata heap capacity for one record.
  - Fresh verification passed during the experiment: targeted record metadata
    tests, `make fmt`, `make clippy`, and `make test`.
  - Fresh Rust-only benchmark regressed both default scenarios versus the
    accepted current-code snapshot, so the inline metadata store code/test was
    reverted:
    - `5,000,000 messages x 10 bytes`: `1,426,581 messages/s`, `13.605 MB/s`
      average over 5.
    - `100,000 messages x 10 KiB`: `30,755 messages/s`, `300.346 MB/s`
      average over 5.
  - 10KiB counters stayed around `produce_requests ~= 35.3k`,
    `records_per_request_avg ~= 2.829-2.833`, `retries=0`, `errors=0`,
    `in_flight_stalls=0`, `request_splits=0`, `requeues=0`, so this allocation
    reduction did not improve request packing and hurt the API-thread path.
- [x] Added Rust five-run average counter output to the real Kafka benchmark:
  - RED/GREEN tests added `rust average counters` formatting with the same
    compact counter schema as per-run output.
  - A follow-up regression test caught and fixed `u64` to `f64` saturation for
    multi-run 10KiB request-byte totals above `u32::MAX`.
  - Fresh verification passed: `cargo test -p kacrab-benches --bin
    producer_kafka_bench -- --nocapture`, `make fmt`, `make clippy`, and
    `make test`.
  - Fresh Rust-only `make bench-kafka` confirmed the new average counter lines:
    - `5,000,000 messages x 10 bytes`: `1,465,228 messages/s`, `13.974 MB/s`
      average over 5; average counters include `produce_requests=5506.200`,
      `records_per_request_avg=908.067`, `request_size_avg=16376.644`,
      `retries=0`, `errors=0`, `in_flight_stalls=0`, `request_splits=0`,
      `requeues=0`.
    - `100,000 messages x 10 KiB`: `28,705 messages/s`, `280.317 MB/s`
      average over 5; average counters include `produce_requests=35348.600`,
      `records_per_request_avg=2.829`, `request_size_avg=29219.104`,
      `record_batch_payload_bytes_per_request_avg=29172.301`, `retries=0`,
      `errors=0`, `in_flight_stalls=0`, `request_splits=0`, `requeues=0`.
  - This is benchmark evidence improvement only. It does not prove Rust/Java
    parity and does not change producer throughput architecture.
- [x] Added Java five-run average counter output to the real Kafka benchmark
  wrapper:
  - RED/GREEN Python tests added a reusable parser for Java
    `kafka-producer-perf-test.sh --print-metrics` output and covered both
    per-run `java producer counters` formatting and five-run
    `java average counters` aggregation.
  - Java average `records_per_request_avg` is weighted from total sent records
    over derived total Produce requests. `request_size_avg` is weighted by
    derived Produce requests so it matches the request-level counter meaning.
  - The wrapper now keeps each run log and prints `java average counters` after
    the existing five-run throughput average for each scenario.
  - Fresh verification passed: `python3 -m unittest
    benches/scripts/test_producer_counter_metrics.py`,
    `bash -n benches/scripts/producer_default_matrix.sh`, `make fmt`,
    `make clippy`, `make test`, and `git diff --check`.
  - Fresh `make bench-kafka-java-default` confirmed the new Java average
    counter lines:
    - `5,000,000 messages x 10 bytes`: `3,564,910 messages/s`,
      `33.998 MB/s` average over 5; average counters include
      `produce_requests=5206.400`, `records_per_request_avg=960.357`,
      `request_size_avg=17351.524`, `retries=0.000`, `errors=0.000`,
      `batch_splits=0.000`.
    - `100,000 messages x 10 KiB`: `30,230 messages/s`, `295.218 MB/s`
      average over 5; average counters include
      `produce_requests=33333.333`, `records_per_request_avg=3.000`,
      `request_size_avg=31017.666`, `retries=0.000`, `errors=0.000`,
      `batch_splits=0.000`.
  - This is benchmark evidence improvement only. It does not prove Rust/Java
    parity and does not change producer throughput architecture.
- [x] Moved one step closer to a Java-style background Sender loop:
  - RED/GREEN integration test added
    `kafka_producer_background_sender_dispatches_ready_batch_without_linger_or_flush`,
    proving a ready batch with `linger=0` can be delivered by the background
    sender without an explicit public `flush`/`poll`.
  - `Producer::from_parts` now starts the background sender loop whenever a
    Tokio runtime handle is available, instead of only when `linger > 0`.
  - The sender loop now pauses background dispatch after a metadata-missing
    requeue so it does not immediately spin/retry requeued batches behind a
    failing `flush`; append-ready work clears the pause and notifies the loop.
  - The metadata-missing requeue integration test now waits for a buffered
    snapshot because `Producer::buffered_bytes()` is intentionally
    opportunistic (`try_lock`) while the background sender can hold the sender
    mutex.
  - Fresh verification passed after the sender-loop change:
    `cargo test -p kacrab --all-features
    kafka_producer_background_sender_dispatches_ready_batch_without_linger_or_flush
    --test producer_dispatcher -- --nocapture`,
    `cargo test -p kacrab --all-features
    kafka_producer_requeues_in_flight_batch_when_metadata_is_missing --test
    producer_dispatcher -- --nocapture`, `make fmt`, `make clippy`,
    `make test`, and `git diff --check`.
  - Fresh Rust-only `make bench-kafka` ran before deleting `kacrab-bench` topic
    data to recover disk:
    - `5,000,000 messages x 10 bytes`: `1,459,390 messages/s`,
      `13.918 MB/s` average over 5; average counters include
      `produce_requests=5502.400`, `records_per_batch_avg=904.192`,
      `records_per_request_avg=908.694`, `request_size_avg=16387.902`,
      `retries=0.000`, `errors=0.000`, `in_flight_stalls=0.000`,
      `request_splits=0.000`, `requeues=0.000`.
    - `100,000 messages x 10 KiB`: `34,144 messages/s`, `333.433 MB/s`
      average over 5; average counters include
      `produce_requests=35315.000`, `records_per_request_avg=2.832`,
      `request_size_avg=29246.878`, `retries=0.000`, `errors=0.000`,
      `in_flight_stalls=0.000`, `request_splits=0.000`, `requeues=0.000`.
  - This is architecture progress and Rust-only evidence. It does not prove
    Rust/Java parity; the benchmark topic was deleted afterwards to free disk
    space and must be recreated before the next real Kafka comparison run.
- [x] Hardened real-Kafka benchmark topic lifecycle after local disk pressure:
  - `kacrab-bench` topic data grew the native Kafka data directory to roughly
    57 GiB during repeated parity runs. The topic was deleted and Kafka's
    `kacrab-bench-*-delete` dirs were pruned after stopping Kafka, reducing the
    local Kafka data directory to tens of MiB and filesystem use to about 81%.
  - `bench-kafka` now depends on `kafka-topic-create`, and
    `bench-kafka-java-default` depends on `kafka-topic-create`, so deleting the
    benchmark topic for disk recovery does not leave the next parity run with a
    missing topic.
  - Added `bench-kafka-topic` as an alias for `kafka-topic-create`, keeping the
    benchmark topic lifecycle discoverable from the benchmark target namespace.
  - The README now documents `kafka-topic-recreate` before comparison runs and
    `kafka-topic-delete` plus `kafka-stop`/`kafka-topic-prune-delete-dirs` after
    large runs. This is harness hygiene only and does not prove Rust/Java
    producer parity.
- [x] Tried and rejected fully deferring callback ready dispatch to the
  background sender loop:
  - RED/GREEN unit test temporarily added
    `send_with_callback_defers_ready_dispatch_to_background_sender_loop`,
    proving current `send_with_callback` still spawned an in-flight dispatch on
    the caller path once the callback ready-batch poll budget was reached
    (`in_flight_len` was `1` instead of `0`).
  - The experiment downgraded callback `DriveReady` decisions to
    `MarkBatchReady` whenever a background sender loop existed, leaving append
    notification for the sender loop but removing caller-side ready dispatch
    cadence.
  - Fresh Rust-only `make bench-kafka` after the experiment showed this was not
    an acceptable producer hot-path change:
    - `5,000,000 messages x 10 bytes`: `1,508,773 messages/s`,
      `14.389 MB/s` average over 5; average counters included
      `produce_requests=5481.000`, `records_per_request_avg=912.242`,
      `request_size_avg=16451.997`, `retries=0.000`, `errors=0.000`,
      `in_flight_stalls=0.000`, `request_splits=0.000`, `requeues=0.000`.
    - `100,000 messages x 10 KiB`: `25,410 messages/s`, `248.148 MB/s`
      average over 5; average counters included
      `produce_requests=33365.000`, `records_per_request_avg=2.997`,
      `request_size_avg=30954.619`, `retries=0.000`, `errors=0.000`,
      `in_flight_stalls=0.000`, `request_splits=0.000`, `requeues=0.000`.
  - Although request packing reached Java-like `records/request ~= 3.0`, the
    10KiB throughput regressed sharply from the accepted sender-loop snapshot
    (`34,144 messages/s`, `333.433 MB/s`), so the code and test were reverted.
  - Fresh verification after revert passed: `make fmt`, `make clippy`, and
    `make test`.
  - The benchmark topic was deleted after the run and Kafka `*-delete` dirs were
    pruned back to a 48 MiB local Kafka data directory. This rejected experiment
    does not prove Rust/Java producer parity.
- [x] Refreshed `BENCHMARK_PARITY_AUDIT.md` to match the current benchmark
  harness:
  - Removed stale claims that Rust default benchmark used
    `send_batch_with_callback`, chunk-level latency timestamps, or a batched API
    advantage versus Java producer-perf.
  - The audit now records the current default shape: `KACRAB_BENCH_API=per-record`,
    public `send_with_callback` per record, per-record start timestamps, Java
    five-run averaging, and compact Rust/Java counters.
  - The remaining audit conclusion is still not parity: Rust producer internals
    differ from Java's mature background `Sender`/`NetworkClient` and
    accumulator path, so any faster/slower claim still needs fresh same-machine
    Rust and Java evidence.
- [x] Moved callback append dispatch application further into sender ownership
  without changing callback registration ordering:
  - RED test
    `producer::sender::tests::producer_sender_appends_callback_delivery_and_applies_dispatch_decision`
    first failed with missing
    `append_callback_delivery_record_then_apply_dispatch`, then passed after
    `ProducerSender` gained a helper that appends callback delivery records,
    runs a pre-dispatch registration hook, and applies the callback append
    dispatch decision in one sender-owned path.
  - `send_with_callback` now registers interceptor/user callbacks through that
    pre-dispatch hook before ready dispatch can complete the delivery, preserving
    the existing callback ordering invariant while avoiding the previous public
    client append-then-relock-to-apply orchestration.
  - `CallbackAppendFastPath` is now test-only because production callback send no
    longer splits fast append and dispatch application across two client-level
    sender lock sections.
  - Verification:
    `cargo test -p kacrab --all-features producer::sender::tests::producer_sender_appends_callback_delivery_and_applies_dispatch_decision --lib`
    passed; `cargo test -p kacrab --all-features producer::client::tests:: --lib`
    passed; `cargo test -p kacrab --all-features producer::sender::tests:: --lib`
    passed; `make fmt`, `make clippy`, and `make test` passed.
  - This is architecture/hot-path cleanup only. Kafka remained stopped after the
    disk cleanup, so no fresh Rust/Java throughput comparison was run and this is
    not parity evidence.
- [x] Moved normal `send()` append dispatch application into sender ownership:
  - RED test
    `producer::sender::tests::producer_sender_appends_delivery_and_applies_dispatch_decision`
    first failed with missing `AppendDeliveryRecord` and
    `append_delivery_record_then_apply_dispatch`, then passed after
    `ProducerSender` gained the normal-delivery equivalent of the callback
    helper.
  - `send()` now registers interceptor acks through a pre-dispatch hook and lets
    `ProducerSender` append, derive the single-record append dispatch decision,
    and apply that decision in the sender-owned path. This removes the previous
    client-level append-return-status-then-lock-again-to-apply orchestration for
    normal sends while keeping ack registration before dispatch completion.
  - Removed now-dead client helpers for applying append dispatch decisions and
    registering interceptor acks outside the sender-owned append path.
  - Verification:
    `cargo test -p kacrab --all-features producer::sender::tests::producer_sender_appends_delivery_and_applies_dispatch_decision --lib`
    passed; `cargo test -p kacrab --all-features producer::client::tests:: --lib`
    passed; `cargo test -p kacrab --all-features producer::sender::tests:: --lib`
    passed; `make fmt`, `make clippy`, and `make test` passed.
  - This is architecture/hot-path cleanup only. Kafka stayed stopped to preserve
    the post-cleanup disk state, so no Rust/Java benchmark was run and this is
    not parity evidence.
- [x] Moved tracked batch delivery append/status application into sender
  ownership:
  - RED test
    `producer::sender::tests::producer_sender_appends_batch_delivery_and_applies_batch_status`
    first failed with missing
    `append_batch_delivery_record_then_apply_batch_status`, then passed after
    `ProducerSender` gained a helper that appends a batch delivery record, runs a
    pre-dispatch hook for optional callback registration, and applies the batch
    append poll budget/status in the sender-owned path.
  - `send_batch` and `send_batch_with_callback` now call the sender-owned batch
    append/status helper for tracked batch records. This removes the tracked
    batch client pattern of append-return-status then client-side
    `apply_batch_append_status`, and keeps batch callback registration before a
    dispatch can complete the delivery.
  - Introduced `AppendBatchDeliveryApply` to keep the sender API compact and
    clippy-clean while carrying the mutable append poll budget, append request,
    and sticky-topic marker together.
  - Verification:
    `cargo test -p kacrab --all-features producer::sender::tests::producer_sender_appends_batch_delivery_and_applies_batch_status --lib`
    passed; `cargo test -p kacrab --all-features producer::client::tests:: --lib`
    passed; `cargo test -p kacrab --all-features --test producer_dispatcher kafka_producer_send_batch`
    passed; `cargo test -p kacrab --all-features producer::sender::tests:: --lib`
    passed; `make fmt`, `make clippy`, and `make test` passed.
  - This is architecture/hot-path cleanup only. Kafka stayed stopped, so no
    Rust/Java benchmark was run and this is not parity evidence.
- [x] Moved untracked batch append/status application into sender ownership:
  - RED test
    `producer::sender::tests::producer_sender_appends_untracked_batch_record_and_applies_batch_status`
    first failed with missing `AppendUntrackedBatchApply`,
    `AppendUntrackedRecord`, and
    `append_untracked_record_then_apply_batch_status`, then passed after the
    sender-owned helper and request structs were added.
  - `send_batch_untracked` now passes `AppendUntrackedBatchApply` into
    `ProducerSender`, so the sender owns capacity wait, accumulator append, and
    batch append poll-budget/status application for untracked batch records.
    Client-level record size validation, metrics observers, and sticky-topic
    markers are preserved.
  - Removed the untracked batch client pattern of
    append-return-status followed by client-side `apply_batch_append_status`.
  - Verification:
    `cargo test -p kacrab --all-features producer::sender::tests::producer_sender_appends_untracked_batch_record_and_applies_batch_status --lib`
    passed; `cargo test -p kacrab --all-features producer::sender::tests:: --lib`
    passed; `cargo test -p kacrab --all-features producer::client::tests:: --lib`
    passed; `cargo test -p kacrab --all-features --test producer_dispatcher send_batch_untracked`
    passed; `make fmt`, `make clippy`, `make test`, and `git diff --check`
    passed.
  - This is architecture/hot-path cleanup only. Kafka stayed stopped to preserve
    the post-cleanup disk state, so no Rust/Java benchmark was run and this is
    not parity evidence.
- [x] Revalidated and fixed send-family transaction-error guard ownership:
  - The tracker already recorded this as sender-owned, but current source still
    had direct `Producer::dispatcher.fail_if_transaction_error()` calls in
    `send`, `send_with_callback`, `send_batch`, `send_batch_with_callback`, and
    `send_batch_untracked`.
  - RED structural test
    `producer::client::tests::send_family_transaction_error_guards_route_through_sender`
    first failed on `send` because it still called `Producer::dispatcher`
    directly, then passed after the send-family methods were routed through
    `Producer::fail_if_send_transaction_error`.
  - `ProducerSender::fail_if_transaction_error` is now available to production
    code, and the client helper calls it through the sender owner.
  - Verification:
    `cargo test -p kacrab --all-features producer::client::tests::send_family_transaction_error_guards_route_through_sender --lib`
    passed; `cargo test -p kacrab --all-features producer::client::tests:: --lib`
    passed; `cargo test -p kacrab --all-features producer::sender::tests::producer_sender_checks_transaction_error_through_owned_dispatcher --lib`
    passed; `make fmt`, `make clippy`, `make test`, and `git diff --check`
    passed.
  - This is architecture cleanup only. Kafka stayed stopped, so no Rust/Java
    benchmark was run and this is not parity evidence.
- [x] Removed direct dispatcher read from client sticky-partitioner policy:
  - RED structural test
    `producer::client::tests::sticky_partitioner_policy_does_not_read_dispatcher_from_client_hot_path`
    first failed because `Producer::uses_sticky_partitioner` read
    `Producer::dispatcher` directly in the send/topic-plan hot path.
  - `Producer` now stores the immutable `partitioner_ignore_keys` runtime
    config snapshot and computes the same sticky-policy predicate locally:
    no custom partitioner, unassigned record, and either `ignore_keys=true` or
    the record has no key.
  - This keeps topic-plan sticky decisions off direct dispatcher access while
    preserving the dispatcher-side assignment semantics.
  - Verification:
    `cargo test -p kacrab --all-features producer::client::tests::sticky_partitioner_policy_does_not_read_dispatcher_from_client_hot_path --lib`
    passed; `cargo test -p kacrab --all-features producer::dispatcher::tests::ignore_keys_uses_sticky_default_partitioner --lib`
    passed; `cargo test -p kacrab --all-features producer::client::tests:: --lib`
    passed; `make fmt`, `make clippy`, `make test`, and `git diff --check`
    passed.
  - This is architecture/hot-path cleanup only. Kafka stayed stopped, so no
    Rust/Java benchmark was run and this is not parity evidence.
- [x] Fused batch metadata fetch with default partition assignment under one
  sender lock:
  - RED structural test
    `producer::client::tests::batch_metadata_fetch_and_default_assignment_share_sender_lock`
    first failed because `send_batch`, `send_batch_with_callback`, and
    `send_batch_untracked` fetched batch metadata through one sender lock and
    then called `assign_default_partitions_for_batch`, which took a second
    sender lock for default partition assignment/load-stat refresh.
  - Added `Producer::metadata_for_batch_and_assign_default_partitions`, which
    fetches metadata and, when no custom partitioner is installed, refreshes
    load stats and assigns default partitions while holding the same sender
    guard. The helper still returns the metadata snapshot so installed native
    custom partitioners can assign from the same metadata snapshot.
  - The three public batch send paths now call this combined helper for
    unassigned batches.
  - Verification:
    `cargo test -p kacrab --all-features producer::client::tests::batch_metadata_fetch_and_default_assignment_share_sender_lock --lib`
    passed; `cargo test -p kacrab --all-features producer::client::tests:: --lib`
    passed; `cargo test -p kacrab --all-features --test producer_dispatcher kafka_producer_send_batch`
    passed; `make fmt`, `make clippy`, `make test`, and `git diff --check`
    passed.
  - This is sender lock-boundary cleanup only. Kafka stayed stopped, so no
    Rust/Java benchmark was run and this is not parity evidence.
- [x] Removed the client hot-path sender lock used only to create batch append
  poll budgets:
  - RED structural test
    `producer::client::tests::batch_append_poll_budget_is_not_created_by_client_hot_path_lock`
    first failed because `send_batch`, `send_batch_with_callback`, and
    `send_batch_untracked` each called
    `.sender.lock().await.append_poll_budget()` before appending records.
  - `Producer` now keeps the immutable `max_in_flight_requests` runtime snapshot
    from construction, and the batch send paths create their per-call
    `AppendPollBudget` through
    `ProducerSender::append_poll_budget_for_max_in_flight(...)` without taking
    an async sender lock just to read the threshold.
  - Existing sender-owned append/status helpers still own capacity waiting,
    accumulator append, sticky-ready marking, and dispatch application; this
    change only removes a client-level lock boundary before those helpers.
  - Verification:
    `cargo test -p kacrab --all-features producer::client::tests::batch_append_poll_budget_is_not_created_by_client_hot_path_lock --lib`
    passed after first failing; `cargo test -p kacrab --all-features producer::client::tests:: --lib`
    passed; `cargo test -p kacrab --all-features producer::sender::tests:: --lib`
    passed; `cargo test -p kacrab --all-features --test producer_dispatcher send_batch`
    passed; `make fmt`, `make clippy`, `make test`, and `git diff --check`
    passed.
  - This is sender lock-boundary cleanup only. Kafka stayed stopped after disk
    cleanup, so no Rust/Java benchmark was run and this is not parity evidence.
- [x] Stopped public batch send endings from forcing a final ready-dispatch pass
  on the caller thread:
  - RED structural test
    `producer::client::tests::batch_send_family_leaves_final_ready_dispatch_to_sender_loop_or_explicit_poll`
    first failed because `send_batch`, `send_batch_with_callback`, and
    `send_batch_untracked` ended by calling
    `self.finish_batch_append_dispatch()`.
  - The batch send family now appends records, applies per-append sender-owned
    batch status decisions, wakes the background sender loop through existing
    append notification, and returns. Explicit `poll()` / `flush()` still drive
    dispatch through the sender owner when the caller asks for it.
  - The now-unused `Producer::finish_batch_append_dispatch` wrapper was removed;
    the lower-level sender/state finish helpers are test-only coverage for the
    dispatch primitive.
  - Verification:
    `cargo test -p kacrab --all-features producer::client::tests::batch_send_family_leaves_final_ready_dispatch_to_sender_loop_or_explicit_poll --lib`
    passed after first failing; `cargo test -p kacrab --all-features producer::client::tests:: --lib`
    passed; `cargo test -p kacrab --all-features producer::sender::tests:: --lib`
    passed; `cargo test -p kacrab --all-features --test producer_dispatcher send_batch`
    passed; `cargo test -p kacrab --all-features --test producer_dispatcher background_sender`
    passed; `make fmt`, `make clippy`, `make test`, and `git diff --check`
    passed.
  - This is Java Sender-shape architecture cleanup only. Kafka stayed stopped
    after disk cleanup, so no Rust/Java benchmark was run and this is not parity
    evidence.
- [x] Removed async default-fallback assignment from installed custom partitioner
  batch paths:
  - RED structural test
    `producer::client::tests::batch_custom_partitioner_path_does_not_use_async_fallback_assignment`
    first failed because `send_batch`, `send_batch_with_callback`, and
    `send_batch_untracked` routed installed native custom partition assignment
    through `Producer::assign_partition_with_metadata`, an async helper that is
    also responsible for the no-custom default fallback path.
  - Source check showed `ProducerPartitionerHandle::partition` returns
    `Option<Result<i32>>`: `None` means no custom partitioner is installed, not
    that an installed partitioner declined a record. The batch custom path now
    uses sync `Producer::assign_custom_partition_with_metadata`, validates the
    selected partition, and then appends through the existing sender-owned
    append/status helper.
  - The single-record `assign_partition_with_metadata` helper still keeps the
    async default fallback for the no-custom path.
  - Verification:
    `cargo test -p kacrab --all-features producer::client::tests::batch_custom_partitioner_path_does_not_use_async_fallback_assignment --lib`
    passed after first failing; `cargo test -p kacrab --all-features producer::client::tests:: --lib`
    passed; `cargo test -p kacrab --all-features --test producer_dispatcher kafka_producer_builder_uses_native_partitioner_instead_of_jvm_class_loading`
    passed; `cargo test -p kacrab --all-features --test producer_dispatcher send_batch`
    passed; `make fmt`, `make clippy`, `make test`, and `git diff --check`
    passed.
  - This is batch hot-path cleanup only. Kafka stayed stopped after disk cleanup,
    so no Rust/Java benchmark was run and this is not parity evidence.
- [x] Shared dispatcher metrics enablement across producer and sender dispatcher
  clones:
  - RED test
    `producer::dispatcher::tests::dispatcher_metrics_enablement_is_shared_across_clones`
    first failed because `ProducerDispatcher` cloned `metrics_enabled` as a
    plain bool, so enabling metrics through the producer-owned dispatcher did
    not enable request/retry/error counters on an already-cloned sender
    dispatcher.
  - `ProducerDispatcher::metrics_enabled` is now an `Arc<AtomicBool>`, and
    `ProducerDispatcher::enable_metrics` takes `&self`. The producer no longer
    needs a best-effort sender `try_lock` just to enable metrics on the sender
    clone.
  - Removed the redundant sender `enable_metrics` wrapper and updated tests and
    bench fixtures for the shared enablement API.
  - Verification:
    `cargo test -p kacrab --all-features producer::dispatcher::tests::dispatcher_metrics_enablement_is_shared_across_clones --lib`
    passed after first failing; `cargo test -p kacrab --all-features producer::client::tests::flush_records_local_metrics_like_java --lib`
    passed; `cargo test -p kacrab --all-features producer::client::tests::metrics_registry_exposes_named_snapshot_values_like_java_metrics_map --lib`
    passed; `cargo test -p kacrab --all-features --test producer_dispatcher kafka_producer_metrics_snapshot_reports_queue_and_dispatch_counters`
    passed; `cargo test -p kacrab --all-features producer::dispatcher::tests:: --lib`
    passed; `make fmt`, `make clippy`, and `make test` passed.
  - This is metrics correctness and sender-clone ownership cleanup only. Kafka
    topic data was deleted for disk cleanup, so no Rust/Java benchmark was run
    and this is not parity evidence.
- [x] Lazily start the background sender loop when a producer is constructed
  outside a Tokio runtime:
  - RED test
    `producer::client::tests::producer_built_outside_runtime_starts_sender_loop_lazily`
    first failed because `Producer::from_parts` only tried to spawn the
    background sender loop during construction; outside a runtime this left
    `sender_loop` permanently absent.
  - Added `Producer::ensure_background_sender_loop`, which retries spawning
    from public async send/batch/poll/flush paths once a Tokio runtime is
    available. Existing construction-inside-runtime behavior is unchanged.
  - Verification:
    `cargo test -p kacrab --all-features producer::client::tests::producer_built_outside_runtime_starts_sender_loop_lazily --lib`
    passed after first failing; `cargo test -p kacrab --all-features --test producer_dispatcher background_sender`
    passed; `cargo test -p kacrab --all-features producer::client::tests::batch_send_family_leaves_final_ready_dispatch_to_sender_loop_or_explicit_poll --lib`
    passed; `cargo test -p kacrab --all-features producer::sender::tests::producer_sender_notifies_sender_loop_after_untracked_append --lib`
    passed; `make fmt`, `make clippy`, and `make test` passed.
  - This is Java Sender-shape lifecycle cleanup only. Kafka topic data was
    deleted for disk cleanup, so no Rust/Java benchmark was run and this is not
    parity evidence.
- [x] Moved the background sender loop body out of the public producer facade:
  - RED structural test
    `producer::client::tests::background_sender_loop_body_is_owned_by_sender_module`
    first failed because `client.rs` still contained the
    `drive_wake_until_waiting` loop and switched directly on `SenderLoopWait`.
  - At this step, `ProducerSender::spawn_background_loop` owned the
    wake/dispatch/wait loop body, including metrics observers and
    dispatch-error accounting. A later cleanup superseded that API with
    `ProducerSenderLoop::spawn` / `ProducerSenderLoop::ensure_running`, so the
    facade no longer stores the returned raw `AbortHandle`.
  - This keeps the current callback dispatch cadence unchanged; it does not
    repeat the rejected experiment that fully deferred callback ready dispatch
    to the background loop.
  - Verification:
    `cargo test -p kacrab --all-features producer::client::tests::background_sender_loop_body_is_owned_by_sender_module --lib`
    passed after first failing; `cargo test -p kacrab --all-features producer::client::tests::producer_built_outside_runtime_starts_sender_loop_lazily --lib`
    passed; `cargo test -p kacrab --all-features --test producer_dispatcher background_sender`
    passed; `cargo test -p kacrab --all-features producer::sender::tests::producer_sender_drives_available_wake_work_until_waiting --lib`
    passed; `cargo test -p kacrab --all-features producer::sender::tests::producer_sender_loop_tick_waits_after_reaching_dispatch_completion --lib`
    passed; `make fmt`, `make clippy`, and `make test` passed.
  - This is sender ownership architecture cleanup only. Kafka topic data remains
    deleted after disk cleanup, so no Rust/Java benchmark was run and this is
    not parity evidence.
- [x] Removed the remaining client-side background sender loop wrapper:
  - RED structural assertion inside
    `producer::client::tests::background_sender_loop_body_is_owned_by_sender_module`
    first failed because `client.rs` still defined
    `fn spawn_background_sender_loop(...)` as a pass-through wrapper.
  - At this step, `Producer::from_parts` and
    `Producer::ensure_background_sender_loop` called the sender module helper
    directly. A later cleanup superseded this with
    `ProducerSenderLoop::spawn` / `ProducerSenderLoop::ensure_running`, so the
    public facade no longer owns raw sender-loop lifecycle.
  - Verification:
    `cargo test -p kacrab --all-features producer::client::tests::background_sender_loop_body_is_owned_by_sender_module --lib`
    passed after first failing; `cargo test -p kacrab --all-features producer::client::tests::producer_built_outside_runtime_starts_sender_loop_lazily --lib`
    passed; `cargo test -p kacrab --all-features --test producer_dispatcher background_sender`
    passed; `make fmt`, `make clippy`, and `make test` passed.
  - This is sender ownership architecture cleanup only. Kafka topic data remains
    deleted after disk cleanup, so no Rust/Java benchmark was run and this is
    not parity evidence.
- [x] Routed `partitions_for` metadata fetches through the sender-owned
  dispatcher helper:
  - RED structural test
    `producer::client::tests::partitions_for_fetches_metadata_through_sender_owned_dispatcher`
    first failed because public `Producer::partitions_for` called
    `self.dispatcher.metadata_for_topics(...)` directly.
  - `partitions_for` now locks `ProducerSender` and calls its
    `metadata_for_topics` helper, keeping public producer metadata access on the
    sender-owned dispatcher path. The existing metadata wait metric and returned
    `ProducerPartitionInfo` shape are unchanged.
  - Verification:
    `cargo test -p kacrab --all-features producer::client::tests::partitions_for_fetches_metadata_through_sender_owned_dispatcher --lib`
    passed after first failing; `cargo test -p kacrab --all-features --test producer_dispatcher kafka_producer_partitions_for_returns_topic_metadata`
    passed; `cargo test -p kacrab --all-features producer::sender::tests::producer_sender_fetches_metadata_through_owned_dispatcher --lib`
    passed; `make fmt`, `make clippy`, and `make test` passed.
  - This is sender ownership architecture cleanup only. No native Kafka broker
    process was running and benchmark topic data remains deleted after disk
    cleanup, so no Rust/Java benchmark was run and this is not parity evidence.
- [x] Routed producer telemetry control requests through the sender-owned
  dispatcher helper:
  - RED structural test
    `producer::client::tests::telemetry_control_requests_route_through_sender_owned_dispatcher`
    first failed because `push_telemetry` and
    `fetch_telemetry_subscription` called `self.dispatcher.any_broker_id()` and
    `self.dispatcher.send_control_request(...)` directly.
  - `ProducerSender` now exposes a sender-owned `control_dispatcher()` snapshot
    helper. Producer telemetry subscription and push paths use that snapshot
    instead of calling `Producer::dispatcher` directly.
  - Verification:
    `cargo test -p kacrab --all-features producer::client::tests::telemetry_control_requests_route_through_sender_owned_dispatcher --lib`
    passed after first failing; `cargo test -p kacrab --all-features --test producer_dispatcher telemetry`
    passed; `cargo test -p kacrab --all-features producer::client::tests::producer_otlp_metrics_data_includes_registered_kafka_metrics_like_java --lib`
    passed; `make fmt`, `make clippy`, and `make test` passed.
  - This is sender ownership/control-plane cleanup only. No native Kafka broker
    process was running and benchmark topic data remains deleted after disk
    cleanup, so no Rust/Java benchmark was run and this is not parity evidence.
- [x] Avoided holding the sender lock across telemetry control request network
  awaits:
  - RED structural test
    `producer::client::tests::telemetry_control_requests_route_through_sender_owned_dispatcher`
    first failed because telemetry paths did not snapshot a sender-owned
    dispatcher via `sender.control_dispatcher()`.
  - `ProducerSender` now exposes `control_dispatcher()` as the narrow control
    plane snapshot point. `push_telemetry` and
    `fetch_telemetry_subscription` clone the sender-owned dispatcher while the
    sender lock is held, then perform `any_broker_id` and
    `send_control_request` outside that lock. This keeps control requests on the
    sender-owned dispatcher path without blocking appends/flushes for the
    duration of broker IO.
  - Verification:
    `cargo test -p kacrab --all-features producer::client::tests::telemetry_control_requests_route_through_sender_owned_dispatcher --lib`
    failed first with
    `push_telemetry should snapshot the sender-owned dispatcher under the sender lock`;
    after implementation the same command passed. Also passed:
    `cargo test -p kacrab --all-features producer::client::tests::telemetry --lib`,
    `cargo test -p kacrab --all-features --test producer_dispatcher telemetry`,
    `make fmt`, `make clippy`, and `make test`.
  - This is sender ownership/control-plane cleanup only. No Rust/Java benchmark
    was run after Kafka topic data cleanup, so it is not throughput or parity
    evidence.
- [x] Routed async transaction control through a shared non-blocking dispatcher
  snapshot helper:
  - RED structural test
    `producer::client::tests::async_transaction_control_routes_through_sender_owned_dispatcher_snapshot`
    first failed because `init_transactions_with_max_block` still cloned
    `self.dispatcher` directly.
  - The first implementation used `sender.lock().await.control_dispatcher()`.
    Full `make test` then caught a real regression:
    `kafka_producer_add_partitions_fatal_error_blocks_abort` timed out because
    abort must read fatal transaction state without waiting behind the sender
    loop lock. The implementation was corrected to use a non-blocking
    `Producer::control_dispatcher()` shared dispatcher snapshot, while the async
    transaction methods no longer scatter direct `self.dispatcher` calls.
  - Covered methods: `init_transactions_with_max_block`, `commit_transaction`,
    `abort_transaction`, `end_transaction_with_max_block`,
    `send_offsets_to_transaction`, and
    `send_offsets_to_transaction_with_max_block`. `begin_transaction()` remains
    synchronous and `enable_metrics()` remains a direct shared dispatcher state
    toggle.
  - Verification:
    `cargo test -p kacrab --all-features producer::client::tests::async_transaction_control_uses_nonblocking_dispatcher_snapshot --lib`
    passed after the RED failure and correction;
    `cargo test -p kacrab --all-features --test producer_dispatcher kafka_producer_add_partitions_fatal_error_blocks_abort -- --nocapture`
    passed after reproducing the timeout regression; also passed
    `cargo test -p kacrab --all-features --test producer_dispatcher transaction`,
    `make fmt`, `make clippy`, `make test`, and `git diff --check`.
  - This is transaction/control-plane architecture cleanup only. No Rust/Java
    benchmark was run after Kafka topic data cleanup, so it is not throughput or
    parity evidence.
- [x] Renamed the remaining producer facade dispatcher clone to an explicit
  control-plane field:
  - RED structural test
    `producer::client::tests::producer_facade_names_remaining_dispatcher_clone_as_control_plane`
    first failed because `Producer` still had an ambiguous
    `dispatcher: ProducerDispatcher` field.
  - `Producer` now stores that clone as `control_dispatcher`, and production
    methods use `control_dispatcher` or `ProducerSender` instead of
    `self.dispatcher`. This keeps the naming aligned with the architecture:
    the sender owns hot-path accumulator/dispatch scheduling, while the facade
    keeps only a non-blocking shared control-plane snapshot for synchronous
    APIs and transaction checks.
  - Verification:
    `cargo test -p kacrab --all-features producer::client::tests::producer_facade_names_remaining_dispatcher_clone_as_control_plane --lib`
    failed first with
    `Producer facade should not expose an ambiguous dispatcher field`; after
    implementation the same command passed. Also passed:
    `cargo test -p kacrab --all-features producer::client::tests::commit_transaction --lib`,
    `cargo test -p kacrab --all-features producer::client::tests::abort_transaction_drops_buffered_records_like_java --lib`,
    `make fmt`, `make clippy`, `make test`, and `git diff --check`.
  - This is architecture/readability cleanup only. No Rust/Java benchmark was
    run after Kafka topic data cleanup, so it is not throughput or parity
    evidence.
- [x] Wired Java counter parser tests into the standard `make test` gate:
  - RED Python test
    `benches/scripts/test_producer_counter_metrics.py::test_make_test_runs_benchmark_script_tests`
    first failed because `Makefile` had no `test-bench-scripts` target and
    `make test` only ran Cargo tests.
  - Added `test-bench-scripts` to the Makefile and made `test` depend on it, so
    the Java producer counter parser/averager tests now run before
    `cargo test --workspace --all-features`.
  - Verification:
    `python3 -m unittest benches/scripts/test_producer_counter_metrics.py`
    failed first with `AssertionError: 'test-bench-scripts' not found`; after
    implementation it passed with 3 tests. Also passed:
    `make test-bench-scripts`, `make fmt`, `make clippy`, `make test`, and
    `git diff --check`. The `make test` output now starts with the Python
    counter-parser test run before Cargo tests.
  - This is benchmark parity coverage only. No Rust/Java real Kafka benchmark
    was run, so it is not throughput or parity evidence.
- [x] Made benchmark script tests artifact-clean:
  - RED Python test
    `benches/scripts/test_producer_counter_metrics.py::test_makefile_benchmark_script_tests_do_not_write_pycache`
    first failed because `test-bench-scripts` invoked Python without disabling
    bytecode writes.
  - `test-bench-scripts` now runs with `PYTHONDONTWRITEBYTECODE=1`, keeping the
    benchmark counter-parser gate from creating `benches/scripts/__pycache__`
    in the worktree.
  - Verification:
    `python3 -m unittest benches/scripts/test_producer_counter_metrics.py`
    failed first with the missing `PYTHONDONTWRITEBYTECODE=1` assertion; after
    implementation it passed with 4 tests. Also passed:
    `make test-bench-scripts`, `make fmt`, `make clippy`,
    `rm -rf benches/scripts/__pycache__ && make test && test ! -d benches/scripts/__pycache__`,
    and `git diff --check`.
  - This is benchmark parity gate hygiene only. No Rust/Java real Kafka
    benchmark was run, so it is not throughput or parity evidence.
- [x] Moved sender-loop lifecycle handle ownership out of the producer facade:
  - RED structural test
    `producer::client::tests::producer_facade_does_not_own_raw_sender_loop_abort_handle`
    first failed because `client.rs` still imported `task::AbortHandle` and
    stored `sender_loop: Option<AbortHandle>`.
  - Added `ProducerSenderLoop` in `sender.rs`; `Producer` now stores
    `sender_loop: ProducerSenderLoop`, starts it through
    `ProducerSenderLoop::spawn`, delegates lazy startup/restart to
    `self.sender_loop.ensure_running(...)`, and relies on the wrapper's `Drop`
    to abort the background task. The facade no longer imports, owns, or calls
    the raw sender-loop task spawner directly.
  - Verification:
    `cargo test -p kacrab --all-features producer_facade_does_not_own_raw_sender_loop_abort_handle --lib`
    failed first with
    `Producer facade should not import the raw sender-loop abort handle`; after
    implementation it passed. Also passed:
    `cargo test -p kacrab --all-features producer_built_outside_runtime_starts_sender_loop_lazily --lib`,
    `cargo test -p kacrab --all-features poll_waits_for_one_in_flight_slot_before_spawning_ready_batch --lib`,
    `cargo test -p kacrab --all-features poll_waits_until_blocked_in_flight_task_completes --lib`,
    `cargo test -p kacrab --all-features poll_waits_for_dispatch_slot_before_preparing_ready_batches --lib`,
    `cargo test -p kacrab --all-features background_sender_loop_body_is_owned_by_sender_module --lib`,
    `make fmt`, `make clippy`, `make test`, and `git diff --check`.
  - Follow-up RED/green evidence:
    `cargo test -p kacrab --all-features background_sender_loop_body_is_owned_by_sender_module --lib`
    then failed with
    `Producer facade should create the background loop through the sender-loop handle`;
    after moving spawn/restart into `ProducerSenderLoop::spawn` and
    `ProducerSenderLoop::ensure_running`, the same command passed. Freshly
    passed again: `make fmt`, `make clippy`, `make test`, and
    `git diff --check`.
  - This is sender architecture cleanup only. No Rust/Java real Kafka benchmark
    was run, so it is not throughput or parity evidence.
- [x] Grouped sender runtime state behind a sender-owned runtime handle:
  - RED structural test
    `producer::client::tests::producer_facade_stores_sender_runtime_instead_of_loop_parts`
    first failed because `Producer` still stored loose sender-loop pieces
    instead of one sender runtime component.
  - Added `ProducerSenderRuntime` in `sender.rs`; it owns the shared sender
    lock, sender-loop handle, loop metrics flag, and accumulator batch size used
    by loop metrics. `Producer` now stores `sender: ProducerSenderRuntime` and
    delegates lazy loop startup, loop metrics enablement, test loop inspection,
    and sender lock access through that runtime.
  - Verification:
    `cargo test -p kacrab --all-features producer_facade_stores_sender_runtime_instead_of_loop_parts --lib`
    failed first with
    `Producer facade should store the sender runtime as one owned component`;
    after implementation the same command passed. Also passed:
    `cargo test -p kacrab --all-features background_sender_loop_body_is_owned_by_sender_module --lib`,
    `cargo test -p kacrab --all-features producer_built_outside_runtime_starts_sender_loop_lazily --lib`,
    `cargo test -p kacrab --all-features producer_facade_does_not_own_raw_sender_loop_abort_handle --lib`,
    `cargo test -p kacrab --all-features poll_waits_for_one_in_flight_slot_before_spawning_ready_batch --lib`,
    `cargo test -p kacrab --all-features poll_waits_until_blocked_in_flight_task_completes --lib`,
    `cargo test -p kacrab --all-features poll_waits_for_dispatch_slot_before_preparing_ready_batches --lib`,
    `make fmt`, `make clippy`, `make test`, and `git diff --check`.
  - This is sender architecture cleanup only. No Rust/Java real Kafka benchmark
    was run, so it is not throughput or parity evidence.
- [x] Moved raw sender construction behind `ProducerSenderRuntime`:
  - RED structural test
    `producer::client::tests::producer_facade_delegates_sender_construction_to_runtime`
    first failed because `Producer::from_parts` still called
    `ProducerSender::with_dispatcher(...)` directly.
  - Added `ProducerSenderRuntime::with_dispatcher(...)`; `Producer::from_parts`
    now constructs the sender runtime through that API, and the facade no
    longer imports the raw `ProducerSender` type for construction. The batch
    append poll budget helper used by facade batch APIs is also exposed through
    `ProducerSenderRuntime` so the facade does not need the raw sender type.
  - Verification:
    `cargo test -p kacrab --all-features producer_facade_delegates_sender_construction_to_runtime --lib`
    failed first with
    `Producer facade should construct sender internals through the runtime`;
    after implementation the same command passed. Also passed:
    `cargo test -p kacrab --all-features producer_facade_stores_sender_runtime_instead_of_loop_parts --lib`,
    `cargo test -p kacrab --all-features background_sender_loop_body_is_owned_by_sender_module --lib`,
    `cargo test -p kacrab --all-features producer_built_outside_runtime_starts_sender_loop_lazily --lib`,
    `cargo test -p kacrab --all-features batch_send_family_leaves_final_ready_dispatch_to_sender_loop_or_explicit_poll --lib`,
    `make fmt`, `make clippy`, `make test`, and `git diff --check`.
  - This is sender architecture cleanup only. No Rust/Java real Kafka benchmark
    was run, so it is not throughput or parity evidence.
- [x] Let sender runtime retain the loop metrics handle for restarts:
  - RED structural test
    `producer::client::tests::producer_facade_does_not_pass_metrics_into_sender_loop_restart`
    first failed because `Producer::ensure_background_sender_loop` still passed
    `self.metrics.clone()` into the sender runtime when restarting the loop.
  - `ProducerSenderRuntime` now stores its own `ProducerMetrics` handle, and
    `ensure_loop_running()` is parameterless from the facade. `Producer` now
    only asks the sender runtime to ensure the loop is running; the runtime owns
    the loop restart details and the metrics handle needed by the loop.
  - Verification:
    `cargo test -p kacrab --all-features producer_facade_does_not_pass_metrics_into_sender_loop_restart --lib`
    failed first with
    `Producer facade should ask the sender runtime to ensure its own loop`;
    after implementation the same command passed. Also passed:
    `cargo test -p kacrab --all-features producer_built_outside_runtime_starts_sender_loop_lazily --lib`,
    `cargo test -p kacrab --all-features background_sender_loop_body_is_owned_by_sender_module --lib`,
    `make fmt`, `make clippy`, `make test`, and `git diff --check`.
  - This is sender architecture cleanup only. No Rust/Java real Kafka benchmark
    was run, so it is not throughput or parity evidence.
- [x] Route single-record delivery append dispatch through `ProducerSenderRuntime`:
  - RED structural test
    `producer::client::tests::producer_facade_routes_delivery_append_through_sender_runtime`
    first failed with
    `Producer facade should not lock raw sender state for delivery append dispatch`.
  - Added runtime wrappers for `append_delivery_record_then_apply_dispatch`
    and `append_callback_delivery_record_then_apply_dispatch`. The public
    facade still owns public API validation/interceptor setup, but the sender
    runtime now owns the raw sender lock boundary for the single-record delivery
    append/dispatch path.
  - Verification:
    `cargo test -p kacrab --all-features producer_facade_routes_delivery_append_through_sender_runtime --lib`
    failed first, then passed after implementation. Also passed:
    `cargo test -p kacrab --all-features send_with_callback_without_interceptors_does_not_clone_successful_record --lib`,
    `cargo test -p kacrab --all-features producer_built_outside_runtime_starts_sender_loop_lazily --lib`,
    `make fmt`, `make clippy`, `make test`, and `git diff --check`.
  - This is sender architecture cleanup only. No Rust/Java real Kafka benchmark
    was run, so it is not throughput or parity evidence.

## Reporting Rules

Allowed:

- "Implemented tracker item X; evidence: command Y output Z."
- "Rust benchmark produced number N; Java comparison missing, no parity claim."
- "This item remains open because evidence X is missing."

Not allowed:

- "Done" without source lines and fresh commands.
- "Faster/slower than Java" without same-machine Java 5-run output and matching
  counters.
- "Parity" when only config or harness shape matches.
