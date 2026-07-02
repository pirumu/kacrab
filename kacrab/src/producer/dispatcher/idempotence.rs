use super::{
    AHashMap, AHashSet, BTreeSet, ErrorCode, INVALID_LAST_ACKED_OFFSET, IdempotentRetryDecision,
    NO_LAST_ACKED_SEQUENCE, PENDING_TRANSACTION_OPERATION_MESSAGE,
    PendingTransactionOperationStatus, ProduceRoute, ProducerError, ProducerIdentity, Result,
    TransactionOperation, TransactionPendingOperationStart, TransactionRequestQueue,
    TransactionState, TransactionalRequestResult, increment_sequence,
};

#[derive(Debug, Default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Mirrors Kafka's TransactionManager flat transaction/idempotence state flags."
)]
pub(crate) struct ProducerIdempotenceState {
    pub(crate) identity: Option<ProducerIdentity>,
    /// Per-partition idempotent bookkeeping (Kafka `TxnPartitionMap` /
    /// `TxnPartitionEntry`): next sequence, last-acked sequence and offset, and
    /// any unresolved-sequence marker.
    pub(crate) partitions: AHashMap<TopicPartitionKey, IdempotentPartitionEntry>,
    pub(crate) coordinator_id: Option<i32>,
    pub(crate) transaction_state: TransactionState,
    pub(crate) in_transaction: bool,
    pub(crate) transaction_started: bool,
    pub(crate) new_partitions_in_transaction: AHashSet<TopicPartitionKey>,
    pub(crate) pending_partitions_in_transaction: AHashSet<TopicPartitionKey>,
    pub(crate) partitions_in_transaction: AHashSet<TopicPartitionKey>,
    pub(crate) abortable_error: Option<ErrorCode>,
    pub(crate) fatal_error: Option<ErrorCode>,
    pub(crate) epoch_bump_required: bool,
    /// Set once the transaction coordinator is observed to advertise an
    /// `InitProducerId` version below v3, meaning it cannot bump the producer
    /// epoch. Kafka's `coordinatorSupportsBumpingEpoch` (the inverse) gates
    /// whether an abortable error that needs an epoch bump can be recovered or
    /// must escalate to a fatal error. Defaults to `false` (assume supported),
    /// matching modern brokers; flipped only when an old coordinator is seen.
    pub(crate) coordinator_lacks_epoch_bump_support: bool,
    pub(crate) pending_operation: Option<TransactionOperation>,
    pub(crate) pending_result: Option<TransactionalRequestResult>,
    pub(crate) pending_operation_status: PendingTransactionOperationStatus,
    pub(crate) pending_requests: TransactionRequestQueue,
}

impl ProducerIdempotenceState {
    pub(crate) const fn transition_to(&mut self, target: TransactionState) -> Result<()> {
        if !self.transaction_state.is_transition_valid(target) {
            self.transaction_state = TransactionState::FatalError;
            self.fatal_error = Some(ErrorCode::InvalidTxnState);
            return Err(ProducerError::InvalidTransactionState(
                "invalid transaction state transition",
            ));
        }
        self.transaction_state = target;
        match target {
            TransactionState::InTransaction
            | TransactionState::PreparedTransaction
            | TransactionState::CommittingTransaction
            | TransactionState::AbortingTransaction
            | TransactionState::AbortableError => {
                self.in_transaction = true;
            },
            TransactionState::Uninitialized
            | TransactionState::Initializing
            | TransactionState::Ready
            | TransactionState::FatalError => {
                self.in_transaction = false;
            },
        }
        Ok(())
    }

    pub(crate) fn mark_new_transaction_partition(&mut self, key: TopicPartitionKey) -> bool {
        if self.transaction_contains_partition(&key) || self.is_partition_pending_add(&key) {
            return false;
        }
        self.new_partitions_in_transaction.insert(key)
    }

    pub(crate) fn begin_pending_transaction_partitions(&mut self) -> Vec<TopicPartitionKey> {
        let pending: Vec<_> = self.new_partitions_in_transaction.drain().collect();
        self.pending_partitions_in_transaction
            .extend(pending.iter().cloned());
        pending
    }

    pub(crate) fn complete_pending_transaction_partitions(
        &mut self,
        partitions: &[TopicPartitionKey],
    ) {
        for partition in partitions {
            let _removed = self.pending_partitions_in_transaction.remove(partition);
            let _inserted = self.partitions_in_transaction.insert(partition.clone());
        }
    }

    pub(crate) fn fail_pending_transaction_partitions(&mut self, partitions: &[TopicPartitionKey]) {
        for partition in partitions {
            let _removed = self.pending_partitions_in_transaction.remove(partition);
        }
    }

    pub(crate) fn is_partition_pending_add(&self, key: &TopicPartitionKey) -> bool {
        self.new_partitions_in_transaction.contains(key)
            || self.pending_partitions_in_transaction.contains(key)
    }

    pub(crate) fn transaction_contains_partition(&self, key: &TopicPartitionKey) -> bool {
        self.partitions_in_transaction.contains(key)
    }

    pub(crate) fn clear_pending_transaction_operation(&mut self, operation: TransactionOperation) {
        let _removed = self.pending_requests.remove_first(operation.request_kind());
        if self.pending_operation == Some(operation) {
            self.pending_operation = None;
            self.pending_result = None;
            self.pending_operation_status = PendingTransactionOperationStatus::Active;
        }
    }

    pub(crate) fn begin_pending_transaction_operation(
        &mut self,
        operation: TransactionOperation,
    ) -> Result<TransactionPendingOperationStart> {
        if let Some(pending_operation) = self.pending_operation {
            if let Some(result) = self.pending_result.clone() {
                if result.is_acked() {
                    self.clear_pending_transaction_operation(pending_operation);
                } else if pending_operation == operation {
                    return Ok(TransactionPendingOperationStart::Cached(result));
                } else {
                    return Err(ProducerError::InvalidTransactionState(
                        PENDING_TRANSACTION_OPERATION_MESSAGE,
                    ));
                }
            } else {
                return Err(ProducerError::InvalidTransactionState(
                    PENDING_TRANSACTION_OPERATION_MESSAGE,
                ));
            }
        }

        let result = TransactionalRequestResult::new();
        self.pending_operation = Some(operation);
        self.pending_result = Some(result.clone());
        self.pending_operation_status = PendingTransactionOperationStatus::Active;
        self.pending_requests.push(operation.request_kind());
        Ok(TransactionPendingOperationStart::Started(result))
    }

    pub(crate) fn reset_transaction_after_end(&mut self, clear_abortable_error: bool) {
        self.in_transaction = false;
        self.transaction_state = TransactionState::Ready;
        self.transaction_started = false;
        self.new_partitions_in_transaction.clear();
        self.pending_partitions_in_transaction.clear();
        self.partitions_in_transaction.clear();
        if clear_abortable_error {
            self.abortable_error = None;
        }
    }

    pub(crate) fn reset_sequences_after_epoch_bump(&mut self) {
        self.partitions.clear();
        self.epoch_bump_required = false;
    }

    /// Kafka `requestIdempotentEpochBumpForPartition`: flag that the producer epoch
    /// must be bumped before the next produce (applied in `producer_batch_state`),
    /// healing a sequence gap left by a terminally failed batch.
    pub(crate) const fn request_epoch_bump(&mut self) {
        self.epoch_bump_required = true;
    }

    pub(crate) fn next_sequence(
        &mut self,
        topic: &str,
        partition: i32,
        record_count: i32,
    ) -> Result<i32> {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let entry = self.partitions.entry(key).or_default();
        if entry.unresolved_next_sequence.is_some() {
            return Err(ProducerError::UnresolvedSequence {
                topic: topic.to_owned(),
                partition,
            });
        }
        let base_sequence = entry.next_sequence;
        entry.next_sequence = increment_sequence(base_sequence, record_count);
        Ok(base_sequence)
    }

    pub(crate) fn mark_sequence_unresolved(
        &mut self,
        topic: &str,
        partition: i32,
        base_sequence: i32,
        record_count: i32,
    ) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let next_sequence = increment_sequence(base_sequence, record_count);
        let entry = self.partitions.entry(key).or_default();
        entry.unresolved_next_sequence = Some(
            entry
                .unresolved_next_sequence
                .map_or(next_sequence, |existing| existing.max(next_sequence)),
        );
    }

    pub(crate) fn reset_sequence(&mut self, topic: &str, partition: i32) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        // Kafka startSequencesAtBeginning: sequence restarts at 0 with no acks.
        let entry = self.partitions.entry(key).or_default();
        entry.unresolved_next_sequence = None;
        entry.unresolved_loss_ambiguous = false;
        entry.next_sequence = 0;
        entry.last_acked_sequence = NO_LAST_ACKED_SEQUENCE;
        entry.last_acked_offset = INVALID_LAST_ACKED_OFFSET;
    }

    pub(crate) fn release_sequence(&mut self, topic: &str, partition: i32, base_sequence: i32) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        if let Some(entry) = self.partitions.get_mut(&key)
            && entry
                .unresolved_next_sequence
                .is_some_and(|sequence| sequence <= base_sequence)
        {
            entry.unresolved_next_sequence = None;
        }
    }

    /// Kafka `TxnPartitionEntry::addInflightBatch`: track a dispatched batch's base
    /// sequence as in flight for its partition. Re-adding a retried batch's
    /// sequence is a no-op (the set already holds it).
    pub(crate) fn register_inflight_sequence(
        &mut self,
        topic: &str,
        partition: i32,
        base_sequence: i32,
    ) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let _inserted = self
            .partitions
            .entry(key)
            .or_default()
            .inflight_by_sequence
            .insert(base_sequence);
    }

    /// Kafka `TxnPartitionEntry::removeInFlightBatch`: drop a base sequence once its
    /// batch terminally completes (NOT on requeue).
    pub(crate) fn remove_inflight_sequence(
        &mut self,
        topic: &str,
        partition: i32,
        base_sequence: i32,
    ) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        if let Some(entry) = self.partitions.get_mut(&key) {
            let _removed = entry.inflight_by_sequence.remove(&base_sequence);
        }
    }

    /// Kafka `TransactionManager::hasInflightBatches`: whether the partition still
    /// has any batch dispatched-but-not-terminally-completed. Gates
    /// `resolve_unresolved_sequence_after_drain`.
    pub(crate) fn has_inflight_batches(&self, topic: &str, partition: i32) -> bool {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        self.partitions
            .get(&key)
            .is_some_and(|entry| !entry.inflight_by_sequence.is_empty())
    }

    /// Whether a batch's stamped producer id/epoch is stale relative to the current
    /// producer identity (Kafka `hasStaleProducerIdAndEpoch`). True after an epoch
    /// bump for any batch still carrying the old epoch — such a batch must be
    /// re-stamped (fresh sequence under the new epoch) before it is sent, which is
    /// kacrab's equivalent of Kafka `startSequencesAtBeginning` renumbering an
    /// in-flight batch in place.
    pub(crate) fn is_stale_identity(&self, identity: ProducerIdentity) -> bool {
        self.identity.is_some_and(|current| current != identity)
    }

    /// Kafka `firstInFlightSequence`: the lowest in-flight base sequence for a
    /// partition, or `None` when nothing is in flight. The drain gate defers a
    /// retried batch whose base sequence is not this value, so retries re-send
    /// strictly in sequence order.
    pub(crate) fn first_inflight_sequence(&self, topic: &str, partition: i32) -> Option<i32> {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        self.partitions
            .get(&key)
            .and_then(|entry| entry.inflight_by_sequence.iter().next().copied())
    }

    /// Record whether the loss that left a partition unresolved was ambiguous, so a
    /// deferred resolve (run later, once the partition has drained) bumps the epoch
    /// only for ambiguous losses. Sticky across multiple contributing losses.
    pub(crate) fn record_unresolved_loss_ambiguity(
        &mut self,
        topic: &str,
        partition: i32,
        ambiguous: bool,
    ) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let entry = self.partitions.entry(key).or_default();
        entry.unresolved_loss_ambiguous = entry.unresolved_loss_ambiguous || ambiguous;
    }

    pub(crate) fn unresolved_loss_ambiguous(&self, topic: &str, partition: i32) -> bool {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        self.partitions
            .get(&key)
            .is_some_and(|entry| entry.unresolved_loss_ambiguous)
    }

    /// Kafka `maybeResolveSequences`: once a partition's in-flight batches have
    /// drained with its sequence still unresolved, either confirm it was fully
    /// acknowledged (drop the marker) or recover. When unacked messages remain,
    /// a transactional producer transitions to an abortable error (or fatal when
    /// the coordinator cannot bump the epoch) and an idempotent producer requests
    /// an epoch bump.
    ///
    /// The two boolean flags are order-sensitive: `transactional` picks the
    /// transactional recovery path (abortable, or fatal when the coordinator
    /// cannot bump the epoch) over the idempotent epoch bump; `loss_is_ambiguous`
    /// marks a final failure that could not rule out a partial write, which the
    /// idempotent path requires before bumping.
    pub(crate) fn resolve_unresolved_sequence_after_drain(
        &mut self,
        topic: &str,
        partition: i32,
        transactional: bool,
        loss_is_ambiguous: bool,
    ) {
        // Kafka `maybeResolveSequences` only resolves a partition once it has NO
        // in-flight batches. With multiple in-flight requests per partition, an
        // ambiguous timeout on one batch must NOT bump the epoch while later
        // batches are still in flight under the current epoch — defer until the
        // partition drains. The deferred resolve is retriggered from
        // `release_idempotent_inflight_after_terminal` as each request completes.
        if self.has_inflight_batches(topic, partition) {
            return;
        }
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let (has_unresolved, resolved) = match self.partitions.get(&key) {
            Some(entry) => (
                entry.unresolved_next_sequence.is_some(),
                // isNextSequence: nextSequence - lastAcked == 1 => fully acked.
                // Kafka uses wrapping int subtraction so it stays correct across the
                // i32::MAX sequence wraparound.
                entry.last_acked_sequence != NO_LAST_ACKED_SEQUENCE
                    && entry.next_sequence.wrapping_sub(entry.last_acked_sequence) == 1,
            ),
            None => return,
        };
        if !has_unresolved {
            return;
        }
        if !resolved {
            if transactional {
                if self.in_transaction || self.transaction_state.is_completing() {
                    if self.coordinator_lacks_epoch_bump_support {
                        self.transaction_state = TransactionState::FatalError;
                    } else {
                        self.transaction_state = TransactionState::AbortableError;
                        self.epoch_bump_required = true;
                    }
                }
            } else if loss_is_ambiguous {
                // An idempotent (non-transactional) batch whose final failure was a
                // no-response/connection loss MIGHT have been written by the broker,
                // so the per-producer sequence state is now ambiguous and the epoch
                // must be bumped before the next produce. A definitive rejection
                // (e.g. NotLeaderOrFollower) means the records were never written, so
                // the sequence can simply be released/rewound without an epoch bump.
                self.epoch_bump_required = true;
            }
        }
        if let Some(entry) = self.partitions.get_mut(&key) {
            entry.unresolved_next_sequence = None;
            entry.unresolved_loss_ambiguous = false;
        }
    }

    /// Kafka `TxnPartitionEntry::maybeUpdateLastAckedSequence`: record the highest
    /// acknowledged sequence for a partition.
    pub(crate) fn maybe_update_last_acked_sequence(
        &mut self,
        topic: &str,
        partition: i32,
        sequence: i32,
    ) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let entry = self.partitions.entry(key).or_default();
        if sequence > entry.last_acked_sequence {
            entry.last_acked_sequence = sequence;
        }
    }

    /// Kafka `TxnPartitionMap::updateLastAckedOffset`: record the highest
    /// acknowledged base offset, used to disambiguate `UnknownProducerId`.
    pub(crate) fn update_last_acked_offset(
        &mut self,
        topic: &str,
        partition: i32,
        last_offset: i64,
    ) {
        if last_offset == INVALID_LAST_ACKED_OFFSET {
            return;
        }
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let entry = self.partitions.entry(key).or_default();
        if last_offset > entry.last_acked_offset {
            entry.last_acked_offset = last_offset;
        }
    }

    pub(crate) fn last_acked_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        self.partitions
            .get(&key)
            .map(|entry| entry.last_acked_offset)
            .filter(|offset| *offset != INVALID_LAST_ACKED_OFFSET)
    }

    pub(crate) fn should_reset_sequence_for_idempotent_retry(
        &self,
        decision: IdempotentRetryDecision<'_>,
    ) -> bool {
        if matches!(decision.error, ErrorCode::UnknownProducerId) {
            // Retention elapsed (lastAckedOffset < logStartOffset) is recoverable
            // by resetting; genuine data loss is not. Without a last-acked offset
            // we fall back to Kafka's logStartOffset != -1 heuristic.
            if let Some(last_acked_offset) =
                self.last_acked_offset(decision.topic, decision.partition)
                && decision.log_start_offset != -1
            {
                return last_acked_offset < decision.log_start_offset;
            }
            return decision.log_start_offset != -1;
        }
        if !matches!(decision.error, ErrorCode::OutOfOrderSequenceNumber) {
            return true;
        }
        let Some(base_sequence) = decision.base_sequence else {
            return true;
        };
        let key = TopicPartitionKey {
            topic: decision.topic.to_owned(),
            partition: decision.partition,
        };
        self.partitions
            .get(&key)
            .and_then(|entry| entry.unresolved_next_sequence)
            .is_none_or(|unresolved_next_sequence| base_sequence == unresolved_next_sequence)
    }

    pub(crate) fn rewind_sequence_to(&mut self, topic: &str, partition: i32, base_sequence: i32) {
        let key = TopicPartitionKey {
            topic: topic.to_owned(),
            partition,
        };
        let entry = self
            .partitions
            .entry(key)
            .or_insert_with(|| IdempotentPartitionEntry {
                next_sequence: base_sequence,
                ..IdempotentPartitionEntry::default()
            });
        entry.unresolved_next_sequence = None;
        if entry.next_sequence >= base_sequence {
            entry.next_sequence = base_sequence;
        }
    }
}

/// Per-partition idempotent bookkeeping, mirroring Kafka `TxnPartitionEntry`.
#[derive(Debug, Clone)]
pub(crate) struct IdempotentPartitionEntry {
    /// Base sequence of the next batch bound for this partition.
    pub(crate) next_sequence: i32,
    /// Sequence of the last record of the last acknowledged batch, or
    /// [`NO_LAST_ACKED_SEQUENCE`] when nothing has been acknowledged.
    pub(crate) last_acked_sequence: i32,
    /// Last acknowledged base offset, or [`INVALID_LAST_ACKED_OFFSET`].
    pub(crate) last_acked_offset: i64,
    /// When set, new sends to this partition block until the marked sequence is
    /// resolved (Kafka `partitionsWithUnresolvedSequences`).
    pub(crate) unresolved_next_sequence: Option<i32>,
    /// Whether the loss that marked this partition unresolved was ambiguous (a
    /// no-response timeout that MIGHT have been written), recorded so the deferred
    /// resolve (run once the partition has no in-flight batches) bumps the epoch
    /// only for ambiguous losses. Kafka carries this via the batch's last error.
    pub(crate) unresolved_loss_ambiguous: bool,
    /// Base sequences of this partition's batches that have been dispatched at
    /// least once and not yet terminally completed (Kafka
    /// `TxnPartitionEntry::inflightBatchesBySequence`). A sequence is added when
    /// its batch is dispatched and removed only on terminal completion (NOT on
    /// requeue), so `has_inflight_batches` gates `maybeResolveSequences`: an
    /// unresolved sequence is only resolved (and the epoch bumped) once every
    /// in-flight batch for the partition has drained — otherwise an ambiguous
    /// timeout on one batch could bump the epoch while later batches are still in
    /// flight under the old epoch.
    pub(crate) inflight_by_sequence: BTreeSet<i32>,
}

impl Default for IdempotentPartitionEntry {
    fn default() -> Self {
        Self {
            next_sequence: 0,
            last_acked_sequence: NO_LAST_ACKED_SEQUENCE,
            last_acked_offset: INVALID_LAST_ACKED_OFFSET,
            unresolved_next_sequence: None,
            unresolved_loss_ambiguous: false,
            inflight_by_sequence: BTreeSet::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TopicPartitionKey {
    pub(crate) topic: String,
    pub(crate) partition: i32,
}

impl From<&ProduceRoute> for TopicPartitionKey {
    fn from(route: &ProduceRoute) -> Self {
        Self {
            topic: route.topic.clone(),
            partition: route.partition,
        }
    }
}
