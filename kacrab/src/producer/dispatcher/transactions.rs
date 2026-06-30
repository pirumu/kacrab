use super::{
    AddPartitionsToTxnRequestData, AddPartitionsToTxnTopic, AddPartitionsToTxnTransaction, Arc,
    AtomicBool, ErrorCode, KafkaString, Mutex, Notify, OffsetAndMetadata, Ordering,
    PENDING_TRANSACTION_OPERATION_MESSAGE, ProduceRoute, ProducerError, ProducerIdempotenceState,
    ProducerIdentity, Result, StdMutex, TopicPartition, TransactionState,
    TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic, TxnOffsetCommitResponseData,
    is_txn_offset_commit_coordinator_error,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransactionOperation {
    InitTransactions,
    SendOffsetsToTransaction,
    EndTransaction { committed: bool },
}

impl TransactionOperation {
    pub(crate) const fn request_kind(self) -> TransactionRequestKind {
        match self {
            Self::InitTransactions => TransactionRequestKind::InitProducerId,
            Self::SendOffsetsToTransaction => TransactionRequestKind::AddPartitionsOrOffsets,
            Self::EndTransaction { .. } => TransactionRequestKind::EndTxn,
        }
    }
}

#[derive(Debug)]
pub(crate) enum TransactionPendingOperationStart {
    Started(TransactionalRequestResult),
    Cached(TransactionalRequestResult),
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PendingTransactionOperationStatus {
    #[default]
    Active,
    TimedOut,
}

#[derive(Debug, Clone)]
pub(crate) struct TransactionalRequestResult {
    pub(crate) inner: Arc<TransactionalRequestResultInner>,
}

#[derive(Debug)]
pub(crate) struct TransactionalRequestResultInner {
    pub(crate) completion: StdMutex<Option<TransactionRequestCompletion>>,
    pub(crate) acked: AtomicBool,
    pub(crate) notify: Notify,
}

impl TransactionalRequestResult {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(TransactionalRequestResultInner {
                completion: StdMutex::new(None),
                acked: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    pub(crate) fn done(&self) {
        self.complete(TransactionRequestCompletion::Success);
    }

    pub(crate) fn fail(&self, error: &ProducerError) {
        self.complete(TransactionRequestCompletion::Failure(
            CachedProducerError::from(error),
        ));
    }

    pub(crate) async fn wait(&self) -> Result<()> {
        loop {
            if let Some(completion) = self.completion()? {
                self.inner.acked.store(true, Ordering::Release);
                return completion.into_result();
            }
            self.inner.notify.notified().await;
        }
    }

    pub(crate) fn completion(&self) -> Result<Option<TransactionRequestCompletion>> {
        let completion = self.inner.completion.lock().map_err(|_error| {
            ProducerError::InvalidTransactionState("cached transaction result lock poisoned")
        })?;
        Ok(completion.clone())
    }

    pub(crate) fn complete(&self, completion: TransactionRequestCompletion) {
        if let Ok(mut pending_completion) = self.inner.completion.lock()
            && pending_completion.is_none()
        {
            *pending_completion = Some(completion);
            self.inner.notify.notify_waiters();
        }
    }

    #[cfg(test)]
    pub(crate) fn is_same_handle(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    #[cfg(test)]
    pub(crate) fn is_completed(&self) -> bool {
        self.completion()
            .is_ok_and(|completion| completion.is_some())
    }

    pub(crate) fn is_acked(&self) -> bool {
        self.inner.acked.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn is_successful(&self) -> bool {
        self.completion().is_ok_and(|completion| {
            matches!(completion, Some(TransactionRequestCompletion::Success))
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) enum TransactionRequestCompletion {
    Success,
    Failure(CachedProducerError),
}

impl TransactionRequestCompletion {
    pub(crate) fn into_result(self) -> Result<()> {
        match self {
            Self::Success => Ok(()),
            Self::Failure(error) => Err(error.into_producer_error()),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum CachedProducerError {
    Transaction {
        operation: &'static str,
        error: ErrorCode,
    },
    InvalidTransactionState(&'static str),
    TransactionalIdRequired,
    TransactionStateBusy,
    InvalidConsumerGroupMetadata(&'static str),
    TelemetryDisabled,
    Telemetry {
        operation: &'static str,
        error: ErrorCode,
    },
    InvalidTelemetrySubscription(&'static str),
    InvalidTelemetryTimeout {
        timeout_ms: i64,
    },
    InvalidCloseTimeout {
        timeout_ms: i64,
    },
    UnsupportedOperation(&'static str),
    DispatchTask(String),
}

impl CachedProducerError {
    pub(crate) fn into_producer_error(self) -> ProducerError {
        match self {
            Self::Transaction { operation, error } => {
                ProducerError::Transaction { operation, error }
            },
            Self::InvalidTransactionState(message) => {
                ProducerError::InvalidTransactionState(message)
            },
            Self::TransactionalIdRequired => ProducerError::TransactionalIdRequired,
            Self::TransactionStateBusy => ProducerError::TransactionStateBusy,
            Self::InvalidConsumerGroupMetadata(message) => {
                ProducerError::InvalidConsumerGroupMetadata(message)
            },
            Self::TelemetryDisabled => ProducerError::TelemetryDisabled,
            Self::Telemetry { operation, error } => ProducerError::Telemetry { operation, error },
            Self::InvalidTelemetrySubscription(message) => {
                ProducerError::InvalidTelemetrySubscription(message)
            },
            Self::InvalidTelemetryTimeout { timeout_ms } => {
                ProducerError::InvalidTelemetryTimeout { timeout_ms }
            },
            Self::InvalidCloseTimeout { timeout_ms } => {
                ProducerError::InvalidCloseTimeout { timeout_ms }
            },
            Self::UnsupportedOperation(operation) => ProducerError::UnsupportedOperation(operation),
            Self::DispatchTask(message) => ProducerError::DispatchTask(message),
        }
    }
}

impl From<&ProducerError> for CachedProducerError {
    fn from(error: &ProducerError) -> Self {
        match error {
            ProducerError::Transaction { operation, error } => Self::Transaction {
                operation,
                error: *error,
            },
            ProducerError::InvalidTransactionState(message) => {
                Self::InvalidTransactionState(message)
            },
            ProducerError::TransactionalIdRequired => Self::TransactionalIdRequired,
            ProducerError::TransactionStateBusy => Self::TransactionStateBusy,
            ProducerError::InvalidConsumerGroupMetadata(message) => {
                Self::InvalidConsumerGroupMetadata(message)
            },
            ProducerError::TelemetryDisabled => Self::TelemetryDisabled,
            ProducerError::Telemetry { operation, error } => Self::Telemetry {
                operation,
                error: *error,
            },
            ProducerError::InvalidTelemetrySubscription(message) => {
                Self::InvalidTelemetrySubscription(message)
            },
            ProducerError::InvalidTelemetryTimeout { timeout_ms } => {
                Self::InvalidTelemetryTimeout {
                    timeout_ms: *timeout_ms,
                }
            },
            ProducerError::InvalidCloseTimeout { timeout_ms } => Self::InvalidCloseTimeout {
                timeout_ms: *timeout_ms,
            },
            ProducerError::UnsupportedOperation(operation) => Self::UnsupportedOperation(operation),
            _ => Self::DispatchTask(format!("cached transaction operation failed: {error}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransactionRequestKind {
    FindCoordinator,
    InitProducerId,
    AddPartitionsOrOffsets,
    EndTxn,
    EpochBump,
}

impl TransactionRequestKind {
    pub(crate) const fn priority(self) -> u8 {
        match self {
            Self::FindCoordinator => 0,
            Self::InitProducerId => 1,
            Self::AddPartitionsOrOffsets => 2,
            Self::EndTxn => 3,
            Self::EpochBump => 4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransactionRequestQueueEntry {
    pub(crate) kind: TransactionRequestKind,
    pub(crate) sequence: u64,
}

#[derive(Debug, Default)]
pub(crate) struct TransactionRequestQueue {
    pub(crate) entries: Vec<TransactionRequestQueueEntry>,
    pub(crate) next_sequence: u64,
}

impl TransactionRequestQueue {
    pub(crate) fn push(&mut self, kind: TransactionRequestKind) {
        self.entries.push(TransactionRequestQueueEntry {
            kind,
            sequence: self.next_sequence,
        });
        self.next_sequence = self.next_sequence.saturating_add(1);
    }

    pub(crate) fn pop_next(&mut self) -> Option<TransactionRequestKind> {
        let index = self.next_index()?;
        Some(self.entries.remove(index).kind)
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Sender-loop transaction request selection is covered before the async \
                      dispatcher consumes it directly."
        )
    )]
    pub(crate) fn next_request(
        &mut self,
        has_incomplete_batches: bool,
    ) -> Option<TransactionRequestKind> {
        let index = self.next_index()?;
        let kind = self.entries.get(index)?.kind;
        if kind == TransactionRequestKind::EndTxn && has_incomplete_batches {
            return None;
        }
        Some(self.entries.remove(index).kind)
    }

    pub(crate) fn remove_first(&mut self, kind: TransactionRequestKind) -> bool {
        if self
            .next_index()
            .and_then(|index| self.entries.get(index))
            .is_some_and(|entry| entry.kind == kind)
        {
            return self.pop_next().is_some();
        }
        let Some(index) = self.entries.iter().position(|entry| entry.kind == kind) else {
            return false;
        };
        let _entry = self.entries.remove(index);
        true
    }

    pub(crate) fn next_index(&self) -> Option<usize> {
        self.entries
            .iter()
            .enumerate()
            .min_by_key(|(_index, entry)| (entry.kind.priority(), entry.sequence))
            .map(|(index, _entry)| index)
    }
}

#[derive(Debug)]
pub(crate) struct TransactionRequestGuard {
    pub(crate) producer_state: Option<Arc<Mutex<ProducerIdempotenceState>>>,
    pub(crate) kind: TransactionRequestKind,
    pub(crate) armed: bool,
}

impl TransactionRequestGuard {
    pub(crate) const fn new(
        producer_state: Arc<Mutex<ProducerIdempotenceState>>,
        kind: TransactionRequestKind,
    ) -> Self {
        Self {
            producer_state: Some(producer_state),
            kind,
            armed: true,
        }
    }

    pub(crate) const fn empty() -> Self {
        Self {
            producer_state: None,
            kind: TransactionRequestKind::FindCoordinator,
            armed: false,
        }
    }

    pub(crate) async fn clear(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(producer_state) = &self.producer_state {
            clear_transaction_request(producer_state, self.kind).await;
        }
        self.armed = false;
    }
}

impl Drop for TransactionRequestGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some(producer_state) = self.producer_state.clone() else {
            return;
        };
        let kind = self.kind;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let _clear_task = handle.spawn(async move {
                clear_transaction_request(&producer_state, kind).await;
            });
        }
    }
}

#[derive(Debug)]
pub(crate) struct PendingTransactionOperationGuard {
    pub(crate) producer_state: Arc<Mutex<ProducerIdempotenceState>>,
    pub(crate) operation: TransactionOperation,
    pub(crate) result: TransactionalRequestResult,
    pub(crate) armed: bool,
}

impl PendingTransactionOperationGuard {
    pub(crate) const fn new(
        producer_state: Arc<Mutex<ProducerIdempotenceState>>,
        operation: TransactionOperation,
        result: TransactionalRequestResult,
    ) -> Self {
        Self {
            producer_state,
            operation,
            result,
            armed: true,
        }
    }

    pub(crate) async fn complete(&mut self, result: &Result<()>) {
        if self.armed {
            match result {
                Ok(()) => self.result.done(),
                Err(error) => self.result.fail(error),
            }
            complete_pending_transaction_operation(&self.producer_state, self.operation).await;
            self.armed = false;
        }
    }
}

impl Drop for PendingTransactionOperationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let producer_state = Arc::clone(&self.producer_state);
        let operation = self.operation;
        self.result.fail(&ProducerError::InvalidTransactionState(
            "pending transaction operation dropped before completion",
        ));
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let _clear_task = handle.spawn(async move {
                clear_pending_transaction_operation(&producer_state, operation).await;
            });
        }
    }
}

pub(crate) fn add_partitions_to_txn_request(
    transactional_id: &str,
    identity: ProducerIdentity,
    route: &ProduceRoute,
) -> AddPartitionsToTxnRequestData {
    AddPartitionsToTxnRequestData {
        transactions: vec![AddPartitionsToTxnTransaction {
            transactional_id: KafkaString::from(transactional_id.to_owned()),
            producer_id: identity.producer_id,
            producer_epoch: identity.producer_epoch,
            verify_only: false,
            topics: vec![AddPartitionsToTxnTopic {
                name: KafkaString::from(route.topic.clone()),
                partitions: vec![route.partition],
                _unknown_tagged_fields: Vec::new(),
            }],
            _unknown_tagged_fields: Vec::new(),
        }],
        ..AddPartitionsToTxnRequestData::default()
    }
}

pub(crate) fn txn_offset_commit_topics(
    offsets: Vec<(TopicPartition, OffsetAndMetadata)>,
) -> Vec<TxnOffsetCommitRequestTopic> {
    let mut topics: Vec<TxnOffsetCommitRequestTopic> = Vec::new();
    for (topic_partition, offset) in offsets {
        let partition = TxnOffsetCommitRequestPartition {
            partition_index: topic_partition.partition,
            committed_offset: offset.offset,
            committed_leader_epoch: offset.leader_epoch.unwrap_or(-1),
            committed_metadata: Some(KafkaString::from(offset.metadata.unwrap_or_default())),
            ..TxnOffsetCommitRequestPartition::default()
        };
        if let Some(topic) = topics
            .iter_mut()
            .find(|topic| topic.name.to_string() == topic_partition.topic)
        {
            topic.partitions.push(partition);
        } else {
            topics.push(TxnOffsetCommitRequestTopic {
                name: KafkaString::from(topic_partition.topic),
                partitions: vec![partition],
                _unknown_tagged_fields: Vec::new(),
            });
        }
    }
    topics
}

pub(crate) fn fail_transaction_state_if_needed(
    state: &ProducerIdempotenceState,
    allow_abortable_abort: bool,
) -> Result<()> {
    if let Some(error) = state.fatal_error {
        return Err(ProducerError::Transaction {
            operation: "transaction_state",
            error,
        });
    }
    if state.transaction_state == TransactionState::FatalError {
        return Err(ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::InvalidTxnState,
        });
    }
    if !allow_abortable_abort && let Some(error) = state.abortable_error {
        return Err(ProducerError::Transaction {
            operation: "transaction_state",
            error,
        });
    }
    if !allow_abortable_abort && state.transaction_state == TransactionState::AbortableError {
        return Err(ProducerError::Transaction {
            operation: "transaction_state",
            error: ErrorCode::InvalidTxnState,
        });
    }
    Ok(())
}

pub(crate) fn fail_pending_transaction_operation(
    state: &mut ProducerIdempotenceState,
) -> Result<()> {
    clear_acked_pending_transaction_operation(state);
    if state.pending_operation.is_some() {
        return Err(ProducerError::InvalidTransactionState(
            PENDING_TRANSACTION_OPERATION_MESSAGE,
        ));
    }
    Ok(())
}

pub(crate) fn clear_acked_pending_transaction_operation(state: &mut ProducerIdempotenceState) {
    let Some(pending_operation) = state.pending_operation else {
        return;
    };
    if state
        .pending_result
        .as_ref()
        .is_some_and(TransactionalRequestResult::is_acked)
    {
        state.clear_pending_transaction_operation(pending_operation);
    }
}

pub(crate) async fn complete_pending_transaction_operation(
    producer_state: &Mutex<ProducerIdempotenceState>,
    operation: TransactionOperation,
) {
    let mut state = producer_state.lock().await;
    let _removed = state
        .pending_requests
        .remove_first(operation.request_kind());
    if state.pending_operation == Some(operation) {
        let keep_for_retry = state.pending_operation_status
            == PendingTransactionOperationStatus::TimedOut
            && state
                .pending_result
                .as_ref()
                .is_some_and(|result| !result.is_acked());
        if !keep_for_retry {
            state.clear_pending_transaction_operation(operation);
        }
    }
}

pub(crate) async fn clear_pending_transaction_operation(
    producer_state: &Mutex<ProducerIdempotenceState>,
    operation: TransactionOperation,
) {
    let mut state = producer_state.lock().await;
    state.clear_pending_transaction_operation(operation);
}

pub(crate) async fn clear_transaction_request(
    producer_state: &Mutex<ProducerIdempotenceState>,
    kind: TransactionRequestKind,
) {
    let mut state = producer_state.lock().await;
    let _removed = state.pending_requests.remove_first(kind);
}

pub(crate) fn is_add_partitions_retry_error(error: ErrorCode) -> bool {
    error.is_retriable() || matches!(error, ErrorCode::ConcurrentTransactions)
}

pub(crate) fn txn_offset_commit_error(response: &TxnOffsetCommitResponseData) -> Option<ErrorCode> {
    let mut coordinator_error = None;
    let mut retriable_error = None;
    for error in response
        .topics
        .iter()
        .flat_map(|topic| topic.partitions.iter())
        .map(|partition| ErrorCode::from(partition.error_code))
        .filter(ErrorCode::is_error)
    {
        if is_txn_offset_commit_coordinator_error(error) {
            let _ = coordinator_error.get_or_insert(error);
            continue;
        }
        if error.is_retriable() {
            let _ = retriable_error.get_or_insert(error);
            continue;
        }
        return Some(error);
    }
    coordinator_error.or(retriable_error)
}
