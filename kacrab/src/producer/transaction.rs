//! Producer id, sequence, and transaction state helpers.

/// Producer identity assigned by Kafka's `InitProducerId` API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerIdentity {
    /// Kafka producer id.
    pub producer_id: i64,
    /// Kafka producer epoch.
    pub producer_epoch: i16,
}

/// Record-batch idempotence fields for one topic-partition batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerBatchState {
    /// Producer identity used to write this batch.
    pub identity: ProducerIdentity,
    /// Base sequence for the first record in this batch.
    pub base_sequence: i32,
}

/// Java `TransactionManager.State` equivalent.
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "The full Java transaction state enum is ported before every 2PC transition is \
                  wired."
    )
)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransactionState {
    #[default]
    Uninitialized,
    Initializing,
    Ready,
    InTransaction,
    PreparedTransaction,
    CommittingTransaction,
    AbortingTransaction,
    AbortableError,
    FatalError,
}

impl TransactionState {
    #[cfg(test)]
    pub(crate) const ALL: [Self; 9] = [
        Self::Uninitialized,
        Self::Initializing,
        Self::Ready,
        Self::InTransaction,
        Self::PreparedTransaction,
        Self::CommittingTransaction,
        Self::AbortingTransaction,
        Self::AbortableError,
        Self::FatalError,
    ];

    pub(crate) const fn is_transition_valid(self, target: Self) -> bool {
        match target {
            Self::Uninitialized => matches!(self, Self::Ready | Self::AbortableError),
            Self::Initializing => matches!(
                self,
                Self::Uninitialized | Self::CommittingTransaction | Self::AbortingTransaction
            ),
            Self::Ready => matches!(
                self,
                Self::Initializing | Self::CommittingTransaction | Self::AbortingTransaction
            ),
            Self::InTransaction => matches!(self, Self::Ready),
            Self::PreparedTransaction => matches!(self, Self::InTransaction | Self::Initializing),
            Self::CommittingTransaction => {
                matches!(self, Self::InTransaction | Self::PreparedTransaction)
            },
            Self::AbortingTransaction => matches!(
                self,
                Self::InTransaction | Self::PreparedTransaction | Self::AbortableError
            ),
            Self::AbortableError => matches!(
                self,
                Self::InTransaction
                    | Self::CommittingTransaction
                    | Self::AbortableError
                    | Self::Initializing
            ),
            Self::FatalError => true,
        }
    }

    pub(crate) const fn is_completing(self) -> bool {
        matches!(
            self,
            Self::CommittingTransaction | Self::AbortingTransaction
        )
    }
}

#[cfg(test)]
mod tests {
    use super::TransactionState;

    #[test]
    fn is_completing_only_while_committing_or_aborting() {
        for state in [
            TransactionState::CommittingTransaction,
            TransactionState::AbortingTransaction,
        ] {
            assert!(state.is_completing(), "{state:?} should be completing");
        }
        for state in [
            TransactionState::Uninitialized,
            TransactionState::Initializing,
            TransactionState::Ready,
            TransactionState::InTransaction,
            TransactionState::PreparedTransaction,
            TransactionState::AbortableError,
            TransactionState::FatalError,
        ] {
            assert!(!state.is_completing(), "{state:?} should not be completing");
        }
    }
}
