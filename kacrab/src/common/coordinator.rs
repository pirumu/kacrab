//! The kind of coordinator a `FindCoordinator` request resolves, mirroring
//! Kafka's `FindCoordinatorRequest.CoordinatorType` (`GROUP` / `TRANSACTION`).

/// The coordinator a `FindCoordinator` request targets, mirroring the `key_type`
/// byte of Kafka's `FindCoordinatorRequest`: a consumer group coordinator or a
/// transaction coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CoordinatorType {
    /// Consumer group coordinator (`key_type` 0).
    Group,
    /// Transaction coordinator (`key_type` 1).
    Transaction,
}

impl CoordinatorType {
    /// The `FindCoordinator` `key_type` wire byte (`Group` = 0, `Transaction` = 1).
    #[must_use]
    pub(crate) const fn key_type(self) -> i8 {
        match self {
            Self::Group => 0,
            Self::Transaction => 1,
        }
    }
}
