#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use kacrab_protocol::generated::stop_replica_response::*;

use crate::TestInstance;

impl TestInstance for StopReplicaResponseData {
    fn test_populated(_version: i16) -> Self {
        Self {
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        Self {
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            _unknown_tagged_fields: Vec::new(),
        }
    }
}
