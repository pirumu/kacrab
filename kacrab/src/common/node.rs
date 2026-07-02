//! A broker node in the cluster, mirroring Kafka's `org.apache.kafka.common.Node`.

/// A broker node in the cluster, as returned by cluster/topic/group describe
/// operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    /// Broker id.
    pub id: i32,
    /// Advertised host.
    pub host: String,
    /// Advertised port.
    pub port: i32,
    /// Rack identifier, if the broker advertises one.
    pub rack: Option<String>,
}

impl Node {
    /// Create a broker node without a rack.
    #[must_use]
    pub fn new(id: i32, host: impl Into<String>, port: i32) -> Self {
        Self {
            id,
            host: host.into(),
            port,
            rack: None,
        }
    }

    /// Attach a rack identifier.
    #[must_use]
    pub fn with_rack(mut self, rack: impl Into<String>) -> Self {
        self.rack = Some(rack.into());
        self
    }

    /// Whether this node is the sentinel "no node", detected by a negative id
    /// (Kafka's `Node.noNode()` sentinel). This intentionally checks only the id,
    /// unlike Java's `Node.isEmpty`, which also treats a null/blank host as empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.id < 0
    }
}
