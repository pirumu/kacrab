//! Consumer group metadata, returned by `Consumer::group_metadata` and used by
//! transactional offset commits.

use std::fmt;

/// Consumer group metadata, returned by `Consumer::group_metadata` and used by
/// transactional offset commits.
#[derive(Debug, Clone, PartialEq, Eq)]
#[expect(
    clippy::struct_field_names,
    reason = "Field names intentionally mirror Kafka's ConsumerGroupMetadata accessors."
)]
pub struct ConsumerGroupMetadata {
    /// Consumer group id.
    pub group_id: String,
    /// Consumer group generation id, or `-1` when unknown.
    pub generation_id: i32,
    /// Consumer group member id, or an empty string when unknown.
    pub member_id: String,
    /// Optional static group instance id.
    pub group_instance_id: Option<String>,
}

impl ConsumerGroupMetadata {
    /// Create consumer group metadata.
    #[must_use]
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            generation_id: -1,
            member_id: String::new(),
            group_instance_id: None,
        }
    }

    /// Create consumer group metadata with the full Kafka constructor shape.
    #[must_use]
    pub fn from_parts(
        group_id: impl Into<String>,
        generation_id: i32,
        member_id: impl Into<String>,
        group_instance_id: Option<String>,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            generation_id,
            member_id: member_id.into(),
            group_instance_id,
        }
    }

    /// Set the consumer group generation id.
    #[must_use]
    pub const fn generation_id(mut self, generation_id: i32) -> Self {
        self.generation_id = generation_id;
        self
    }

    /// Set the consumer group member id.
    #[must_use]
    pub fn member_id(mut self, member_id: impl Into<String>) -> Self {
        self.member_id = member_id.into();
        self
    }

    /// Set the optional static group instance id.
    #[must_use]
    pub fn group_instance_id(mut self, group_instance_id: impl Into<String>) -> Self {
        self.group_instance_id = Some(group_instance_id.into());
        self
    }
}

impl fmt::Display for ConsumerGroupMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "GroupMetadata(groupId = {}, generationId = {}, memberId = {}, groupInstanceId = {})",
            self.group_id,
            self.generation_id,
            self.member_id,
            self.group_instance_id.as_deref().unwrap_or("")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::ConsumerGroupMetadata;

    #[test]
    fn consumer_group_metadata_display_matches_java_to_string_shape() {
        let metadata = ConsumerGroupMetadata::new("group-a")
            .generation_id(42)
            .member_id("member-a")
            .group_instance_id("instance-a");

        assert_eq!(
            metadata.to_string(),
            "GroupMetadata(groupId = group-a, generationId = 42, memberId = member-a, \
             groupInstanceId = instance-a)"
        );

        assert_eq!(
            ConsumerGroupMetadata::new("group-a").to_string(),
            "GroupMetadata(groupId = group-a, generationId = -1, memberId = , groupInstanceId = )"
        );
    }

    #[test]
    fn consumer_group_metadata_from_parts_matches_java_full_constructor_shape() {
        let metadata =
            ConsumerGroupMetadata::from_parts("group-a", 42, "member-a", Some("instance-a".into()));

        assert_eq!(metadata.group_id, "group-a");
        assert_eq!(metadata.generation_id, 42);
        assert_eq!(metadata.member_id, "member-a");
        assert_eq!(metadata.group_instance_id.as_deref(), Some("instance-a"));
    }
}
