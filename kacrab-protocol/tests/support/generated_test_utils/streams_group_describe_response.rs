#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::streams_group_describe_response::*, *};

use crate::TestInstance;

impl TestInstance for StreamsGroupDescribeResponseData {
    fn test_populated(version: i16) -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            groups: vec![<DescribedGroup as TestInstance>::test_populated(version)],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        Self {
            throttle_time_ms: 0_i32,
            groups: vec![<DescribedGroup as TestInstance>::test_null_optionals(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            throttle_time_ms: 0_i32,
            groups: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            throttle_time_ms: 23456_i32,
            groups: vec![
                <DescribedGroup as TestInstance>::test_populated(version),
                <DescribedGroup as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            throttle_time_ms: i32::MIN,
            groups: vec![<DescribedGroup as TestInstance>::test_numeric_boundaries(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            throttle_time_ms: 12345_i32,
            groups: vec![<DescribedGroup as TestInstance>::test_tagged_fields(
                version,
            )],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for DescribedGroup {
    fn test_populated(version: i16) -> Self {
        Self {
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            group_id: KafkaString::from("test".to_owned()),
            group_state: KafkaString::from("test".to_owned()),
            group_epoch: 12345_i32,
            assignment_epoch: 12345_i32,
            topology: Some(Box::new(<Topology as TestInstance>::test_populated(
                version,
            ))),
            members: vec![<Member as TestInstance>::test_populated(version)],
            authorized_operations: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        drop(<Topology as TestInstance>::test_null_optionals(version));
        Self {
            error_code: 0_i16,
            error_message: None,
            group_id: KafkaString::default(),
            group_state: KafkaString::default(),
            group_epoch: 0_i32,
            assignment_epoch: 0_i32,
            topology: None,
            members: vec![<Member as TestInstance>::test_null_optionals(version)],
            authorized_operations: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            error_code: 0_i16,
            error_message: Some(KafkaString::default()),
            group_id: KafkaString::default(),
            group_state: KafkaString::default(),
            group_epoch: 0_i32,
            assignment_epoch: 0_i32,
            topology: Some(Box::new(<Topology as TestInstance>::test_null_optionals(
                version,
            ))),
            members: Vec::new(),
            authorized_operations: 0_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            error_code: 43_i16,
            error_message: Some(KafkaString::from("test-2".to_owned())),
            group_id: KafkaString::from("test-2".to_owned()),
            group_state: KafkaString::from("test-2".to_owned()),
            group_epoch: 23456_i32,
            assignment_epoch: 23456_i32,
            topology: Some(Box::new(
                <Topology as TestInstance>::test_multi_element_collections(version),
            )),
            members: vec![
                <Member as TestInstance>::test_populated(version),
                <Member as TestInstance>::test_multi_element_collections(version),
            ],
            authorized_operations: 23456_i32,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            error_code: i16::MIN,
            error_message: Some(KafkaString::from("boundary".to_owned())),
            group_id: KafkaString::from("boundary".to_owned()),
            group_state: KafkaString::from("boundary".to_owned()),
            group_epoch: i32::MIN,
            assignment_epoch: i32::MIN,
            topology: Some(Box::new(
                <Topology as TestInstance>::test_numeric_boundaries(version),
            )),
            members: vec![<Member as TestInstance>::test_numeric_boundaries(version)],
            authorized_operations: i32::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            error_code: 42_i16,
            error_message: Some(KafkaString::from("test".to_owned())),
            group_id: KafkaString::from("test".to_owned()),
            group_state: KafkaString::from("test".to_owned()),
            group_epoch: 12345_i32,
            assignment_epoch: 12345_i32,
            topology: Some(Box::new(<Topology as TestInstance>::test_tagged_fields(
                version,
            ))),
            members: vec![<Member as TestInstance>::test_tagged_fields(version)],
            authorized_operations: 12345_i32,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for Topology {
    fn test_populated(version: i16) -> Self {
        Self {
            epoch: 12345_i32,
            subtopologies: Some(vec![<Subtopology as TestInstance>::test_populated(version)]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        drop(<Subtopology as TestInstance>::test_null_optionals(version));
        Self {
            epoch: 0_i32,
            subtopologies: None,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            epoch: 0_i32,
            subtopologies: Some(Vec::new()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            epoch: 23456_i32,
            subtopologies: Some(vec![
                <Subtopology as TestInstance>::test_populated(version),
                <Subtopology as TestInstance>::test_multi_element_collections(version),
            ]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            epoch: i32::MIN,
            subtopologies: Some(vec![
                <Subtopology as TestInstance>::test_numeric_boundaries(version),
            ]),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            epoch: 12345_i32,
            subtopologies: Some(vec![<Subtopology as TestInstance>::test_tagged_fields(
                version,
            )]),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for Subtopology {
    fn test_populated(version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("test".to_owned()),
            source_topics: vec![KafkaString::from("test".to_owned())],
            repartition_sink_topics: vec![KafkaString::from("test".to_owned())],
            state_changelog_topics: vec![<TopicInfo as TestInstance>::test_populated(version)],
            repartition_source_topics: vec![<TopicInfo as TestInstance>::test_populated(version)],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        Self {
            subtopology_id: KafkaString::default(),
            source_topics: vec![KafkaString::default()],
            repartition_sink_topics: vec![KafkaString::default()],
            state_changelog_topics: vec![<TopicInfo as TestInstance>::test_null_optionals(version)],
            repartition_source_topics: vec![<TopicInfo as TestInstance>::test_null_optionals(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::default(),
            source_topics: Vec::new(),
            repartition_sink_topics: Vec::new(),
            state_changelog_topics: Vec::new(),
            repartition_source_topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("test-2".to_owned()),
            source_topics: vec![
                KafkaString::from("test".to_owned()),
                KafkaString::from("test-2".to_owned()),
            ],
            repartition_sink_topics: vec![
                KafkaString::from("test".to_owned()),
                KafkaString::from("test-2".to_owned()),
            ],
            state_changelog_topics: vec![
                <TopicInfo as TestInstance>::test_populated(version),
                <TopicInfo as TestInstance>::test_multi_element_collections(version),
            ],
            repartition_source_topics: vec![
                <TopicInfo as TestInstance>::test_populated(version),
                <TopicInfo as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("boundary".to_owned()),
            source_topics: vec![KafkaString::from("boundary".to_owned())],
            repartition_sink_topics: vec![KafkaString::from("boundary".to_owned())],
            state_changelog_topics: vec![<TopicInfo as TestInstance>::test_numeric_boundaries(
                version,
            )],
            repartition_source_topics: vec![<TopicInfo as TestInstance>::test_numeric_boundaries(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("test".to_owned()),
            source_topics: vec![KafkaString::from("test".to_owned())],
            repartition_sink_topics: vec![KafkaString::from("test".to_owned())],
            state_changelog_topics: vec![<TopicInfo as TestInstance>::test_tagged_fields(version)],
            repartition_source_topics: vec![<TopicInfo as TestInstance>::test_tagged_fields(
                version,
            )],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for Member {
    fn test_populated(version: i16) -> Self {
        Self {
            member_id: KafkaString::from("test".to_owned()),
            member_epoch: 12345_i32,
            instance_id: Some(KafkaString::from("test".to_owned())),
            rack_id: Some(KafkaString::from("test".to_owned())),
            client_id: KafkaString::from("test".to_owned()),
            client_host: KafkaString::from("test".to_owned()),
            topology_epoch: 12345_i32,
            process_id: KafkaString::from("test".to_owned()),
            user_endpoint: Some(Box::new(<Endpoint as TestInstance>::test_populated(
                version,
            ))),
            client_tags: vec![<KeyValue as TestInstance>::test_populated(version)],
            task_offsets: vec![<TaskOffset as TestInstance>::test_populated(version)],
            task_end_offsets: vec![<TaskOffset as TestInstance>::test_populated(version)],
            assignment: <Assignment as TestInstance>::test_populated(version),
            target_assignment: <Assignment as TestInstance>::test_populated(version),
            is_classic: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        drop(<Endpoint as TestInstance>::test_null_optionals(version));
        Self {
            member_id: KafkaString::default(),
            member_epoch: 0_i32,
            instance_id: None,
            rack_id: None,
            client_id: KafkaString::default(),
            client_host: KafkaString::default(),
            topology_epoch: 0_i32,
            process_id: KafkaString::default(),
            user_endpoint: None,
            client_tags: vec![<KeyValue as TestInstance>::test_null_optionals(version)],
            task_offsets: vec![<TaskOffset as TestInstance>::test_null_optionals(version)],
            task_end_offsets: vec![<TaskOffset as TestInstance>::test_null_optionals(version)],
            assignment: <Assignment as TestInstance>::test_null_optionals(version),
            target_assignment: <Assignment as TestInstance>::test_null_optionals(version),
            is_classic: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            member_id: KafkaString::default(),
            member_epoch: 0_i32,
            instance_id: Some(KafkaString::default()),
            rack_id: Some(KafkaString::default()),
            client_id: KafkaString::default(),
            client_host: KafkaString::default(),
            topology_epoch: 0_i32,
            process_id: KafkaString::default(),
            user_endpoint: Some(Box::new(<Endpoint as TestInstance>::test_null_optionals(
                version,
            ))),
            client_tags: Vec::new(),
            task_offsets: Vec::new(),
            task_end_offsets: Vec::new(),
            assignment: <Assignment as TestInstance>::test_null_optionals(version),
            target_assignment: <Assignment as TestInstance>::test_null_optionals(version),
            is_classic: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            member_id: KafkaString::from("test-2".to_owned()),
            member_epoch: 23456_i32,
            instance_id: Some(KafkaString::from("test-2".to_owned())),
            rack_id: Some(KafkaString::from("test-2".to_owned())),
            client_id: KafkaString::from("test-2".to_owned()),
            client_host: KafkaString::from("test-2".to_owned()),
            topology_epoch: 23456_i32,
            process_id: KafkaString::from("test-2".to_owned()),
            user_endpoint: Some(Box::new(
                <Endpoint as TestInstance>::test_multi_element_collections(version),
            )),
            client_tags: vec![
                <KeyValue as TestInstance>::test_populated(version),
                <KeyValue as TestInstance>::test_multi_element_collections(version),
            ],
            task_offsets: vec![
                <TaskOffset as TestInstance>::test_populated(version),
                <TaskOffset as TestInstance>::test_multi_element_collections(version),
            ],
            task_end_offsets: vec![
                <TaskOffset as TestInstance>::test_populated(version),
                <TaskOffset as TestInstance>::test_multi_element_collections(version),
            ],
            assignment: <Assignment as TestInstance>::test_multi_element_collections(version),
            target_assignment: <Assignment as TestInstance>::test_multi_element_collections(
                version,
            ),
            is_classic: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            member_id: KafkaString::from("boundary".to_owned()),
            member_epoch: i32::MIN,
            instance_id: Some(KafkaString::from("boundary".to_owned())),
            rack_id: Some(KafkaString::from("boundary".to_owned())),
            client_id: KafkaString::from("boundary".to_owned()),
            client_host: KafkaString::from("boundary".to_owned()),
            topology_epoch: i32::MIN,
            process_id: KafkaString::from("boundary".to_owned()),
            user_endpoint: Some(Box::new(
                <Endpoint as TestInstance>::test_numeric_boundaries(version),
            )),
            client_tags: vec![<KeyValue as TestInstance>::test_numeric_boundaries(version)],
            task_offsets: vec![<TaskOffset as TestInstance>::test_numeric_boundaries(
                version,
            )],
            task_end_offsets: vec![<TaskOffset as TestInstance>::test_numeric_boundaries(
                version,
            )],
            assignment: <Assignment as TestInstance>::test_numeric_boundaries(version),
            target_assignment: <Assignment as TestInstance>::test_numeric_boundaries(version),
            is_classic: true,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            member_id: KafkaString::from("test".to_owned()),
            member_epoch: 12345_i32,
            instance_id: Some(KafkaString::from("test".to_owned())),
            rack_id: Some(KafkaString::from("test".to_owned())),
            client_id: KafkaString::from("test".to_owned()),
            client_host: KafkaString::from("test".to_owned()),
            topology_epoch: 12345_i32,
            process_id: KafkaString::from("test".to_owned()),
            user_endpoint: Some(Box::new(<Endpoint as TestInstance>::test_tagged_fields(
                version,
            ))),
            client_tags: vec![<KeyValue as TestInstance>::test_tagged_fields(version)],
            task_offsets: vec![<TaskOffset as TestInstance>::test_tagged_fields(version)],
            task_end_offsets: vec![<TaskOffset as TestInstance>::test_tagged_fields(version)],
            assignment: <Assignment as TestInstance>::test_tagged_fields(version),
            target_assignment: <Assignment as TestInstance>::test_tagged_fields(version),
            is_classic: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for Endpoint {
    fn test_populated(_version: i16) -> Self {
        Self {
            host: KafkaString::from("test".to_owned()),
            port: 42_u16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            host: KafkaString::default(),
            port: 0_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            host: KafkaString::default(),
            port: 0_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            host: KafkaString::from("test-2".to_owned()),
            port: 43_u16,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            host: KafkaString::from("boundary".to_owned()),
            port: u16::MAX,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            host: KafkaString::from("test".to_owned()),
            port: 42_u16,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for TaskOffset {
    fn test_populated(_version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("test".to_owned()),
            partition: 12345_i32,
            offset: 9_876_543_210_i64,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            subtopology_id: KafkaString::default(),
            partition: 0_i32,
            offset: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::default(),
            partition: 0_i32,
            offset: 0_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("test-2".to_owned()),
            partition: 23456_i32,
            offset: 9_876_543_211_i64,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("boundary".to_owned()),
            partition: i32::MIN,
            offset: i64::MIN,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("test".to_owned()),
            partition: 12345_i32,
            offset: 9_876_543_210_i64,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for Assignment {
    fn test_populated(version: i16) -> Self {
        Self {
            active_tasks: vec![<TaskIds as TestInstance>::test_populated(version)],
            standby_tasks: vec![<TaskIds as TestInstance>::test_populated(version)],
            warmup_tasks: vec![<TaskIds as TestInstance>::test_populated(version)],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        Self {
            active_tasks: vec![<TaskIds as TestInstance>::test_null_optionals(version)],
            standby_tasks: vec![<TaskIds as TestInstance>::test_null_optionals(version)],
            warmup_tasks: vec![<TaskIds as TestInstance>::test_null_optionals(version)],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            active_tasks: Vec::new(),
            standby_tasks: Vec::new(),
            warmup_tasks: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            active_tasks: vec![
                <TaskIds as TestInstance>::test_populated(version),
                <TaskIds as TestInstance>::test_multi_element_collections(version),
            ],
            standby_tasks: vec![
                <TaskIds as TestInstance>::test_populated(version),
                <TaskIds as TestInstance>::test_multi_element_collections(version),
            ],
            warmup_tasks: vec![
                <TaskIds as TestInstance>::test_populated(version),
                <TaskIds as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            active_tasks: vec![<TaskIds as TestInstance>::test_numeric_boundaries(version)],
            standby_tasks: vec![<TaskIds as TestInstance>::test_numeric_boundaries(version)],
            warmup_tasks: vec![<TaskIds as TestInstance>::test_numeric_boundaries(version)],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            active_tasks: vec![<TaskIds as TestInstance>::test_tagged_fields(version)],
            standby_tasks: vec![<TaskIds as TestInstance>::test_tagged_fields(version)],
            warmup_tasks: vec![<TaskIds as TestInstance>::test_tagged_fields(version)],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for TaskIds {
    fn test_populated(_version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("test".to_owned()),
            partitions: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            subtopology_id: KafkaString::default(),
            partitions: vec![0_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::default(),
            partitions: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("test-2".to_owned()),
            partitions: vec![12345_i32, 23456_i32],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("boundary".to_owned()),
            partitions: vec![i32::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("test".to_owned()),
            partitions: vec![12345_i32],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for KeyValue {
    fn test_populated(_version: i16) -> Self {
        Self {
            key: KafkaString::from("test".to_owned()),
            value: KafkaString::from("test".to_owned()),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            key: KafkaString::default(),
            value: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            key: KafkaString::default(),
            value: KafkaString::default(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            key: KafkaString::from("test-2".to_owned()),
            value: KafkaString::from("test-2".to_owned()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            key: KafkaString::from("boundary".to_owned()),
            value: KafkaString::from("boundary".to_owned()),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            key: KafkaString::from("test".to_owned()),
            value: KafkaString::from("test".to_owned()),
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for TopicInfo {
    fn test_populated(version: i16) -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partitions: 12345_i32,
            replication_factor: 42_i16,
            topic_configs: vec![<KeyValue as TestInstance>::test_populated(version)],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        Self {
            name: KafkaString::default(),
            partitions: 0_i32,
            replication_factor: 0_i16,
            topic_configs: vec![<KeyValue as TestInstance>::test_null_optionals(version)],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            name: KafkaString::default(),
            partitions: 0_i32,
            replication_factor: 0_i16,
            topic_configs: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            name: KafkaString::from("test-2".to_owned()),
            partitions: 23456_i32,
            replication_factor: 43_i16,
            topic_configs: vec![
                <KeyValue as TestInstance>::test_populated(version),
                <KeyValue as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            name: KafkaString::from("boundary".to_owned()),
            partitions: i32::MIN,
            replication_factor: i16::MIN,
            topic_configs: vec![<KeyValue as TestInstance>::test_numeric_boundaries(version)],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            name: KafkaString::from("test".to_owned()),
            partitions: 12345_i32,
            replication_factor: 42_i16,
            topic_configs: vec![<KeyValue as TestInstance>::test_tagged_fields(version)],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <StreamsGroupDescribeResponseData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <StreamsGroupDescribeResponseData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <StreamsGroupDescribeResponseData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <StreamsGroupDescribeResponseData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <StreamsGroupDescribeResponseData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <StreamsGroupDescribeResponseData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <StreamsGroupDescribeResponseData as TestInstance>::test_multi_element_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <StreamsGroupDescribeResponseData as TestInstance>::test_multi_element_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message =
        <StreamsGroupDescribeResponseData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <StreamsGroupDescribeResponseData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <StreamsGroupDescribeResponseData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <StreamsGroupDescribeResponseData as TestInstance>::test_tagged_fields(version);
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = StreamsGroupDescribeResponseData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "StreamsGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.StreamsGroupDescribeResponseData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "StreamsGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.StreamsGroupDescribeResponseData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "StreamsGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.StreamsGroupDescribeResponseData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "StreamsGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.StreamsGroupDescribeResponseData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "StreamsGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.StreamsGroupDescribeResponseData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "StreamsGroupDescribeResponse",
        java_class: "org.apache.kafka.common.message.StreamsGroupDescribeResponseData",
        version: 0i16,
        fixture: "tagged_fields",
        rust_encode: encode_tagged_fields,
        rust_encoded_len: encoded_len_tagged_fields,
        rust_reencode: reencode,
    },
];
pub(crate) fn append_protocol_cases(cases: &mut Vec<crate::MatrixCase>) {
    cases.extend_from_slice(MATRIX_CASES);
}
