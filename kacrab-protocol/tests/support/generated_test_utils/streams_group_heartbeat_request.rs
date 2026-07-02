#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    reason = "Generated test fixtures mirror Kafka's schema shape and trade hand-written lint \
              style for reproducible output, matching the generated protocol modules."
)]
use bytes::{Bytes, BytesMut};
use kacrab_protocol::{generated::streams_group_heartbeat_request::*, *};

use crate::TestInstance;

impl TestInstance for StreamsGroupHeartbeatRequestData {
    fn test_populated(version: i16) -> Self {
        Self {
            group_id: KafkaString::from("test".to_owned()),
            member_id: KafkaString::from("test".to_owned()),
            member_epoch: 12345_i32,
            endpoint_information_epoch: 12345_i32,
            instance_id: Some(KafkaString::from("test".to_owned())),
            rack_id: Some(KafkaString::from("test".to_owned())),
            rebalance_timeout_ms: 12345_i32,
            topology: Some(Box::new(<Topology as TestInstance>::test_populated(
                version,
            ))),
            active_tasks: Some(vec![<TaskIds as TestInstance>::test_populated(version)]),
            standby_tasks: Some(vec![<TaskIds as TestInstance>::test_populated(version)]),
            warmup_tasks: Some(vec![<TaskIds as TestInstance>::test_populated(version)]),
            process_id: Some(KafkaString::from("test".to_owned())),
            user_endpoint: Some(Box::new(<Endpoint as TestInstance>::test_populated(
                version,
            ))),
            client_tags: Some(vec![<KeyValue as TestInstance>::test_populated(version)]),
            task_offsets: Some(vec![<TaskOffset as TestInstance>::test_populated(version)]),
            task_end_offsets: Some(vec![<TaskOffset as TestInstance>::test_populated(version)]),
            shutdown_application: true,
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(<Topology as TestInstance>::test_null_optionals(version));
        drop(<TaskIds as TestInstance>::test_null_optionals(version));
        drop(<TaskIds as TestInstance>::test_null_optionals(version));
        drop(<TaskIds as TestInstance>::test_null_optionals(version));
        drop(<Endpoint as TestInstance>::test_null_optionals(version));
        drop(<KeyValue as TestInstance>::test_null_optionals(version));
        drop(<TaskOffset as TestInstance>::test_null_optionals(version));
        drop(<TaskOffset as TestInstance>::test_null_optionals(version));
        Self {
            group_id: KafkaString::default(),
            member_id: KafkaString::default(),
            member_epoch: 0_i32,
            endpoint_information_epoch: 0_i32,
            instance_id: None,
            rack_id: None,
            rebalance_timeout_ms: 0_i32,
            topology: None,
            active_tasks: None,
            standby_tasks: None,
            warmup_tasks: None,
            process_id: None,
            user_endpoint: None,
            client_tags: None,
            task_offsets: None,
            task_end_offsets: None,
            shutdown_application: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(version: i16) -> Self {
        Self {
            group_id: KafkaString::default(),
            member_id: KafkaString::default(),
            member_epoch: 0_i32,
            endpoint_information_epoch: 0_i32,
            instance_id: Some(KafkaString::default()),
            rack_id: Some(KafkaString::default()),
            rebalance_timeout_ms: 0_i32,
            topology: Some(Box::new(<Topology as TestInstance>::test_null_optionals(
                version,
            ))),
            active_tasks: Some(Vec::new()),
            standby_tasks: Some(Vec::new()),
            warmup_tasks: Some(Vec::new()),
            process_id: Some(KafkaString::default()),
            user_endpoint: Some(Box::new(<Endpoint as TestInstance>::test_null_optionals(
                version,
            ))),
            client_tags: Some(Vec::new()),
            task_offsets: Some(Vec::new()),
            task_end_offsets: Some(Vec::new()),
            shutdown_application: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            group_id: KafkaString::from("test-2".to_owned()),
            member_id: KafkaString::from("test-2".to_owned()),
            member_epoch: 23456_i32,
            endpoint_information_epoch: 23456_i32,
            instance_id: Some(KafkaString::from("test-2".to_owned())),
            rack_id: Some(KafkaString::from("test-2".to_owned())),
            rebalance_timeout_ms: 23456_i32,
            topology: Some(Box::new(
                <Topology as TestInstance>::test_multi_element_collections(version),
            )),
            active_tasks: Some(vec![
                <TaskIds as TestInstance>::test_populated(version),
                <TaskIds as TestInstance>::test_multi_element_collections(version),
            ]),
            standby_tasks: Some(vec![
                <TaskIds as TestInstance>::test_populated(version),
                <TaskIds as TestInstance>::test_multi_element_collections(version),
            ]),
            warmup_tasks: Some(vec![
                <TaskIds as TestInstance>::test_populated(version),
                <TaskIds as TestInstance>::test_multi_element_collections(version),
            ]),
            process_id: Some(KafkaString::from("test-2".to_owned())),
            user_endpoint: Some(Box::new(
                <Endpoint as TestInstance>::test_multi_element_collections(version),
            )),
            client_tags: Some(vec![
                <KeyValue as TestInstance>::test_populated(version),
                <KeyValue as TestInstance>::test_multi_element_collections(version),
            ]),
            task_offsets: Some(vec![
                <TaskOffset as TestInstance>::test_populated(version),
                <TaskOffset as TestInstance>::test_multi_element_collections(version),
            ]),
            task_end_offsets: Some(vec![
                <TaskOffset as TestInstance>::test_populated(version),
                <TaskOffset as TestInstance>::test_multi_element_collections(version),
            ]),
            shutdown_application: false,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            group_id: KafkaString::from("boundary".to_owned()),
            member_id: KafkaString::from("boundary".to_owned()),
            member_epoch: i32::MIN,
            endpoint_information_epoch: i32::MIN,
            instance_id: Some(KafkaString::from("boundary".to_owned())),
            rack_id: Some(KafkaString::from("boundary".to_owned())),
            rebalance_timeout_ms: i32::MIN,
            topology: Some(Box::new(
                <Topology as TestInstance>::test_numeric_boundaries(version),
            )),
            active_tasks: Some(vec![<TaskIds as TestInstance>::test_numeric_boundaries(
                version,
            )]),
            standby_tasks: Some(vec![<TaskIds as TestInstance>::test_numeric_boundaries(
                version,
            )]),
            warmup_tasks: Some(vec![<TaskIds as TestInstance>::test_numeric_boundaries(
                version,
            )]),
            process_id: Some(KafkaString::from("boundary".to_owned())),
            user_endpoint: Some(Box::new(
                <Endpoint as TestInstance>::test_numeric_boundaries(version),
            )),
            client_tags: Some(vec![<KeyValue as TestInstance>::test_numeric_boundaries(
                version,
            )]),
            task_offsets: Some(vec![<TaskOffset as TestInstance>::test_numeric_boundaries(
                version,
            )]),
            task_end_offsets: Some(vec![<TaskOffset as TestInstance>::test_numeric_boundaries(
                version,
            )]),
            shutdown_application: true,
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            group_id: KafkaString::from("test".to_owned()),
            member_id: KafkaString::from("test".to_owned()),
            member_epoch: 12345_i32,
            endpoint_information_epoch: 12345_i32,
            instance_id: Some(KafkaString::from("test".to_owned())),
            rack_id: Some(KafkaString::from("test".to_owned())),
            rebalance_timeout_ms: 12345_i32,
            topology: Some(Box::new(<Topology as TestInstance>::test_tagged_fields(
                version,
            ))),
            active_tasks: Some(vec![<TaskIds as TestInstance>::test_tagged_fields(version)]),
            standby_tasks: Some(vec![<TaskIds as TestInstance>::test_tagged_fields(version)]),
            warmup_tasks: Some(vec![<TaskIds as TestInstance>::test_tagged_fields(version)]),
            process_id: Some(KafkaString::from("test".to_owned())),
            user_endpoint: Some(Box::new(<Endpoint as TestInstance>::test_tagged_fields(
                version,
            ))),
            client_tags: Some(vec![<KeyValue as TestInstance>::test_tagged_fields(
                version,
            )]),
            task_offsets: Some(vec![<TaskOffset as TestInstance>::test_tagged_fields(
                version,
            )]),
            task_end_offsets: Some(vec![<TaskOffset as TestInstance>::test_tagged_fields(
                version,
            )]),
            shutdown_application: true,
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
            subtopologies: vec![<Subtopology as TestInstance>::test_populated(version)],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(version: i16) -> Self {
        drop(Self::default());
        Self {
            epoch: 0_i32,
            subtopologies: vec![<Subtopology as TestInstance>::test_null_optionals(version)],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            epoch: 0_i32,
            subtopologies: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(version: i16) -> Self {
        Self {
            epoch: 23456_i32,
            subtopologies: vec![
                <Subtopology as TestInstance>::test_populated(version),
                <Subtopology as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            epoch: i32::MIN,
            subtopologies: vec![<Subtopology as TestInstance>::test_numeric_boundaries(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            epoch: 12345_i32,
            subtopologies: vec![<Subtopology as TestInstance>::test_tagged_fields(version)],
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
            source_topic_regex: vec![KafkaString::from("test".to_owned())],
            state_changelog_topics: vec![<TopicInfo as TestInstance>::test_populated(version)],
            repartition_sink_topics: vec![KafkaString::from("test".to_owned())],
            repartition_source_topics: vec![<TopicInfo as TestInstance>::test_populated(version)],
            copartition_groups: vec![<CopartitionGroup as TestInstance>::test_populated(version)],
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
            source_topic_regex: vec![KafkaString::default()],
            state_changelog_topics: vec![<TopicInfo as TestInstance>::test_null_optionals(version)],
            repartition_sink_topics: vec![KafkaString::default()],
            repartition_source_topics: vec![<TopicInfo as TestInstance>::test_null_optionals(
                version,
            )],
            copartition_groups: vec![<CopartitionGroup as TestInstance>::test_null_optionals(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::default(),
            source_topics: Vec::new(),
            source_topic_regex: Vec::new(),
            state_changelog_topics: Vec::new(),
            repartition_sink_topics: Vec::new(),
            repartition_source_topics: Vec::new(),
            copartition_groups: Vec::new(),
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
            source_topic_regex: vec![
                KafkaString::from("test".to_owned()),
                KafkaString::from("test-2".to_owned()),
            ],
            state_changelog_topics: vec![
                <TopicInfo as TestInstance>::test_populated(version),
                <TopicInfo as TestInstance>::test_multi_element_collections(version),
            ],
            repartition_sink_topics: vec![
                KafkaString::from("test".to_owned()),
                KafkaString::from("test-2".to_owned()),
            ],
            repartition_source_topics: vec![
                <TopicInfo as TestInstance>::test_populated(version),
                <TopicInfo as TestInstance>::test_multi_element_collections(version),
            ],
            copartition_groups: vec![
                <CopartitionGroup as TestInstance>::test_populated(version),
                <CopartitionGroup as TestInstance>::test_multi_element_collections(version),
            ],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("boundary".to_owned()),
            source_topics: vec![KafkaString::from("boundary".to_owned())],
            source_topic_regex: vec![KafkaString::from("boundary".to_owned())],
            state_changelog_topics: vec![<TopicInfo as TestInstance>::test_numeric_boundaries(
                version,
            )],
            repartition_sink_topics: vec![KafkaString::from("boundary".to_owned())],
            repartition_source_topics: vec![<TopicInfo as TestInstance>::test_numeric_boundaries(
                version,
            )],
            copartition_groups: vec![<CopartitionGroup as TestInstance>::test_numeric_boundaries(
                version,
            )],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(version: i16) -> Self {
        Self {
            subtopology_id: KafkaString::from("test".to_owned()),
            source_topics: vec![KafkaString::from("test".to_owned())],
            source_topic_regex: vec![KafkaString::from("test".to_owned())],
            state_changelog_topics: vec![<TopicInfo as TestInstance>::test_tagged_fields(version)],
            repartition_sink_topics: vec![KafkaString::from("test".to_owned())],
            repartition_source_topics: vec![<TopicInfo as TestInstance>::test_tagged_fields(
                version,
            )],
            copartition_groups: vec![<CopartitionGroup as TestInstance>::test_tagged_fields(
                version,
            )],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
}
impl TestInstance for CopartitionGroup {
    fn test_populated(_version: i16) -> Self {
        Self {
            source_topics: vec![42_i16],
            source_topic_regex: vec![42_i16],
            repartition_source_topics: vec![42_i16],
            _unknown_tagged_fields: vec![RawTaggedField {
                tag: 254,
                data: Bytes::from_static(&[0xab]),
            }],
        }
    }
    fn test_null_optionals(_version: i16) -> Self {
        drop(Self::default());
        Self {
            source_topics: vec![0_i16],
            source_topic_regex: vec![0_i16],
            repartition_source_topics: vec![0_i16],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_empty_collections(_version: i16) -> Self {
        Self {
            source_topics: Vec::new(),
            source_topic_regex: Vec::new(),
            repartition_source_topics: Vec::new(),
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_multi_element_collections(_version: i16) -> Self {
        Self {
            source_topics: vec![42_i16, 43_i16],
            source_topic_regex: vec![42_i16, 43_i16],
            repartition_source_topics: vec![42_i16, 43_i16],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_numeric_boundaries(_version: i16) -> Self {
        Self {
            source_topics: vec![i16::MIN],
            source_topic_regex: vec![i16::MIN],
            repartition_source_topics: vec![i16::MIN],
            _unknown_tagged_fields: Vec::new(),
        }
    }
    fn test_tagged_fields(_version: i16) -> Self {
        Self {
            source_topics: vec![42_i16],
            source_topic_regex: vec![42_i16],
            repartition_source_topics: vec![42_i16],
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
fn encode_populated(version: i16) -> crate::MatrixResult<String> {
    let message = <StreamsGroupHeartbeatRequestData as TestInstance>::test_populated(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_populated(version: i16) -> crate::MatrixResult<usize> {
    let message = <StreamsGroupHeartbeatRequestData as TestInstance>::test_populated(version);
    Ok(message.encoded_len(version)?)
}
fn encode_null_optionals(version: i16) -> crate::MatrixResult<String> {
    let message = <StreamsGroupHeartbeatRequestData as TestInstance>::test_null_optionals(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_null_optionals(version: i16) -> crate::MatrixResult<usize> {
    let message = <StreamsGroupHeartbeatRequestData as TestInstance>::test_null_optionals(version);
    Ok(message.encoded_len(version)?)
}
fn encode_empty_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <StreamsGroupHeartbeatRequestData as TestInstance>::test_empty_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_empty_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <StreamsGroupHeartbeatRequestData as TestInstance>::test_empty_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_multi_element_collections(version: i16) -> crate::MatrixResult<String> {
    let message =
        <StreamsGroupHeartbeatRequestData as TestInstance>::test_multi_element_collections(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_multi_element_collections(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <StreamsGroupHeartbeatRequestData as TestInstance>::test_multi_element_collections(version);
    Ok(message.encoded_len(version)?)
}
fn encode_numeric_boundaries(version: i16) -> crate::MatrixResult<String> {
    let message =
        <StreamsGroupHeartbeatRequestData as TestInstance>::test_numeric_boundaries(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_numeric_boundaries(version: i16) -> crate::MatrixResult<usize> {
    let message =
        <StreamsGroupHeartbeatRequestData as TestInstance>::test_numeric_boundaries(version);
    Ok(message.encoded_len(version)?)
}
fn encode_tagged_fields(version: i16) -> crate::MatrixResult<String> {
    let message = <StreamsGroupHeartbeatRequestData as TestInstance>::test_tagged_fields(version);
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
fn encoded_len_tagged_fields(version: i16) -> crate::MatrixResult<usize> {
    let message = <StreamsGroupHeartbeatRequestData as TestInstance>::test_tagged_fields(version);
    Ok(message.encoded_len(version)?)
}
fn reencode(version: i16, hex_input: &str) -> crate::MatrixResult<String> {
    let mut input = Bytes::from(crate::decode_hex(hex_input)?);
    let message = StreamsGroupHeartbeatRequestData::read(&mut input, version)?;
    crate::ensure_input_consumed(&input)?;
    let mut out = BytesMut::new();
    message.write(&mut out, version)?;
    Ok(crate::hex(out.as_ref())?)
}
const MATRIX_CASES: &[crate::MatrixCase] = &[
    crate::MatrixCase {
        schema_name: "StreamsGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData",
        version: 0i16,
        fixture: "null_optionals",
        rust_encode: encode_null_optionals,
        rust_encoded_len: encoded_len_null_optionals,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "StreamsGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData",
        version: 0i16,
        fixture: "populated",
        rust_encode: encode_populated,
        rust_encoded_len: encoded_len_populated,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "StreamsGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData",
        version: 0i16,
        fixture: "empty_collections",
        rust_encode: encode_empty_collections,
        rust_encoded_len: encoded_len_empty_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "StreamsGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData",
        version: 0i16,
        fixture: "multi_element_collections",
        rust_encode: encode_multi_element_collections,
        rust_encoded_len: encoded_len_multi_element_collections,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "StreamsGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData",
        version: 0i16,
        fixture: "numeric_boundaries",
        rust_encode: encode_numeric_boundaries,
        rust_encoded_len: encoded_len_numeric_boundaries,
        rust_reencode: reencode,
    },
    crate::MatrixCase {
        schema_name: "StreamsGroupHeartbeatRequest",
        java_class: "org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData",
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
