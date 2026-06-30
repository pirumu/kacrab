#![cfg(feature = "admin")]
//! Real Kafka admin integration smoke test.
//!
//! Brought up by `docker-compose.kafka.yml` (apache/kafka:4.3.0, single-broker
//! KRaft). Run with `cargo test --features admin --test real_kafka_admin -- --ignored --nocapture`.

#![allow(
    clippy::default_constructed_unit_structs,
    clippy::doc_markdown,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::too_many_lines,
    clippy::unwrap_used,
    reason = "Ignored real-broker tests are explicit smoke checks with direct failure output."
)]

use std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
};

use kacrab::admin::{
    AdminClient, AlterConfigOp, AlterConfigsOptions, ConfigResource, CreatePartitionsOptions,
    CreateTopicsOptions, DescribeConsumerGroupsOptions, DescribeTopicsOptions, ElectionType,
    ListConsumerGroupOffsetsOptions, ListConsumerGroupsOptions, ListTopicsOptions,
    ListTransactionsOptions, NewPartitions, NewTopic, OffsetAndMetadata, OffsetSpec, ResourceType,
    TopicPartition,
};

#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka.yml"]
async fn real_kafka_admin_smoke() {
    let bootstrap = bootstrap_addr();
    let topic = unique_topic();
    println!("real Kafka admin smoke: bootstrap={bootstrap}, topic={topic}");

    let admin = AdminClient::from_map([("bootstrap.servers", bootstrap.clone())])
        .await
        .expect("admin client should connect to local Kafka");

    // --- cluster / metadata reads ---
    let cluster = admin.describe_cluster().await.expect("describe_cluster");
    println!(
        "  describe_cluster: id={:?} controller={:?} nodes={}",
        cluster.cluster_id,
        cluster.controller.as_ref().map(|n| n.id),
        cluster.nodes.len()
    );
    assert!(!cluster.nodes.is_empty(), "cluster must report >=1 node");

    let features = admin.describe_features().await.expect("describe_features");
    println!(
        "  describe_features: supported={} finalized={}",
        features.supported_features.len(),
        features.finalized_features.len()
    );

    let quorum = admin
        .describe_metadata_quorum()
        .await
        .expect("describe_metadata_quorum");
    println!(
        "  describe_metadata_quorum: leader={} epoch={} hw={} voters={}",
        quorum.leader_id,
        quorum.leader_epoch,
        quorum.high_watermark,
        quorum.voters.len()
    );
    assert!(!quorum.voters.is_empty(), "KRaft quorum must have voters");

    let config_resources = admin
        .list_config_resources(vec![ResourceType::Topic])
        .await
        .expect("list_config_resources");
    println!("  list_config_resources(Topic): {}", config_resources.len());

    // --- create / describe / list topics ---
    admin
        .create_topics(
            vec![NewTopic::new(&topic, 2, 1).config("retention.ms", Some("3600000".to_owned()))],
            CreateTopicsOptions::default(),
        )
        .await
        .expect("create_topics");
    println!("  create_topics: {topic} (2 partitions)");

    let listed = admin
        .list_topics(ListTopicsOptions::default())
        .await
        .expect("list_topics");
    assert!(
        listed.iter().any(|t| t.name == topic),
        "created topic must appear in list_topics"
    );

    let described = admin
        .describe_topics(vec![topic.clone()], DescribeTopicsOptions::default())
        .await
        .expect("describe_topics");
    let desc = described.first().expect("one topic description");
    assert_eq!(desc.partitions.len(), 2, "topic must have 2 partitions");
    println!(
        "  describe_topics: {} partitions, leader(p0)={:?}",
        desc.partitions.len(),
        desc.partitions[0].leader.as_ref().map(|n| n.id)
    );

    // --- configs ---
    let configs = admin
        .describe_configs(vec![ConfigResource::topic(&topic)])
        .await
        .expect("describe_configs");
    let retention = configs
        .first()
        .and_then(|rc| rc.entries.iter().find(|e| e.name == "retention.ms"))
        .map(|e| e.value.clone());
    println!("  describe_configs: retention.ms={retention:?}");

    admin
        .incremental_alter_configs(
            vec![(
                ConfigResource::topic(&topic),
                vec![AlterConfigOp::set("retention.ms", "7200000")],
            )],
            AlterConfigsOptions::default(),
        )
        .await
        .expect("incremental_alter_configs");
    println!("  incremental_alter_configs: retention.ms=7200000");

    // --- partitions ---
    admin
        .create_partitions(
            vec![NewPartitions::increase_to(&topic, 3)],
            CreatePartitionsOptions::default(),
        )
        .await
        .expect("create_partitions");
    println!("  create_partitions: -> 3");

    // --- offsets (leader-routed) ---
    let offsets = admin
        .list_offsets(vec![
            (TopicPartition::new(&topic, 0), OffsetSpec::Earliest),
            (TopicPartition::new(&topic, 1), OffsetSpec::Latest),
        ])
        .await
        .expect("list_offsets");
    println!("  list_offsets: {} results", offsets.len());
    assert_eq!(offsets.len(), 2, "expected earliest+latest results");

    // --- log dirs ---
    let log_dirs = admin
        .describe_log_dirs(Vec::new(), Vec::new())
        .await
        .expect("describe_log_dirs");
    println!(
        "  describe_log_dirs: {} brokers, dirs(broker0)={}",
        log_dirs.len(),
        log_dirs.first().map_or(0, |b| b.log_dirs.len())
    );

    // --- elect_leaders (controller) + describe_producers (leader) ---
    admin
        .elect_leaders(
            ElectionType::Preferred,
            vec![TopicPartition::new(&topic, 0)],
        )
        .await
        .expect("elect_leaders");
    println!("  elect_leaders(preferred, p0) — OK");

    let producers = admin
        .describe_producers(vec![TopicPartition::new(&topic, 0)])
        .await
        .expect("describe_producers");
    println!(
        "  describe_producers(p0): {} active",
        producers.first().map_or(0, |p| p.active_producers.len())
    );

    // --- groups / transactions (likely empty on a fresh broker) ---
    let groups = admin
        .list_consumer_groups(ListConsumerGroupsOptions::default())
        .await
        .expect("list_consumer_groups");
    println!("  list_consumer_groups: {}", groups.len());
    let described_groups = admin
        .describe_consumer_groups(Vec::new(), DescribeConsumerGroupsOptions::default())
        .await
        .expect("describe_consumer_groups(empty)");
    assert!(described_groups.is_empty(), "no groups requested");
    let txns = admin
        .list_transactions(ListTransactionsOptions::default())
        .await
        .expect("list_transactions");
    println!("  list_transactions: {}", txns.len());

    // --- coordinator-routed group offset round-trip (FindCoordinator + Offset*
    //     + DescribeGroups + DeleteGroups) ---
    let group = format!("{topic}-grp");
    admin
        .alter_consumer_group_offsets(
            &group,
            vec![(TopicPartition::new(&topic, 0), OffsetAndMetadata::new(42))],
        )
        .await
        .expect("alter_consumer_group_offsets");
    let committed = admin
        .list_consumer_group_offsets(&group, ListConsumerGroupOffsetsOptions::default())
        .await
        .expect("list_consumer_group_offsets");
    println!("  group offsets round-trip: {} committed", committed.len());
    assert!(
        committed
            .iter()
            .any(|o| o.partition.partition == 0 && o.offset.offset == 42),
        "committed offset 42 must read back"
    );
    let group_descs = admin
        .describe_consumer_groups(
            vec![group.clone()],
            DescribeConsumerGroupsOptions::default(),
        )
        .await
        .expect("describe_consumer_groups");
    println!(
        "  describe_consumer_groups: state={:?}",
        group_descs.first().map(|g| &g.state)
    );
    admin
        .delete_consumer_group_offsets(&group, vec![TopicPartition::new(&topic, 0)])
        .await
        .expect("delete_consumer_group_offsets");
    admin
        .delete_consumer_groups(vec![group.clone()])
        .await
        .expect("delete_consumer_groups");
    println!("  group cleanup — OK");

    // --- cleanup ---
    admin
        .delete_topics(vec![topic.clone()])
        .await
        .expect("delete_topics");
    println!("  delete_topics: {topic} — OK");

    println!("real Kafka admin smoke: ALL OK");
}

fn bootstrap_addr() -> String {
    env::var("KACRAB_BOOTSTRAP").unwrap_or_else(|_error| "127.0.0.1:9092".to_owned())
}

fn unique_topic() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_millis();
    format!("kacrab-admin-smoke-{}-{millis}", std::process::id())
}
