//! Admin example covering the common public API paths.
//!
//! Run against a local Kafka broker (e.g. `docker-compose.kafka.yml`):
//!
//! ```text
//! cargo run -p kacrab-examples --example admin
//! ```
//!
//! Optional positional arguments (bootstrap, topic, partitions):
//!
//! ```text
//! cargo run -p kacrab-examples --example admin -- 127.0.0.1:9092 kacrab-admin-example 3
//! ```

use std::{env, error::Error};

use kacrab::admin::{
    AdminClient, AlterConfigOp, AlterConfigsOptions, ConfigResource, CreatePartitionsOptions,
    CreateTopicsOptions, DescribeTopicsOptions, ListConsumerGroupsOptions, ListTopicsOptions,
    NewPartitions, NewTopic, OffsetSpec, TopicPartition,
};

const CLIENT_ID: &str = "kacrab-admin-example";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = ExampleArgs::parse(env::args().skip(1));
    println!(
        "admin example: bootstrap={} topic={} partitions={}",
        args.bootstrap, args.topic, args.partitions
    );

    let admin = AdminClient::from_map([
        ("bootstrap.servers", args.bootstrap.as_str()),
        ("client.id", CLIENT_ID),
    ])
    .await?;

    // --- cluster metadata ---
    let cluster = admin.describe_cluster().await?;
    println!(
        "describe_cluster: id={:?} controller={:?} nodes={}",
        cluster.cluster_id,
        cluster.controller.as_ref().map(|node| node.id),
        cluster.nodes.len()
    );

    // --- create the topic (with an initial retention config) ---
    admin
        .create_topics(
            vec![
                NewTopic::new(&args.topic, args.partitions, 1)
                    .config("retention.ms", Some("3600000".to_owned())),
            ],
            CreateTopicsOptions::default(),
        )
        .await?;
    println!(
        "create_topics: {} ({} partitions)",
        args.topic, args.partitions
    );

    // --- list & describe ---
    let listed = admin.list_topics(ListTopicsOptions::default()).await?;
    println!("list_topics: {} topic(s)", listed.len());

    let described = admin
        .describe_topics(vec![args.topic.clone()], DescribeTopicsOptions)
        .await?;
    if let Some(topic) = described.first() {
        println!(
            "describe_topics: {} has {} partitions, leader(p0)={:?}",
            topic.name,
            topic.partitions.len(),
            topic
                .partitions
                .first()
                .and_then(|p| p.leader.as_ref())
                .map(|node| node.id)
        );
    }

    // --- configs: describe then incrementally alter ---
    let configs = admin
        .describe_configs(vec![ConfigResource::topic(&args.topic)])
        .await?;
    let retention = configs
        .first()
        .and_then(|resource| {
            resource
                .entries
                .iter()
                .find(|entry| entry.name == "retention.ms")
        })
        .and_then(|entry| entry.value.clone());
    println!("describe_configs: retention.ms={retention:?}");

    admin
        .incremental_alter_configs(
            vec![(
                ConfigResource::topic(&args.topic),
                vec![AlterConfigOp::set("retention.ms", "7200000")],
            )],
            AlterConfigsOptions::default(),
        )
        .await?;
    println!("incremental_alter_configs: retention.ms=7200000");

    // --- add a partition ---
    let grown = args.partitions.saturating_add(1);
    admin
        .create_partitions(
            vec![NewPartitions::increase_to(&args.topic, grown)],
            CreatePartitionsOptions::default(),
        )
        .await?;
    println!("create_partitions: -> {grown}");

    // --- offsets (leader-routed): earliest per partition ---
    let specs: Vec<(TopicPartition, OffsetSpec)> = (0..args.partitions)
        .map(|partition| {
            (
                TopicPartition::new(&args.topic, partition),
                OffsetSpec::Earliest,
            )
        })
        .collect();
    let offsets = admin.list_offsets(specs).await?;
    for result in &offsets {
        println!(
            "list_offsets: {}-{} earliest={}",
            result.partition.topic, result.partition.partition, result.offset
        );
    }

    // --- groups ---
    let groups = admin
        .list_consumer_groups(ListConsumerGroupsOptions::default())
        .await?;
    println!("list_consumer_groups: {} group(s)", groups.len());

    // --- clean up ---
    admin.delete_topics(vec![args.topic.clone()]).await?;
    println!("delete_topics: {} — done", args.topic);

    Ok(())
}

struct ExampleArgs {
    bootstrap: String,
    topic: String,
    partitions: i32,
}

impl ExampleArgs {
    fn parse(args: impl IntoIterator<Item = String>) -> Self {
        let mut args = args.into_iter();
        let bootstrap = args.next().unwrap_or_else(|| "127.0.0.1:9092".to_owned());
        let topic = args
            .next()
            .unwrap_or_else(|| "kacrab-admin-example".to_owned());
        let partitions = args
            .next()
            .and_then(|value| value.parse().ok())
            .unwrap_or(3);
        Self {
            bootstrap,
            topic,
            partitions,
        }
    }
}
