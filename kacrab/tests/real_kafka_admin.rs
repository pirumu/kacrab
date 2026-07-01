#![cfg(feature = "admin")]
//! Real Kafka admin integration tests.
//!
//! - `real_kafka_admin_smoke` — core ops over `docker-compose.kafka.yml`.
//! - `real_kafka_admin_extended` — ACLs/quotas/SCRAM/txn/share+streams over
//!   `docker-compose.kafka-admin.yml` (authorizer + share/streams features).
//!
//! Both share one broker, so run single-threaded:
//! `cargo test --features admin --test real_kafka_admin -- --ignored --test-threads=1 --nocapture`.

#![allow(
    clippy::default_constructed_unit_structs,
    clippy::doc_markdown,
    clippy::expect_used,
    clippy::float_cmp,
    clippy::panic,
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
    AclBinding, AclBindingFilter, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
    AdminClient, AdminError, AlterConfigOp, AlterConfigsOptions, ClientQuotaAlteration,
    ClientQuotaEntity, ClientQuotaFilterComponent, ClientQuotaMatch, ClientQuotaOp, ConfigResource,
    CreatePartitionsOptions, CreateTopicsOptions, DescribeConsumerGroupsOptions,
    DescribeTopicsOptions, ElectionType, FeatureUpdate, FeatureUpdateUpgradeType,
    ListConsumerGroupOffsetsOptions, ListConsumerGroupsOptions, ListTopicsOptions,
    ListTransactionsOptions, MemberToRemove, NewPartitions, NewTopic, OffsetAndMetadata,
    OffsetSpec, ResourceType, ScramCredentialDeletion, ScramCredentialUpsertion, ScramMechanism,
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
    if let Some(group) = group_descs.first() {
        println!(
            "  describe_consumer_groups: type={:?} state={:?} epoch={:?}",
            group.group_type, group.state, group.group_epoch
        );
    }
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

/// Extended admin coverage against the authorizer/share/streams-enabled broker
/// from `docker-compose.kafka-admin.yml`. Run with
/// `cargo test --features admin --test real_kafka_admin real_kafka_admin_extended
/// -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "requires local Kafka from docker-compose.kafka-admin.yml (authorizer + share/streams)"]
async fn real_kafka_admin_extended() {
    let bootstrap = bootstrap_addr();
    let topic = unique_topic();
    let suffix = topic.rsplit('-').next().unwrap_or("x").to_owned();
    println!("real Kafka admin extended: bootstrap={bootstrap}, topic={topic}");

    let admin = AdminClient::from_map([("bootstrap.servers", bootstrap)])
        .await
        .expect("admin client should connect");
    admin
        .create_topics(
            vec![NewTopic::new(&topic, 1, 1)],
            CreateTopicsOptions::default(),
        )
        .await
        .expect("create_topics");

    // --- ACLs (authorizer enabled) ---
    let principal = format!("User:alice-{suffix}");
    let binding = AclBinding {
        resource_type: AclResourceType::Topic,
        resource_name: topic.clone(),
        pattern_type: AclPatternType::Literal,
        principal: principal.clone(),
        host: "*".to_owned(),
        operation: AclOperation::Read,
        permission_type: AclPermissionType::Allow,
    };
    admin
        .create_acls(vec![binding.clone()])
        .await
        .expect("create_acls");
    let acls = admin
        .describe_acls(AclBindingFilter {
            principal: Some(principal.clone()),
            ..AclBindingFilter::any()
        })
        .await
        .expect("describe_acls");
    println!("  acls: created+described {} binding(s)", acls.len());
    assert!(
        acls.iter().any(|b| b.principal == principal),
        "ACL must read back"
    );
    let deleted = admin
        .delete_acls(vec![AclBindingFilter {
            principal: Some(principal),
            ..AclBindingFilter::any()
        }])
        .await
        .expect("delete_acls");
    assert_eq!(deleted.len(), 1, "one ACL deleted");

    // --- client quotas ---
    let entity = ClientQuotaEntity::new(vec![(
        ClientQuotaEntity::USER.to_owned(),
        Some(format!("quota-user-{suffix}")),
    )]);
    admin
        .alter_client_quotas(
            vec![ClientQuotaAlteration {
                entity: entity.clone(),
                ops: vec![ClientQuotaOp {
                    key: "producer_byte_rate".to_owned(),
                    value: Some(1_048_576.0),
                }],
            }],
            false,
        )
        .await
        .expect("alter_client_quotas");
    // KRaft applies the controller's quota record to the broker asynchronously,
    // so poll the describe until it lands.
    let mut quota_seen = false;
    for _ in 0..25 {
        let quotas = admin
            .describe_client_quotas(
                vec![ClientQuotaFilterComponent {
                    entity_type: ClientQuotaEntity::USER.to_owned(),
                    match_type: ClientQuotaMatch::Exact(format!("quota-user-{suffix}")),
                }],
                false,
            )
            .await
            .expect("describe_client_quotas");
        if quotas.iter().any(|e| {
            e.quotas
                .iter()
                .any(|(k, v)| k == "producer_byte_rate" && *v == 1_048_576.0)
        }) {
            quota_seen = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    println!("  quotas: read back = {quota_seen}");
    assert!(quota_seen, "quota must read back");
    admin
        .alter_client_quotas(
            vec![ClientQuotaAlteration {
                entity,
                ops: vec![ClientQuotaOp {
                    key: "producer_byte_rate".to_owned(),
                    value: None,
                }],
            }],
            false,
        )
        .await
        .expect("alter_client_quotas remove");

    // --- user SCRAM credentials ---
    let user = format!("scram-user-{suffix}");
    admin
        .alter_user_scram_credentials(
            Vec::new(),
            vec![ScramCredentialUpsertion {
                user: user.clone(),
                mechanism: ScramMechanism::ScramSha256,
                iterations: 8192,
                salt: vec![1_u8; 16],
                salted_password: vec![2_u8; 32],
            }],
        )
        .await
        .expect("alter_user_scram_credentials upsert");
    let mut scram_seen = false;
    for _ in 0..25 {
        let creds = admin
            .describe_user_scram_credentials(vec![user.clone()])
            .await
            .expect("describe_user_scram_credentials");
        if creds.iter().any(|c| {
            c.user == user
                && c.credentials
                    .iter()
                    .any(|i| i.mechanism == ScramMechanism::ScramSha256)
        }) {
            scram_seen = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    println!("  scram: read back = {scram_seen}");
    assert!(scram_seen, "SCRAM credential must read back");
    admin
        .alter_user_scram_credentials(
            vec![ScramCredentialDeletion {
                user,
                mechanism: ScramMechanism::ScramSha256,
            }],
            Vec::new(),
        )
        .await
        .expect("alter_user_scram_credentials delete");

    // --- log dirs (per-replica) + reassignments ---
    let replica_dirs = admin
        .describe_replica_log_dirs(vec![(TopicPartition::new(&topic, 0), 1)])
        .await
        .expect("describe_replica_log_dirs");
    println!(
        "  describe_replica_log_dirs: current={:?}",
        replica_dirs.first().and_then(|r| r.current_log_dir.clone())
    );
    let reassignments = admin
        .list_partition_reassignments(Vec::new())
        .await
        .expect("list_partition_reassignments");
    assert!(reassignments.is_empty(), "no ongoing reassignments");

    // --- transactions: fence / describe / terminate ---
    // The transaction coordinator (__transaction_state) loads lazily on first
    // use, returning the retriable COORDINATOR_NOT_AVAILABLE until ready; the
    // admin client does not retry transient coordinator errors, so poll here.
    let txn_id = format!("kacrab-admin-txn-{suffix}");
    let mut fenced = None;
    for _ in 0..30 {
        match admin.fence_producers(vec![txn_id.clone()]).await {
            Ok(result) => {
                fenced = result.into_iter().next();
                break;
            },
            Err(AdminError::Broker { error, .. }) if error.is_retriable() => {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            },
            Err(other) => panic!("fence_producers: {other:?}"),
        }
    }
    let fenced = fenced.expect("fence_producers should eventually reach the coordinator");
    println!("  fence_producers: producer_id={}", fenced.producer_id);
    let txn_descs = admin
        .describe_transactions(vec![txn_id.clone()])
        .await
        .expect("describe_transactions");
    println!(
        "  describe_transactions: state={:?}",
        txn_descs.first().map(|t| &t.state)
    );
    let terminated = admin
        .force_terminate_transaction(&txn_id)
        .await
        .expect("force_terminate_transaction");
    println!(
        "  force_terminate_transaction: producer_id={} — OK",
        terminated.producer_id
    );

    // --- wire round-trip checks for ops needing cluster state we cannot set up
    //     here (a Broker error code still proves correct encode/decode; only a
    //     Wire/Protocol error would indicate an encoding bug) ---
    assert_wire_ok(
        "describe_share_groups",
        admin
            .describe_share_groups(vec![format!("share-{suffix}")])
            .await,
    );
    assert_wire_ok(
        "list_share_group_offsets",
        admin
            .list_share_group_offsets(&format!("share-{suffix}"), Vec::new())
            .await,
    );
    assert_wire_ok(
        "describe_streams_groups",
        admin
            .describe_streams_groups(vec![format!("streams-{suffix}")])
            .await,
    );
    assert_wire_ok(
        "delete_share_groups",
        admin
            .delete_share_groups(vec![format!("share-{suffix}")])
            .await,
    );
    assert_wire_ok(
        "delete_streams_groups",
        admin
            .delete_streams_groups(vec![format!("streams-{suffix}")])
            .await,
    );
    assert_wire_ok(
        "remove_members_from_consumer_group",
        admin
            .remove_members_from_consumer_group(
                &format!("grp-{suffix}"),
                vec![MemberToRemove::static_member("nope")],
            )
            .await,
    );
    assert_wire_ok(
        "update_features",
        admin
            .update_features(
                vec![FeatureUpdate::new(
                    "transaction.version",
                    2,
                    FeatureUpdateUpgradeType::Upgrade,
                )],
                true,
            )
            .await,
    );
    assert_wire_ok("unregister_broker", admin.unregister_broker(999).await);
    assert_wire_ok(
        "create_delegation_token",
        admin
            .create_delegation_token(kacrab::admin::CreateDelegationTokenOptions::default())
            .await,
    );
    assert_wire_ok("client_instance_id", admin.client_instance_id().await);
    println!("  metrics: {:?}", admin.metrics());

    admin
        .delete_topics(vec![topic])
        .await
        .expect("delete_topics");
    println!("real Kafka admin extended: ALL OK");
}

/// Assert an admin op's request/response was understood at the wire layer: a
/// success or a broker error code both prove correct encode/decode, while a
/// `Wire`/protocol error means an encoding bug.
fn assert_wire_ok<T: std::fmt::Debug>(label: &str, result: Result<T, AdminError>) {
    match result {
        Ok(value) => println!("  {label}: Ok ({value:?})"),
        Err(AdminError::Broker { error, .. }) => {
            println!("  {label}: broker error {error:?} (wire round-trip OK)");
        },
        Err(AdminError::CoordinatorUnavailable { .. } | AdminError::MissingResult { .. }) => {
            println!("  {label}: no-state response (wire round-trip OK)");
        },
        // The broker advertised no supported version for this (optional) API —
        // that is the version negotiation working, not an encoding bug.
        Err(AdminError::Wire(kacrab::wire::WireError::UnsupportedApiVersion(api))) => {
            println!("  {label}: broker does not support {api:?} (negotiated out)");
        },
        Err(other) => panic!("{label}: wire/encoding failure: {other:?}"),
    }
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
