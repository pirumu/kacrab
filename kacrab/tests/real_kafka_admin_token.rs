#![cfg(feature = "admin")]
//! Real Kafka admin delegation-token integration test.
//!
//! Delegation tokens require a SASL-authenticated (non-token) principal and a
//! broker with `delegation.token.secret.key` set — both provided by
//! `docker-compose.auth.yml` (SASL_PLAINTEXT, PLAIN admin/admin-secret). Run:
//! `cargo test --features admin --test real_kafka_admin_token -- --ignored --nocapture`.

#![allow(
    clippy::doc_markdown,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::unwrap_used,
    reason = "Ignored real-broker test is an explicit smoke check with direct failure output."
)]

use std::env;

use kacrab::admin::{AdminClient, CreateDelegationTokenOptions};

#[tokio::test]
#[ignore = "requires the SASL broker from docker-compose.auth.yml"]
async fn real_kafka_admin_delegation_tokens() {
    let bootstrap =
        env::var("KACRAB_SASL_BOOTSTRAP").unwrap_or_else(|_error| "127.0.0.1:19092".to_owned());
    println!("real Kafka admin delegation tokens: bootstrap={bootstrap}");

    let admin = AdminClient::from_map([
        ("bootstrap.servers", bootstrap),
        ("security.protocol", "SASL_PLAINTEXT".to_owned()),
        ("sasl.mechanism", "PLAIN".to_owned()),
        (
            "sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" \
             password=\"admin-secret\";"
                .to_owned(),
        ),
    ])
    .await
    .expect("admin client should connect over SASL/PLAIN");

    // create
    let token = admin
        .create_delegation_token(CreateDelegationTokenOptions::default())
        .await
        .expect("create_delegation_token");
    println!(
        "  create_delegation_token: id={} owner={}:{} expiry={}",
        token.token_id,
        token.owner_principal_type,
        token.owner_principal_name,
        token.expiry_timestamp_ms
    );
    assert!(!token.token_id.is_empty(), "token id must be set");
    assert!(!token.hmac.is_empty(), "token hmac must be set");

    // describe — the token record propagates to the broker asynchronously, poll.
    let mut described_seen = false;
    for _ in 0..25 {
        let described = admin
            .describe_delegation_token(Vec::new())
            .await
            .expect("describe_delegation_token");
        if described.iter().any(|t| t.token_id == token.token_id) {
            described_seen = true;
            println!("  describe_delegation_token: {} token(s)", described.len());
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(described_seen, "created token must be described");

    // renew
    let new_expiry = admin
        .renew_delegation_token(token.hmac.clone(), 3_600_000)
        .await
        .expect("renew_delegation_token");
    println!("  renew_delegation_token: new expiry={new_expiry}");

    // expire (immediately, by passing a non-positive period)
    let expired_at = admin
        .expire_delegation_token(token.hmac, 0)
        .await
        .expect("expire_delegation_token");
    println!("  expire_delegation_token: expiry={expired_at}");

    println!("real Kafka admin delegation tokens: ALL OK");
}
