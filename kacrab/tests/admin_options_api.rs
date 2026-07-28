//! Downstream-buildability contract for the admin options structs.
//!
//! Every `…Options` type is `#[non_exhaustive]`, so adding a field is no longer a
//! breaking change — but that also means a downstream crate cannot write a struct
//! literal or a `..Default::default()` update. The fluent setters are what keeps
//! them usable, and this is an integration test (a separate crate), so it fails
//! exactly when a downstream crate would lose the ability to configure a call.

#![allow(
    clippy::missing_assert_message,
    reason = "Table-shaped assertions read better without per-line messages."
)]

use kacrab::admin::{
    AlterClientQuotasOptions, AlterConfigsOptions, CreateDelegationTokenOptions,
    CreatePartitionsOptions, CreateTopicsOptions, DescribeClientQuotasOptions,
    DescribeConsumerGroupsOptions, DescribeTopicsOptions, ListConsumerGroupOffsetsOptions,
    ListConsumerGroupsOptions, ListTopicsOptions, ListTransactionsOptions, TopicPartition,
    UpdateFeaturesOptions,
};

#[test]
fn every_options_struct_is_buildable_from_outside_the_crate() {
    // A field-less options type still has to be nameable as a value.
    let _describe_topics = DescribeTopicsOptions::default();

    assert!(
        ListTopicsOptions::default()
            .list_internal(true)
            .list_internal
    );
    assert!(
        CreateTopicsOptions::default()
            .validate_only(true)
            .validate_only
    );
    assert!(
        CreatePartitionsOptions::default()
            .validate_only(true)
            .validate_only
    );
    assert!(
        AlterConfigsOptions::default()
            .validate_only(true)
            .validate_only
    );
    assert!(
        DescribeConsumerGroupsOptions::default()
            .include_authorized_operations(true)
            .include_authorized_operations
    );
    assert!(DescribeClientQuotasOptions::default().strict(true).strict);
    assert!(
        AlterClientQuotasOptions::default()
            .validate_only(true)
            .validate_only
    );
    assert!(
        UpdateFeaturesOptions::default()
            .validate_only(true)
            .validate_only
    );
}

#[test]
fn options_setters_chain_across_every_field() {
    let groups = ListConsumerGroupsOptions::default()
        .states_filter(vec!["Stable".to_owned()])
        .types_filter(vec!["consumer".to_owned()]);

    assert_eq!(groups.states_filter, ["Stable"]);
    assert_eq!(groups.types_filter, ["consumer"]);

    let offsets = ListConsumerGroupOffsetsOptions::default()
        .partitions(vec![TopicPartition::new("orders", 0)])
        .require_stable(true);

    assert_eq!(offsets.partitions.len(), 1);
    assert!(offsets.require_stable);

    let transactions = ListTransactionsOptions::default()
        .state_filters(vec!["Ongoing".to_owned()])
        .producer_id_filters(vec![7]);

    assert_eq!(transactions.state_filters, ["Ongoing"]);
    assert_eq!(transactions.producer_id_filters, [7]);

    let token = CreateDelegationTokenOptions::default()
        .owner(Some(("User".to_owned(), "alice".to_owned())))
        .renewers(vec![("User".to_owned(), "bob".to_owned())])
        .max_lifetime_ms(-1);

    assert_eq!(token.owner, Some(("User".to_owned(), "alice".to_owned())));
    assert_eq!(token.renewers.len(), 1);
    assert_eq!(token.max_lifetime_ms, -1);
}
