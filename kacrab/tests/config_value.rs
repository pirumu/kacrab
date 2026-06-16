//! Java-style config value parsing.

use kacrab::config::{
    ByteSize, ConfigList, ConfigValue, DurationMs, ParseConfigValue, ParseConfigValueError,
};

#[test]
fn parses_bool_numbers_duration_bytes_and_lists() {
    assert!(bool::parse_config_value(&ConfigValue::from("true")).expect("bool parses"));
    assert!(!bool::parse_config_value(&ConfigValue::from("FALSE")).expect("bool parses"));

    assert_eq!(
        i32::parse_config_value(&ConfigValue::from("30000")).expect("i32 parses"),
        30_000
    );
    assert_eq!(
        u64::parse_config_value(&ConfigValue::from("1048576")).expect("u64 parses"),
        1_048_576
    );

    let timeout = DurationMs::parse_config_value(&ConfigValue::from("30000"))
        .expect("duration millis parses");
    assert_eq!(timeout.as_millis(), 30_000);

    let bytes =
        ByteSize::parse_config_value(&ConfigValue::from("1048576")).expect("byte size parses");
    assert_eq!(bytes.get(), 1_048_576);

    let list =
        ConfigList::parse_config_value(&ConfigValue::from("a,b, c ,,d")).expect("list parses");
    assert_eq!(list.as_slice(), ["a", "b", "c", "d"]);
}

#[test]
fn parser_errors_name_target_type_and_original_value() {
    let error =
        bool::parse_config_value(&ConfigValue::from("yep")).expect_err("invalid bool should fail");

    assert_eq!(
        error,
        ParseConfigValueError {
            target: "bool",
            value: "yep".into(),
        }
    );
    assert!(error.to_string().contains("bool"));
    assert!(error.to_string().contains("yep"));
}
