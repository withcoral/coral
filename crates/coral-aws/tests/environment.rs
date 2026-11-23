//! The environment snapshot never shows a value.

use std::ffi::OsString;

use coral_aws::Environment;

#[test]
fn debug_names_the_variables_and_never_shows_a_value() {
    let env = Environment::snapshot(
        [
            ("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI-secret"),
            ("AWS_SESSION_TOKEN", "session-token-value"),
            ("AWS_REGION", "eu-west-2"),
        ]
        .map(|(name, value)| (OsString::from(name), OsString::from(value))),
    );

    let shown = format!("{env:?}");

    for value in ["wJalrXUtnFEMI-secret", "session-token-value", "eu-west-2"] {
        assert!(!shown.contains(value), "{shown}");
    }
    assert_eq!(
        shown,
        r#"Environment(["AWS_REGION", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"])"#
    );
}
