//! An identity's settings are checked before boot goes on.

use coral_aws::{AwsIdentity, Environment, Settings, SettingsError};

const ACCOUNT: &str = "111122223333";

fn settings(role_arn: Option<&str>) -> Result<Settings, SettingsError> {
    Settings::new(ACCOUNT, "eu-west-2", None, role_arn)
}

#[test]
fn a_role_in_the_expected_account_is_accepted() {
    for role in [
        "arn:aws:iam::111122223333:role/coral-prod-secrets",
        "arn:aws:iam::111122223333:role/team/coral",
        "arn:aws-us-gov:iam::111122223333:role/coral",
    ] {
        settings(Some(role)).unwrap_or_else(|error| panic!("{role}: {error}"));
    }
}

#[test]
fn malformed_settings_name_the_field_and_never_the_value() {
    let cases: [(Result<Settings, SettingsError>, SettingsError, &str); 9] = [
        (
            settings(Some("arn:aws:iam::444455556666:role/coral")),
            SettingsError::RoleInAnotherAccount,
            "role_arn",
        ),
        (
            settings(Some("arn:aws:iam::111122223333:user/coral")),
            SettingsError::RoleArn,
            "role_arn",
        ),
        (
            settings(Some("arn:aws:iam::111122223333:role/")),
            SettingsError::RoleArn,
            "role_arn",
        ),
        (
            settings(Some("arn:aws:sts::111122223333:assumed-role/coral/s")),
            SettingsError::RoleArn,
            "role_arn",
        ),
        (
            Settings::new("11112222333", "eu-west-2", None, None),
            SettingsError::AccountId,
            "account_id",
        ),
        (
            Settings::new("11112222333x", "eu-west-2", None, None),
            SettingsError::AccountId,
            "account_id",
        ),
        (
            Settings::new(ACCOUNT, "", None, None),
            SettingsError::Region,
            "region",
        ),
        (
            Settings::new(ACCOUNT, "eu west 2", None, None),
            SettingsError::Region,
            "region",
        ),
        (
            Settings::new(ACCOUNT, "eu-west-2", Some(" "), None),
            SettingsError::Profile,
            "profile",
        ),
    ];
    for (result, expected, field) in cases {
        let error = result.expect_err("rejected");
        assert_eq!(error, expected);
        assert_eq!(error.field(), field);
        for value in ["444455556666", "11112222333", "eu west", "coral"] {
            assert!(!error.to_string().contains(value), "{error}");
        }
    }
}

#[test]
fn a_name_that_makes_no_role_session_name_is_rejected() {
    let settings = settings(Some("arn:aws:iam::111122223333:role/coral")).unwrap();
    for name in ["", "has space", &"n".repeat(59)] {
        let error = AwsIdentity::new(settings.clone(), name, &Environment::default())
            .expect_err("rejected");
        assert_eq!(error, SettingsError::SessionName);
    }
    AwsIdentity::new(settings, &"n".repeat(58), &Environment::default()).unwrap();
}

#[test]
fn an_https_proxy_that_is_not_a_url_is_rejected_without_repeating_it() {
    let settings = settings(None).unwrap();
    for name in ["HTTPS_PROXY", "https_proxy", "ALL_PROXY"] {
        let env = Environment::snapshot([(name.into(), "user:hunter2@not a url".into())]);
        let error = AwsIdentity::new(settings.clone(), "prod-aws", &env).expect_err("rejected");
        assert_eq!(error, SettingsError::HttpsProxy, "{name}");
        assert!(!error.to_string().contains("hunter2"), "{error}");
    }
}
