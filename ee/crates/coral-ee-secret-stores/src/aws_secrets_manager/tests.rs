// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

use std::ffi::OsString;
use std::sync::Arc;

use coral_edition::test_fixtures::SwitchedEdition;
use coral_edition::{Edition, EditionCheck};
use coral_outbound::{DestinationPolicy, NetworkAccess};
use coral_secret_store::{
    BuildEnvironment, Error, KeyField, NamedVariables, ReferenceForm, Secret, SecretReference,
    SecretStore, SecretStoreFactory as _, SecretStoreName, SettingError, StoreSettings,
    StoreStatus,
};
use serde_json::json;
use wiremock::matchers::{body_json, body_string_contains, header, header_regex, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::AwsSecretsManagerKind;

const ACCOUNT: &str = "111122223333";
const ACCESS_KEY: &str = "AKIDCORALTEST";

/// AWS, played by one `wiremock` server: STS answers that the test's
/// credentials are in [`ACCOUNT`], and Secrets Manager answers as each test
/// mounts.
async fn aws() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_string_contains("Action=GetCallerIdentity"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "<GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
             <GetCallerIdentityResult><Arn>arn:aws:iam::{ACCOUNT}:user/coral</Arn>\
             <UserId>AIDACORAL</UserId><Account>{ACCOUNT}</Account></GetCallerIdentityResult>\
             <ResponseMetadata><RequestId>1</RequestId></ResponseMetadata>\
             </GetCallerIdentityResponse>"
        )))
        .mount(&server)
        .await;
    server
}

/// Secrets Manager answers `GetSecretValue` with `status` and `body`.
async fn answer(aws: &MockServer, status: u16, body: serde_json::Value) {
    Mock::given(method("POST"))
        .and(header("x-amz-target", "secretsmanager.GetSecretValue"))
        .respond_with(ResponseTemplate::new(status).set_body_json(body))
        .mount(aws)
        .await;
}

fn settings(endpoint: &str) -> StoreSettings {
    StoreSettings::new(
        toml::from_str(&format!(
            "account_id = '{ACCOUNT}'\nregion = 'eu-west-2'\nendpoint = '{endpoint}'\n"
        ))
        .unwrap(),
    )
}

/// An environment with static keys, whose STS is `aws`, that reads no AWS
/// file or instance metadata of the machine running the test.
fn environment(aws: &MockServer) -> BuildEnvironment {
    let vars = [
        ("AWS_ACCESS_KEY_ID", ACCESS_KEY.to_owned()),
        ("AWS_SECRET_ACCESS_KEY", "secret-access-key".to_owned()),
        ("AWS_ENDPOINT_URL_STS", aws.uri()),
        ("AWS_CONFIG_FILE", "/nonexistent/aws/config".to_owned()),
        (
            "AWS_SHARED_CREDENTIALS_FILE",
            "/nonexistent/aws/credentials".to_owned(),
        ),
        ("AWS_EC2_METADATA_DISABLED", "true".to_owned()),
    ];
    BuildEnvironment::new(
        DestinationPolicy::new(NetworkAccess::HttpAndHttps, NetworkAccess::Deny),
        Arc::new(|_name: &str| Ok(None)),
    )
    .with_aws(coral_aws::Environment::snapshot(vars.into_iter().map(
        |(name, value)| (OsString::from(name), OsString::from(value)),
    )))
}

fn store(aws: &MockServer, edition: Edition) -> Arc<dyn SecretStore> {
    AwsSecretsManagerKind::new(EditionCheck::new(Arc::new(edition)))
        .build(&store_name(), &settings(&aws.uri()), &environment(aws))
        .expect("a store")
}

fn store_name() -> SecretStoreName {
    SecretStoreName::parse("prod-aws").unwrap()
}

fn reference(name: &str, key: Option<&str>) -> SecretReference {
    SecretReference::new("prod-aws".into(), name.into(), key.map(str::to_owned)).unwrap()
}

/// Coral asks for the current version of the secret by name, signed for
/// Secrets Manager in the store's region.
#[tokio::test]
async fn a_read_asks_for_the_current_version_signed_for_the_region() {
    let aws = aws().await;
    Mock::given(method("POST"))
        .and(header("x-amz-target", "secretsmanager.GetSecretValue"))
        .and(header("content-type", "application/x-amz-json-1.1"))
        .and(body_json(
            json!({ "SecretId": "team/jira", "VersionStage": "AWSCURRENT" }),
        ))
        .and(header_regex(
            "authorization",
            &format!("^AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/\\d{{8}}/eu-west-2/secretsmanager/aws4_request, "),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "SecretString": "v" })))
        .mount(&aws)
        .await;
    let secret = store(&aws, Edition::Enterprise)
        .read(&reference("team/jira", None))
        .await
        .unwrap();
    assert_eq!(secret.expose(), "v");
}

/// Without a key, Coral returns the whole secret string. With one, the
/// string is a JSON object and Coral returns one of its string values.
#[tokio::test]
async fn a_read_returns_the_secret_string_or_one_json_value() {
    let json = r#"{"token":"t-1","port":5432}"#;
    let cases: [(Option<&str>, serde_json::Value, Result<&str, Error>); 8] = [
        (None, json!({ "SecretString": json }), Ok(json)),
        (
            None,
            json!({ "SecretBinary": "AAEC" }),
            Err(Error::MalformedResponse),
        ),
        (Some("token"), json!({ "SecretString": json }), Ok("t-1")),
        (
            Some("other"),
            json!({ "SecretString": json }),
            Err(Error::KeyNotFound),
        ),
        (
            Some("token"),
            json!({ "SecretString": "plain text" }),
            Err(Error::KeyNotFound),
        ),
        (
            Some("token"),
            json!({ "SecretString": r#"["token"]"# }),
            Err(Error::KeyNotFound),
        ),
        (
            Some("port"),
            json!({ "SecretString": json }),
            Err(Error::MalformedResponse),
        ),
        (
            Some("token"),
            json!({ "SecretBinary": "AAEC" }),
            Err(Error::MalformedResponse),
        ),
    ];
    for (key, body, expected) in cases {
        let aws = aws().await;
        answer(&aws, 200, body.clone()).await;
        let read = store(&aws, Edition::Enterprise)
            .read(&reference("team/db", key))
            .await;
        assert_eq!(
            read.as_ref().map(Secret::expose).map_err(|error| *error),
            expected,
            "{key:?} {body}"
        );
    }
}

/// Each AWS refusal maps to the port error that says what to fix.
#[tokio::test]
async fn each_aws_error_maps_to_what_to_fix() {
    let failure = |error_type: &str| json!({ "__type": error_type, "Message": "m" });
    let cases = [
        (
            400,
            failure("ResourceNotFoundException"),
            Error::EntryNotFound,
        ),
        (
            400,
            failure("com.amazonaws.secretsmanager#ResourceNotFoundException"),
            Error::EntryNotFound,
        ),
        (
            400,
            failure("InvalidRequestException"),
            Error::EntryNotFound,
        ),
        (
            400,
            failure("AccessDeniedException"),
            Error::PermissionDenied,
        ),
        (400, failure("DecryptionFailure"), Error::PermissionDenied),
        (
            400,
            failure("UnrecognizedClientException"),
            Error::PermissionDenied,
        ),
        (403, json!({}), Error::PermissionDenied),
        (400, failure("ThrottlingException"), Error::Unreachable),
        (429, json!({}), Error::Unreachable),
        (500, failure("InternalServiceError"), Error::Unreachable),
        (503, json!({}), Error::Unreachable),
        (400, failure("SomethingNew"), Error::MalformedResponse),
        (200, json!({ "Name": "no value" }), Error::MalformedResponse),
    ];
    for (status, body, expected) in cases {
        let aws = aws().await;
        answer(&aws, status, body.clone()).await;
        let error = store(&aws, Edition::Enterprise)
            .read(&reference("team/jira", None))
            .await
            .unwrap_err();
        assert_eq!(error, expected, "{status} {body}");
    }
}

/// AWS may name the error in a header rather than the body.
#[tokio::test]
async fn an_error_type_header_names_the_error() {
    let aws = aws().await;
    Mock::given(method("POST"))
        .and(header("x-amz-target", "secretsmanager.GetSecretValue"))
        .respond_with(
            ResponseTemplate::new(400)
                .insert_header(
                    "x-amzn-ErrorType",
                    "ResourceNotFoundException:http://internal/",
                )
                .set_body_string(""),
        )
        .mount(&aws)
        .await;
    let error = store(&aws, Edition::Enterprise)
        .read(&reference("team/jira", None))
        .await
        .unwrap_err();
    assert_eq!(error, Error::EntryNotFound);
}

/// A store reads only its own account, so it refuses an ARN, which names
/// one, and sends nothing.
#[tokio::test]
async fn an_arn_is_not_a_secret_name() {
    let aws = aws().await;
    let store = store(&aws, Edition::Enterprise);
    for name in [
        "arn:aws:secretsmanager:eu-west-2:444455556666:secret:team/jira-AbCdEf",
        "team:jira",
    ] {
        let error = store.read(&reference(name, None)).await.unwrap_err();
        assert_eq!(error, Error::EntryNotFound, "{name}");
    }
    assert!(aws.received_requests().await.unwrap().is_empty());
}

/// Credentials in another account fail as a denial, and no read is sent.
#[tokio::test]
async fn credentials_in_another_account_are_denied() {
    let aws = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_string_contains("Action=GetCallerIdentity"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "<GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
             <GetCallerIdentityResult><Arn>arn:aws:iam::444455556666:user/coral</Arn>\
             <UserId>AIDACORAL</UserId><Account>444455556666</Account></GetCallerIdentityResult>\
             <ResponseMetadata><RequestId>1</RequestId></ResponseMetadata>\
             </GetCallerIdentityResponse>",
        ))
        .mount(&aws)
        .await;
    answer(&aws, 200, json!({ "SecretString": "v" })).await;
    let error = store(&aws, Edition::Enterprise)
        .read(&reference("team/jira", None))
        .await
        .unwrap_err();
    assert_eq!(error, Error::PermissionDenied);
    assert_eq!(secrets_manager_calls(&aws).await, 0);
}

/// A destination outside the outbound policy is unreachable, and nothing
/// goes to it.
#[tokio::test]
async fn an_endpoint_outside_the_policy_is_unreachable() {
    let aws = aws().await;
    answer(&aws, 200, json!({ "SecretString": "v" })).await;
    let environment = BuildEnvironment {
        policy: DestinationPolicy::new(NetworkAccess::Deny, NetworkAccess::Deny),
        ..environment(&aws)
    };
    let store = AwsSecretsManagerKind::new(EditionCheck::new(Arc::new(Edition::Enterprise)))
        .build(&store_name(), &settings(&aws.uri()), &environment)
        .unwrap();
    let error = store.read(&reference("team/jira", None)).await.unwrap_err();
    assert_eq!(error, Error::Unreachable);
    assert_eq!(secrets_manager_calls(&aws).await, 0);
}

async fn secrets_manager_calls(aws: &MockServer) -> usize {
    aws.received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|request| request.headers.contains_key("x-amz-target"))
        .count()
}

/// ADR 0033: an Enterprise Feature refuses while the Edition is Community,
/// checks the Edition on every use, and deletes nothing.
#[tokio::test]
async fn a_store_reads_only_while_the_edition_is_enterprise() {
    let aws = aws().await;
    answer(&aws, 200, json!({ "SecretString": "v" })).await;
    let switch = Arc::new(SwitchedEdition::default());
    let store = AwsSecretsManagerKind::new(EditionCheck::new(Arc::clone(&switch) as _))
        .build(&store_name(), &settings(&aws.uri()), &environment(&aws))
        .unwrap();
    let reference = reference("team/jira", None);

    assert_eq!(store.status(), StoreStatus::NeedsEnterprise);
    assert_eq!(
        store.read(&reference).await.unwrap_err(),
        Error::NeedsEnterprise
    );
    assert!(
        aws.received_requests().await.unwrap().is_empty(),
        "a refused read sends nothing"
    );

    switch.set(Edition::Enterprise);
    assert_eq!(store.status(), StoreStatus::Available);
    assert_eq!(store.read(&reference).await.unwrap().expose(), "v");

    switch.set(Edition::Community);
    assert_eq!(
        store.read(&reference).await.unwrap_err(),
        Error::NeedsEnterprise
    );
}

#[tokio::test]
async fn a_reference_holds_a_secret_name_and_an_optional_json_key() {
    let aws = aws().await;
    for edition in [Edition::Community, Edition::Enterprise] {
        assert_eq!(
            store(&aws, edition).reference_form(),
            ReferenceForm {
                path_label: "Secret name".into(),
                path_prefix: None,
                key: Some(KeyField {
                    label: "JSON key".into(),
                    required: false,
                }),
            },
            "{edition:?}"
        );
    }
}

const PASTED: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

/// Why validation refused the settings `raw` of the store called `name`.
fn rejection(name: &str, raw: &str) -> Option<SettingError> {
    AwsSecretsManagerKind::new(EditionCheck::new(Arc::new(Edition::Enterprise)))
        .validate(
            &SecretStoreName::parse(name).unwrap(),
            &StoreSettings::new(toml::from_str(raw).unwrap()),
            &NamedVariables::default(),
        )
        .err()
}

#[test]
fn settings_are_checked_at_bootstrap_and_name_the_field() {
    let good = format!(
        "account_id = '{ACCOUNT}'\nregion = 'eu-west-2'\nprofile = 'coral'\n\
         role_arn = 'arn:aws:iam::{ACCOUNT}:role/coral-prod-secrets'\n\
         endpoint = 'https://vpce-1.secretsmanager.eu-west-2.vpce.amazonaws.com'\n"
    );
    assert_eq!(rejection("prod-aws", &good), None);
    // Only account_id and region are required.
    assert_eq!(
        rejection(
            "prod-aws",
            &format!("account_id = '{ACCOUNT}'\nregion = 'eu-west-2'\n"),
        ),
        None
    );
    let long_name = "a".repeat(59);
    for (name, raw, field) in [
        ("prod-aws", good.replace(ACCOUNT, "1234"), "account_id"),
        (
            "prod-aws",
            good.replace("'eu-west-2'", "'EU West'"),
            "region",
        ),
        ("prod-aws", good.replace("'coral'", "' '"), "profile"),
        (
            "prod-aws",
            good.replace(&format!("iam::{ACCOUNT}:role"), "iam::444455556666:role"),
            "role_arn",
        ),
        (
            "prod-aws",
            good.replace(".amazonaws.com'", ".amazonaws.com/path'"),
            "endpoint",
        ),
        ("prod-aws", good.replace("region = 'eu-west-2'\n", ""), ""),
        (
            "prod-aws",
            format!("{good}secret_key = 'x'\n"),
            "secret_key",
        ),
        (long_name.as_str(), good.clone(), "name"),
    ] {
        let error = rejection(name, &raw).expect(&raw);
        assert_eq!(error.field(), field, "{raw}: {error}");
    }
}

/// Operators paste secrets under the wrong key.
#[test]
fn a_rejected_value_is_never_repeated() {
    for raw in [
        format!("account_id = '{PASTED}'\nregion = 'eu-west-2'\n"),
        format!("account_id = '{ACCOUNT}'\nregion = '{PASTED}'\n"),
        format!("account_id = '{ACCOUNT}'\nregion = 'eu-west-2'\nrole_arn = '{PASTED}'\n"),
        format!("account_id = '{ACCOUNT}'\nregion = 'eu-west-2'\nendpoint = '{PASTED}'\n"),
    ] {
        let error = rejection("prod-aws", &raw).expect(&raw).to_string();
        assert!(!error.contains(PASTED), "{raw}: {error}");
    }
}
