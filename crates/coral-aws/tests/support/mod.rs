//! A `wiremock` STS and the environment that points `aws-config` at it.

#![allow(dead_code, reason = "each test file uses part of this module")]

use std::ffi::OsString;
use std::time::{Duration, SystemTime};

use coral_aws::Environment;
use wiremock::matchers::{body_string_contains, header_regex, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub(crate) const ACCOUNT: &str = "111122223333";
pub(crate) const OTHER_ACCOUNT: &str = "444455556666";

/// An STS that answers `GetCallerIdentity` for the credentials whose access
/// key is `access_key` with `account`.
pub(crate) async fn caller_identity(sts: &MockServer, access_key: &str, account: &str) {
    Mock::given(method("POST"))
        .and(body_string_contains("Action=GetCallerIdentity"))
        .and(header_regex(
            "authorization",
            &format!("Credential={access_key}/"),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "<GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
             <GetCallerIdentityResult><Arn>arn:aws:iam::{account}:user/coral</Arn>\
             <UserId>AIDACORAL</UserId><Account>{account}</Account></GetCallerIdentityResult>\
             <ResponseMetadata><RequestId>1</RequestId></ResponseMetadata>\
             </GetCallerIdentityResponse>"
        )))
        .mount(sts)
        .await;
}

/// The `GetCallerIdentity` calls `sts` received.
pub(crate) async fn identity_checks(sts: &MockServer) -> usize {
    sts.received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|request| {
            String::from_utf8_lossy(&request.body).contains("Action=GetCallerIdentity")
        })
        .count()
}

/// An environment with `vars` that sends STS calls to `sts` and reads no
/// AWS file or instance metadata of the machine running the test.
pub(crate) fn environment(sts: &MockServer, vars: &[(&str, &str)]) -> Environment {
    let base = [
        ("AWS_ENDPOINT_URL_STS", sts.uri()),
        ("AWS_CONFIG_FILE", "/nonexistent/aws/config".to_owned()),
        (
            "AWS_SHARED_CREDENTIALS_FILE",
            "/nonexistent/aws/credentials".to_owned(),
        ),
        ("AWS_EC2_METADATA_DISABLED", "true".to_owned()),
    ];
    Environment::snapshot(
        base.into_iter()
            .chain(
                vars.iter()
                    .map(|(name, value)| (*name, (*value).to_owned())),
            )
            .map(|(name, value)| (OsString::from(name), OsString::from(value))),
    )
}

/// 2015-08-30T12:36:00Z, the time of AWS's `SigV4` test vectors.
pub(crate) fn vector_time() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_mins(24_015_636)
}

pub(crate) fn request(method: &str, uri: &str) -> http::Request<Vec<u8>> {
    http::Request::builder()
        .method(method)
        .uri(uri)
        .body(Vec::new())
        .expect("a valid request")
}
