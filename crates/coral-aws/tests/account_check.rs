//! Coral checks which account its credentials are in after each refresh.

mod support;

use std::path::Path;
use std::time::{Duration, SystemTime};

use coral_aws::{Error, Identity, Settings};
use support::{ACCOUNT, OTHER_ACCOUNT, caller_identity, environment, identity_checks, request};
use wiremock::matchers::{body_string_contains, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// When every credential in these tests expires.
const EXPIRY: &str = "2099-01-01T00:00:00Z";

fn expiry() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_hours(1_130_808)
}

fn before_expiry(by: Duration) -> SystemTime {
    expiry().checked_sub(by).expect("after the epoch")
}

const HOUR: Duration = Duration::from_hours(1);
const MINUTE: Duration = Duration::from_mins(1);

/// A profile `coral` whose `credential_process` prints the file `dir/creds`.
struct Profile {
    dir: tempfile::TempDir,
}

impl Profile {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("a temporary directory");
        std::fs::write(
            dir.path().join("config"),
            format!(
                "[profile coral]\ncredential_process = cat {}\n",
                dir.path().join("creds").display()
            ),
        )
        .expect("a written config file");
        Self { dir }
    }

    fn config(&self) -> String {
        self.dir.path().join("config").display().to_string()
    }

    fn creds(&self) -> &Path {
        self.dir.path()
    }

    /// The process now gives credentials with access key `key`.
    fn issue(&self, key: &str) {
        std::fs::write(
            self.creds().join("creds"),
            format!(
                r#"{{"Version":1,"AccessKeyId":"{key}","SecretAccessKey":"secret","SessionToken":"token","Expiration":"{EXPIRY}"}}"#
            ),
        )
        .expect("a written file");
    }

    /// The process now fails.
    fn revoke(&self) {
        std::fs::remove_file(self.creds().join("creds")).expect("a removed file");
    }

    fn identity(&self, sts: &MockServer) -> Identity {
        Identity::new(
            Settings::new(ACCOUNT, "eu-west-2", Some("coral"), None).expect("valid settings"),
            "prod-aws",
            &environment(sts, &[("AWS_CONFIG_FILE", &self.config())]),
        )
        .expect("a valid name")
    }
}

async fn sign(identity: &Identity, now: SystemTime) -> Result<http::Request<Vec<u8>>, Error> {
    let mut request = request("POST", "https://secretsmanager.eu-west-2.amazonaws.com/");
    identity
        .sign(&mut request, "secretsmanager", "eu-west-2", now)
        .await?;
    Ok(request)
}

fn access_key(request: &http::Request<Vec<u8>>) -> &str {
    let authorization = request.headers()["authorization"]
        .to_str()
        .expect("an ASCII header");
    let credential = authorization
        .split("Credential=")
        .nth(1)
        .expect("a credential scope");
    credential.split('/').next().unwrap_or_default()
}

#[tokio::test]
async fn credentials_in_the_expected_account_sign_and_are_checked_once() {
    let sts = MockServer::start().await;
    caller_identity(&sts, "AKIDRIGHT", ACCOUNT).await;
    let profile = Profile::new();
    profile.issue("AKIDRIGHT");
    let identity = profile.identity(&sts);

    for now in [before_expiry(2 * HOUR), before_expiry(HOUR)] {
        let signed = sign(&identity, now).await.unwrap();
        assert_eq!(access_key(&signed), "AKIDRIGHT");
    }
    assert_eq!(identity_checks(&sts).await, 1);
}

#[tokio::test]
async fn credentials_in_another_account_fail_every_use_until_the_next_refresh() {
    let sts = MockServer::start().await;
    caller_identity(&sts, "AKIDWRONG", OTHER_ACCOUNT).await;
    caller_identity(&sts, "AKIDRIGHT", ACCOUNT).await;
    let profile = Profile::new();
    profile.issue("AKIDWRONG");
    let identity = profile.identity(&sts);

    for now in [before_expiry(2 * HOUR), before_expiry(HOUR)] {
        let error = sign(&identity, now).await.unwrap_err();
        assert_eq!(error, Error::WrongAccount);
        assert!(error.is_permission_denied());
    }
    assert_eq!(identity_checks(&sts).await, 1);

    // Near expiry, Coral refreshes and checks the new credentials.
    profile.issue("AKIDRIGHT");
    let signed = sign(&identity, before_expiry(MINUTE)).await.unwrap();
    assert_eq!(access_key(&signed), "AKIDRIGHT");
    assert_eq!(identity_checks(&sts).await, 2);
}

#[tokio::test]
async fn a_failed_refresh_fails_the_call_without_falling_back() {
    let sts = MockServer::start().await;
    caller_identity(&sts, "AKIDRIGHT", ACCOUNT).await;
    let profile = Profile::new();
    profile.issue("AKIDRIGHT");
    let identity = profile.identity(&sts);
    sign(&identity, before_expiry(HOUR)).await.unwrap();

    profile.revoke();

    for now in [before_expiry(MINUTE), before_expiry(HOUR)] {
        assert_eq!(
            sign(&identity, now).await.unwrap_err(),
            Error::NoCredentials
        );
    }
}

#[tokio::test]
async fn credentials_sts_refuses_are_denied() {
    let sts = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_string_contains("Action=GetCallerIdentity"))
        .respond_with(ResponseTemplate::new(403).set_body_string(
            "<ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
             <Error><Type>Sender</Type><Code>InvalidClientTokenId</Code>\
             <Message>The security token included in the request is invalid.</Message></Error>\
             <RequestId>1</RequestId></ErrorResponse>",
        ))
        .mount(&sts)
        .await;
    let profile = Profile::new();
    profile.issue("AKIDREVOKED");

    let error = sign(&profile.identity(&sts), before_expiry(HOUR))
        .await
        .unwrap_err();

    assert_eq!(error, Error::Denied);
}

#[tokio::test]
async fn a_role_is_assumed_with_a_session_named_for_the_identity_and_then_checked() {
    let sts = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_string_contains("Action=AssumeRole"))
        .and(body_string_contains("RoleSessionName=coral-prod-aws"))
        .and(body_string_contains(
            "RoleArn=arn%3Aaws%3Aiam%3A%3A111122223333%3Arole%2Fcoral",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
             <AssumeRoleResult><AssumedRoleUser>\
             <Arn>arn:aws:sts::{ACCOUNT}:assumed-role/coral/coral-prod-aws</Arn>\
             <AssumedRoleId>AROACORAL:coral-prod-aws</AssumedRoleId></AssumedRoleUser>\
             <Credentials><AccessKeyId>AKIDROLE</AccessKeyId><SecretAccessKey>secret</SecretAccessKey>\
             <SessionToken>token</SessionToken><Expiration>{EXPIRY}</Expiration></Credentials>\
             </AssumeRoleResult><ResponseMetadata><RequestId>1</RequestId></ResponseMetadata>\
             </AssumeRoleResponse>"
        )))
        .mount(&sts)
        .await;
    caller_identity(&sts, "AKIDROLE", ACCOUNT).await;
    let identity = Identity::new(
        Settings::new(
            ACCOUNT,
            "eu-west-2",
            None,
            Some("arn:aws:iam::111122223333:role/coral"),
        )
        .unwrap(),
        "prod-aws",
        &environment(
            &sts,
            &[
                ("AWS_ACCESS_KEY_ID", "AKIDBASE"),
                ("AWS_SECRET_ACCESS_KEY", "secret"),
            ],
        ),
    )
    .unwrap();

    let signed = sign(&identity, before_expiry(HOUR)).await.unwrap();

    assert_eq!(access_key(&signed), "AKIDROLE");
    assert_eq!(identity_checks(&sts).await, 1);
}

#[tokio::test]
async fn a_refresh_that_returns_the_same_access_key_is_not_checked_again() {
    let sts = MockServer::start().await;
    caller_identity(&sts, "AKIDRIGHT", ACCOUNT).await;
    let profile = Profile::new();
    profile.issue("AKIDRIGHT");
    let identity = profile.identity(&sts);

    for now in [before_expiry(HOUR), before_expiry(MINUTE)] {
        sign(&identity, now).await.unwrap();
    }

    assert_eq!(identity_checks(&sts).await, 1);
}
