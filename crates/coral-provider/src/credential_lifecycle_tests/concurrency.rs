//! Credential reads beside a writer, and callers that meet at one refresh.
use super::{callback, client, oauth_document};
use crate::access::CredentialRoute;
use crate::account::{
    AccountModule, AccountScope, CredentialEvidence, OAuthExchangeGate, RenewalGate,
};
use crate::account_tests::{CREDENTIAL_REQUIRED_OPENAPI, authorization_request, publish_provider};
use crate::setup_tests::{credential_evidence, harness, user_principal, workspace_owner};
use crate::tests::{Harness, change_context};
use coral_credentials::{CredentialValues, Secret};
use coral_telemetry::testing::MetricCapture;
use std::sync::Arc;
use std::time::Duration;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_string_contains, method, path},
};

coral_test_support::provider_setup_fixtures!(crate);

fn token(values: &CredentialValues) -> &str {
    values.get(CredentialValues::TOKEN).unwrap().expose()
}

/// The write transactions begun so far, as `coral-db` reports them.
fn write_transactions(metrics: &MetricCapture) -> u64 {
    metrics
        .named("coral.persistence.acquisition.duration")
        .into_iter()
        .filter(|point| {
            point.attributes.get("operation").map(String::as_str) == Some("begin_write")
        })
        .map(|point| point.count)
        .sum()
}

/// A User's OAuth Account on a new Provider `oauth`. Its first access token
/// is stamped at 10 ns and lives `expires_in` seconds.
struct Connected {
    server: MockServer,
    h: Harness,
    context: coral_workspace::ChangeContext,
    workspace: coral_workspace::WorkspaceId,
    provider: crate::Provider,
    evidence: CredentialEvidence,
}

async fn connected(user: &str, expires_in: u64) -> Connected {
    let server = MockServer::start().await;
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, user).await;
    let context = change_context(actor.clone(), 10, 'a');
    let provider = publish_provider(
        &h.providers,
        &context,
        workspace.id(),
        &oauth_document(&server),
        "oauth",
        CredentialRoute::Caller,
    )
    .await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "original-canary", "token_type": "Bearer",
            "refresh_token": "refresh-canary", "expires_in": expires_in
        })))
        .expect(1)
        .mount(&server)
        .await;
    let pending = h
        .accounts
        .start_user_authorization(
            &context,
            workspace.id(),
            provider.id(),
            authorization_request(),
            Some(client()),
        )
        .await
        .unwrap();
    callback(&h.accounts, &context, workspace.id(), &pending).await;
    server.reset().await;
    let evidence = credential_evidence(
        &h.database,
        workspace.id(),
        "oauth",
        "getData",
        &user_principal(&actor),
    )
    .await
    .unwrap();
    Connected {
        server,
        context,
        workspace: workspace.id().clone(),
        provider,
        evidence,
        h,
    }
}

impl Connected {
    /// One dispatch's read at `now` through `accounts`, on a task of its own.
    fn read(
        &self,
        accounts: &AccountModule,
        now: i64,
    ) -> tokio::task::JoinHandle<Result<CredentialValues, crate::Error>> {
        let accounts = accounts.clone();
        let workspace = self.workspace.clone();
        let provider = self.provider.id().clone();
        let evidence = self.evidence.clone();
        tokio::spawn(async move {
            accounts
                .credentials(&workspace, &provider, &evidence, now)
                .await
        })
    }

    /// Mounts the one answer the service gives a refresh of the first token.
    async fn answer_one_refresh(&self, answer: ResponseTemplate) {
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .and(body_string_contains("refresh_token=refresh-canary"))
            .respond_with(answer)
            .expect(1)
            .mount(&self.server)
            .await;
    }

    async fn refreshes_sent(&self) -> usize {
        self.server.received_requests().await.unwrap().len()
    }
}

/// The service renews the token for ten minutes and rotates the refresh token,
/// so a second refresh of the first token would be a spent one.
fn renewed() -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(serde_json::json!({
        "access_token": "renewed-canary", "token_type": "Bearer",
        "refresh_token": "rotated-canary", "expires_in": 600
    }))
}

async fn settled<T>(what: &str, task: tokio::task::JoinHandle<T>) -> T {
    tokio::time::timeout(coral_test_support::EVENTUALLY, task)
        .await
        .unwrap_or_else(|_elapsed| panic!("timed out before {what}"))
        .unwrap()
}

async fn entered(what: &str, gate: &OAuthExchangeGate) {
    tokio::time::timeout(coral_test_support::EVENTUALLY, gate.entered.notified())
        .await
        .unwrap_or_else(|_elapsed| panic!("timed out before {what}"));
}

/// Waits until `gate` holds `callers` renewals, each between the read that
/// found it due and its write.
async fn held(what: &str, gate: &RenewalGate, callers: u32) {
    tokio::time::timeout(
        coral_test_support::EVENTUALLY,
        gate.entered.acquire_many(callers),
    )
    .await
    .unwrap_or_else(|_elapsed| panic!("timed out before {what}"))
    .unwrap()
    .forget();
}

/// A dispatch reads a credential that needs no renewal while another
/// transaction holds what a write would wait for: `SQLite`'s one writer, and
/// on Postgres the Account row, held shared as the Policy Enforcement Point
/// holds it. A read that opened a write transaction would wait for that
/// holder and, on `SQLite`, fail as busy.
#[tokio::test]
async fn a_current_credential_is_read_beside_a_writer_without_a_write_transaction() {
    let oauth = connected("user:read-beside-writer", 60).await;
    let (h, owner) = (&oauth.h, &oauth.workspace);
    let typed = h
        .providers
        .publish_shared_fixture(
            &oauth.context,
            owner,
            crate::OpenApiDocument::parse(CREDENTIAL_REQUIRED_OPENAPI).unwrap(),
            "typed",
            &h.accounts,
            CredentialValues::token(Secret::new("typed-canary".into()).unwrap()),
        )
        .await
        .unwrap();
    let principal = user_principal(oauth.context.actor());
    let typed_evidence = credential_evidence(
        &h.database,
        owner,
        "typed",
        "getPrivateForecast",
        &principal,
    )
    .await
    .unwrap();
    let metrics = MetricCapture::default();
    let _recording = tracing::subscriber::set_default(metrics.subscriber());
    for (row, provider, call_path, evidence, expected) in [
        (
            "an OAuth token that has not expired",
            &oauth.provider,
            "coral.providers.oauth.getData",
            &oauth.evidence,
            "original-canary",
        ),
        (
            "typed-in values",
            &typed,
            "coral.providers.typed.getPrivateForecast",
            &typed_evidence,
            "typed-canary",
        ),
    ] {
        let mut writer = h.database.begin().await.unwrap();
        let target = crate::resolve_owner_tool_in_transaction(&mut writer, owner, call_path)
            .await
            .unwrap()
            .unwrap();
        crate::account::retained_account_fact_in_transaction(
            &mut writer,
            &target,
            evidence,
            coral_db::RowLock::Shared,
        )
        .await
        .unwrap();
        let begun = write_transactions(&metrics);
        assert!(begun > 0, "{row}: the held writer is counted");
        let read = tokio::time::timeout(coral_test_support::EVENTUALLY, async {
            h.accounts
                .prepare_dispatch(owner, provider.id(), evidence, 11)
                .await?;
            h.accounts
                .credentials(owner, provider.id(), evidence, 11)
                .await
        })
        .await
        .unwrap_or_else(|_elapsed| panic!("{row}: the read waits for the writer"))
        .unwrap_or_else(|error| panic!("{row}: {error:?}"));
        assert_eq!(token(&read), expected, "{row}");
        assert_eq!(
            write_transactions(&metrics) - begun,
            0,
            "{row}: the read opens no write transaction"
        );
        writer.rollback().await.unwrap();
    }
    assert!(
        oauth.server.received_requests().await.unwrap().is_empty(),
        "a current token is not renewed"
    );
}

/// The first token expired at one second. Every caller reads it expired, with
/// no claim, before any of them writes. They then race for the refresh claim:
/// one takes it and exchanges the refresh token, which the service rotates,
/// and the others read again and wait for that exchange.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn callers_that_meet_at_an_expired_token_send_one_refresh_and_all_read_the_renewed_token() {
    const CALLERS: u32 = 8;
    let oauth = connected("user:one-refresh", 1).await;
    oauth.answer_one_refresh(renewed()).await;
    let renewal = Arc::new(RenewalGate::default());
    let gated = oauth.h.accounts.clone().with_renewal_gate(renewal.clone());
    let callers: Vec<_> = (0..CALLERS)
        .map(|_| oauth.read(&gated, 2_000_000_000))
        .collect();
    held("every caller finds the refresh due", &renewal, CALLERS).await;
    renewal.release.add_permits(CALLERS as usize);
    for (caller, read) in callers.into_iter().enumerate() {
        let values = settled("every caller has its token", read).await.unwrap();
        assert_eq!(token(&values), "renewed-canary", "caller {caller}");
    }
    assert_eq!(oauth.refreshes_sent().await, 1);
}

/// Once a refresh is claimed, no read returns the token it replaces. A caller
/// whose own clock still finds that token current waits for the exchange too.
#[tokio::test]
async fn a_read_during_a_claimed_refresh_waits_for_the_renewed_token_whatever_its_clock() {
    let oauth = connected("user:read-during-refresh", 1).await;
    oauth.answer_one_refresh(renewed()).await;
    let exchange = Arc::new(OAuthExchangeGate::default());
    let refreshing = oauth
        .h
        .accounts
        .clone()
        .with_exchange_gate(exchange.clone());
    let refresher = oauth.read(&refreshing, 2_000_000_000);
    entered("the refresh is claimed", &exchange).await;
    // At 11 ns the first token, stamped at 10 ns, has a second to live.
    let reading =
        oauth
            .h
            .accounts
            .credentials(&oauth.workspace, oauth.provider.id(), &oauth.evidence, 11);
    tokio::pin!(reading);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut reading)
            .await
            .is_err(),
        "the read waits for the claimed refresh"
    );
    assert_eq!(oauth.refreshes_sent().await, 0);
    exchange.release.notify_one();
    let read = tokio::time::timeout(coral_test_support::EVENTUALLY, reading)
        .await
        .expect("the read ends once the refresh does")
        .unwrap();
    assert_eq!(token(&read), "renewed-canary");
    let first = settled("the refresh ends", refresher).await.unwrap();
    assert_eq!(token(&first), "renewed-canary");
    assert_eq!(oauth.refreshes_sent().await, 1);
}

/// What happens to the Account between a caller's read, which finds a refresh
/// due, and that caller's claim.
#[derive(Debug, Clone, Copy)]
enum Meanwhile {
    /// Another caller refreshes the token and stores the renewed one.
    Renewed,
    /// Another caller claims the refresh and is still exchanging.
    Renewing,
    /// The service rejects another caller's refresh for good.
    Rejected,
    /// The User disconnects the Account.
    Disconnected,
}

/// A caller reads an expired token and holds no row while it goes on to claim
/// the refresh. Its claim is conditional on the Account as it read it, so once
/// the Account has moved on the caller never spends the refresh token it read:
/// it reads again, and ends as a caller that arrived later would.
#[tokio::test]
async fn a_refresh_found_due_is_not_claimed_once_the_account_has_moved_on() {
    for meanwhile in [
        Meanwhile::Renewed,
        Meanwhile::Renewing,
        Meanwhile::Rejected,
        Meanwhile::Disconnected,
    ] {
        let oauth = connected("user:stale-claim", 1).await;
        let accounts = &oauth.h.accounts;
        let now = 2_000_000_000;
        let renewal = Arc::new(RenewalGate::default());
        let gated = accounts.clone().with_renewal_gate(renewal.clone());
        let stale = oauth.read(&gated, now);
        held("the caller finds a refresh due", &renewal, 1).await;
        let (stale, renewed_token, refreshes) = match meanwhile {
            Meanwhile::Renewed => {
                oauth.answer_one_refresh(renewed()).await;
                let other = settled("another caller refreshes", oauth.read(accounts, now)).await;
                assert_eq!(token(&other.unwrap()), "renewed-canary");
                (stale, true, 1)
            }
            Meanwhile::Renewing => {
                oauth.answer_one_refresh(renewed()).await;
                let exchange = Arc::new(OAuthExchangeGate::default());
                let renewing = accounts.clone().with_exchange_gate(exchange.clone());
                let other = oauth.read(&renewing, now);
                entered("another caller claims the refresh", &exchange).await;
                renewal.release.add_permits(1);
                let mut stale = stale;
                assert!(
                    tokio::time::timeout(Duration::from_millis(100), &mut stale)
                        .await
                        .is_err(),
                    "the caller waits for the claim it lost"
                );
                assert_eq!(oauth.refreshes_sent().await, 0);
                exchange.release.notify_one();
                let other = settled("another caller refreshes", other).await;
                assert_eq!(token(&other.unwrap()), "renewed-canary");
                (stale, true, 1)
            }
            Meanwhile::Rejected => {
                oauth
                    .answer_one_refresh(
                        ResponseTemplate::new(400)
                            .set_body_json(serde_json::json!({"error": "invalid_grant"})),
                    )
                    .await;
                let other = settled("another caller is rejected", oauth.read(accounts, now)).await;
                assert!(matches!(other, Err(crate::Error::AccountRequired)));
                (stale, false, 1)
            }
            Meanwhile::Disconnected => {
                accounts
                    .disconnect(
                        &oauth.context,
                        &oauth.workspace,
                        oauth.provider.id(),
                        oauth.evidence.account_id,
                    )
                    .await
                    .unwrap();
                (stale, false, 0)
            }
        };
        let account = || {
            accounts.get_account(
                &oauth.context,
                &oauth.workspace,
                oauth.provider.id(),
                &AccountScope::User,
            )
        };
        let before = account().await.unwrap();
        if !matches!(meanwhile, Meanwhile::Renewing) {
            renewal.release.add_permits(1);
        }
        let read = settled("the caller ends", stale).await;
        if renewed_token {
            assert_eq!(token(&read.unwrap()), "renewed-canary", "{meanwhile:?}");
        } else {
            assert!(
                matches!(read, Err(crate::Error::AccountRequired)),
                "{meanwhile:?}: {read:?}"
            );
        }
        assert_eq!(
            oauth.refreshes_sent().await,
            refreshes,
            "{meanwhile:?}: the caller sends no refresh of its own"
        );
        assert_eq!(account().await.unwrap(), before, "{meanwhile:?}");
    }
}

/// A caller reads a refresh claim past its deadline, by its own clock, and
/// goes on to end that claim as a required reconnect. The exchange that held
/// the claim stores its token first, by an earlier clock. The caller's write
/// is conditional on the claim it read, so it leaves the renewed Account
/// usable and reads the renewed token.
#[tokio::test]
async fn a_lapsed_claim_is_not_ended_once_its_exchange_has_stored_the_token() {
    let oauth = connected("user:lapsed-claim", 1).await;
    let accounts = &oauth.h.accounts;
    oauth.answer_one_refresh(renewed()).await;
    let exchange = Arc::new(OAuthExchangeGate::default());
    let refreshing = accounts.clone().with_exchange_gate(exchange.clone());
    // The claim's deadline is a minute after two seconds.
    let refresher = oauth.read(&refreshing, 2_000_000_000);
    entered("the refresh is claimed", &exchange).await;
    let renewal = Arc::new(RenewalGate::default());
    let gated = accounts.clone().with_renewal_gate(renewal.clone());
    let late = oauth.read(&gated, 62_000_000_000);
    held("the late caller finds the claim lapsed", &renewal, 1).await;
    exchange.release.notify_one();
    let first = settled("the refresh ends", refresher).await.unwrap();
    assert_eq!(token(&first), "renewed-canary");
    renewal.release.add_permits(1);
    let read = settled("the late caller ends", late).await.unwrap();
    assert_eq!(token(&read), "renewed-canary");
    assert_eq!(oauth.refreshes_sent().await, 1);
    let account = accounts
        .get_account(
            &oauth.context,
            &oauth.workspace,
            oauth.provider.id(),
            &AccountScope::User,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(account.state, crate::account::AccountState::Ready);
}
