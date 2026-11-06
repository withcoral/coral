//! A shared Account whose Credential Fields a Secret Store holds.
use std::collections::BTreeMap;
use std::sync::Arc;

use coral_credentials::{CredentialField, CredentialValues, Secret, SecretReference};
use coral_secret_store::{Error as StoreError, SecretStores};
use coral_test_support::InMemorySecretStore;
use coral_workspace::{
    AuthenticatedUser, ChangeContext, OrganizationRole, WorkspaceDisplayName, WorkspaceId,
    WorkspaceModule, WorkspaceRole,
};

use super::{
    AccountModule, AccountScope, AccountView, AuthorizationId, AuthorizationState,
    CredentialEvidence, ProviderAccountId,
};
use crate::account_tests::{CREDENTIAL_REQUIRED_OPENAPI, authorization_request};
use crate::tests::{accounts_for, change_context, organization_user, test_modules};
use crate::{Error, ProviderId, ProviderModule};
coral_test_support::provider_setup_fixtures!(crate);

const PATH: &str = "team/forecast";
const TYPED: &str = "typed-canary";

struct Shared {
    db: coral_test_support::TestDatabase,
    workspaces: WorkspaceModule,
    providers: ProviderModule,
    accounts: AccountModule,
    store: Arc<InMemorySecretStore>,
    _vault: tempfile::TempDir,
    /// An Organization Owner and the Workspace's designated Owner.
    owner: ChangeContext,
    member: AuthenticatedUser,
    workspace: WorkspaceId,
    provider: ProviderId,
    account: ProviderAccountId,
}

/// A connected `fixed` Account with one typed field, in a Workspace granted the
/// `team` store. Config also defines `ungranted`.
async fn shared() -> Shared {
    let (db, workspaces, providers) = test_modules().await;
    let owner = change_context(
        organization_user(db.database(), "user:owner", OrganizationRole::Owner).await,
        10,
        'a',
    );
    let member = organization_user(db.database(), "user:member", OrganizationRole::Member).await;
    let workspace = workspaces
        .create(&owner, WorkspaceDisplayName::parse("Stores").unwrap())
        .await
        .unwrap();
    workspaces
        .set_membership(&owner, workspace.id(), &member, WorkspaceRole::Member)
        .await
        .unwrap();
    workspaces
        .grant_secret_store(&owner, workspace.id(), "team")
        .await
        .unwrap();
    let store = Arc::new(InMemorySecretStore::new());
    store.set(PATH, "token", "stored-canary");
    let vault = tempfile::tempdir().unwrap();
    let accounts = accounts_for(&providers, vault.path()).with_secret_stores(
        coral_test_support::secret_stores([
            ("team", Arc::clone(&store) as _),
            ("ungranted", Arc::clone(&store) as _),
        ]),
    );
    let provider = providers
        .publish_shared_fixture(
            &owner,
            workspace.id(),
            crate::OpenApiDocument::parse(CREDENTIAL_REQUIRED_OPENAPI).unwrap(),
            "shared",
            &accounts,
            CredentialValues::token(Secret::new(TYPED.into()).unwrap()),
        )
        .await
        .unwrap();
    let account = accounts
        .get_account(&owner, workspace.id(), provider.id(), &AccountScope::Shared)
        .await
        .unwrap()
        .account
        .unwrap()
        .id;
    Shared {
        db,
        workspaces,
        providers,
        accounts,
        store,
        _vault: vault,
        owner,
        member,
        workspace: workspace.id().clone(),
        provider: provider.id().clone(),
        account,
    }
}

fn reference(store: &str, path: &str, key: &str) -> BTreeMap<String, CredentialField> {
    BTreeMap::from([(
        CredentialValues::TOKEN.to_owned(),
        CredentialField::Reference(
            SecretReference::new(store.into(), path.into(), Some(key.into())).unwrap(),
        ),
    )])
}

impl Shared {
    async fn start(&self) -> AuthorizationId {
        self.accounts
            .start_reconnect(
                &self.owner,
                &self.workspace,
                &self.provider,
                self.account,
                authorization_request(),
                None,
            )
            .await
            .unwrap()
            .id
    }

    async fn save(&self, fields: BTreeMap<String, CredentialField>) -> Result<(), Error> {
        let id = self.start().await;
        let saved = self
            .accounts
            .submit_fields(&self.owner, &self.workspace, id, fields)
            .await?;
        assert_eq!(saved.state, AuthorizationState::Succeeded);
        Ok(())
    }

    async fn evidence(&self) -> CredentialEvidence {
        crate::setup_tests::credential_evidence(
            &self.db.database(),
            &self.workspace,
            "shared",
            "getPrivateForecast",
            &crate::setup_tests::user_principal(self.owner.actor()),
        )
        .await
        .unwrap()
    }

    async fn read(&self, evidence: &CredentialEvidence) -> Result<String, Error> {
        self.accounts
            .credentials(&self.workspace, &self.provider, evidence, 20)
            .await
            .map(|values| {
                values
                    .get(CredentialValues::TOKEN)
                    .unwrap()
                    .expose()
                    .to_owned()
            })
    }

    async fn view(&self) -> AccountView {
        self.accounts
            .get_account(
                &self.owner,
                &self.workspace,
                &self.provider,
                &AccountScope::Shared,
            )
            .await
            .unwrap()
    }
}

// Catches: a value Coral keeps instead of reading the store, so rotation has no
// effect, or a save that never checks the reference.
#[tokio::test]
async fn a_saved_reference_is_checked_once_and_read_again_for_each_dispatch() {
    let f = shared().await;
    let reads = f.store.reads();
    f.save(reference("team", PATH, "token")).await.unwrap();
    assert_eq!(
        f.store.reads(),
        reads + 1,
        "saving reads the reference once"
    );

    let evidence = f.evidence().await;
    assert_eq!(f.read(&evidence).await.unwrap(), "stored-canary");
    f.store.set(PATH, "token", "rotated-canary");
    assert_eq!(f.read(&evidence).await.unwrap(), "rotated-canary");
    assert_eq!(f.store.reads(), reads + 3);
}

// Catches: a save that passes without reading, an unhelpful cause, or a refusal
// that replaces the Account or ends the Authorization.
#[tokio::test]
async fn a_reference_that_fails_its_check_changes_nothing() {
    let f = shared().await;
    let before = f.view().await;
    let evidence = f.evidence().await;
    for failure in [
        StoreError::Unreachable,
        StoreError::PermissionDenied,
        StoreError::MalformedResponse,
    ] {
        f.store.fail_with(Some(failure));
        let result = f.save(reference("team", PATH, "token")).await;
        assert!(matches!(result, Err(Error::SecretStoreRead(read)) if read == failure));
    }
    f.store.fail_with(None);
    for (path, key, failure) in [
        ("team/missing", "token", StoreError::EntryNotFound),
        (PATH, "missing", StoreError::KeyNotFound),
    ] {
        let result = f.save(reference("team", path, key)).await;
        assert!(matches!(result, Err(Error::SecretStoreRead(read)) if read == failure));
    }

    // The Workspace holds no grant for one, and config does not define the other.
    let reads = f.store.reads();
    for store in ["ungranted", "undefined"] {
        let result = f.save(reference(store, PATH, "token")).await;
        assert!(
            matches!(result, Err(Error::SecretStoreNotGranted)),
            "{store}"
        );
    }
    assert_eq!(f.store.reads(), reads, "a refused store is never read");

    assert_eq!(f.view().await, before);
    assert_eq!(f.read(&evidence).await.unwrap(), TYPED);
}

// Catches: a failed Authorization that cannot try again after the Owner fixes it.
#[tokio::test]
async fn a_failed_check_leaves_the_authorization_open_for_another_try() {
    let f = shared().await;
    let id = f.start().await;
    assert!(matches!(
        f.accounts
            .submit_fields(
                &f.owner,
                &f.workspace,
                id,
                reference("team", "wrong", "token")
            )
            .await,
        Err(Error::SecretStoreRead(StoreError::EntryNotFound))
    ));
    let saved = f
        .accounts
        .submit_fields(&f.owner, &f.workspace, id, reference("team", PATH, "token"))
        .await
        .unwrap();
    assert_eq!(saved.state, AuthorizationState::Succeeded);
}

// Catches: a Coral failure that names the path, or a fallback to a field Coral can
// read, or a read during the preparation that runs before enforcement decides.
#[tokio::test]
async fn a_failed_read_fails_closed_without_naming_the_path() {
    let f = shared().await;
    f.save(reference("team", PATH, "token")).await.unwrap();
    let evidence = f.evidence().await;
    for failure in [
        StoreError::Unreachable,
        StoreError::EntryNotFound,
        StoreError::KeyNotFound,
        StoreError::PermissionDenied,
        StoreError::MalformedResponse,
    ] {
        f.store.fail_with(Some(failure));
        let error = f.read(&evidence).await.unwrap_err();
        assert!(matches!(error, Error::SecretStoreRead(read) if read == failure));
        assert!(!format!("{error} {error:?}").contains("team/"), "{error}");
    }
    f.store.fail_with(None);

    let reads = f.store.reads();
    f.accounts
        .prepare_dispatch(&f.workspace, &f.provider, &evidence, 20)
        .await
        .unwrap();
    assert_eq!(f.store.reads(), reads, "preparation never reads the store");
}

// Catches: a read the operator can't trace to its store, since the metric never
// names one, or a record that carries the path, the key or the value.
#[tokio::test]
async fn each_read_logs_its_store_and_never_its_path_or_value() {
    use tracing_subscriber::layer::SubscriberExt as _;

    // For the whole process: `set_default` misses a callsite another test's
    // thread registered first. No other test here asserts on logs.
    let exported = coral_telemetry::testing::ExportCapture::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(exported.log_layer()),
    )
    .unwrap();
    let f = shared().await;
    f.save(reference("team", PATH, "token")).await.unwrap();
    let evidence = f.evidence().await;
    f.read(&evidence).await.unwrap();
    f.store.fail_with(Some(StoreError::PermissionDenied));
    f.read(&evidence).await.unwrap_err();

    let logs = exported.text();
    for expected in [
        "coral.secret_store.name",
        "\"team\"",
        "coral.secret_store.read_ms",
        "permission_denied",
    ] {
        assert!(logs.contains(expected), "{expected} in {logs}");
    }
    for secret in [PATH, "stored-canary"] {
        assert!(!logs.contains(secret), "{secret} in {logs}");
    }
}

// Catches: a revoked store still answering, which ADR 0032 forbids, or a removed
// grant reaching the store at all.
#[tokio::test]
async fn a_store_the_workspace_lost_is_never_read() {
    let f = shared().await;
    f.save(reference("team", PATH, "token")).await.unwrap();
    let evidence = f.evidence().await;
    f.workspaces
        .revoke_secret_store(&f.owner, &f.workspace, "team")
        .await
        .unwrap();
    let reads = f.store.reads();

    assert!(matches!(
        f.read(&evidence).await,
        Err(Error::AccountRequired)
    ));
    assert!(matches!(
        f.accounts
            .prepare_dispatch(&f.workspace, &f.provider, &evidence, 20)
            .await,
        Err(Error::AccountRequired)
    ));
    assert_eq!(f.store.reads(), reads);

    // A store config removed reads the same, from a deployment without it.
    f.workspaces
        .grant_secret_store(&f.owner, &f.workspace, "team")
        .await
        .unwrap();
    let without = f
        .accounts
        .clone()
        .with_secret_stores(SecretStores::default());
    assert!(matches!(
        without
            .credentials(&f.workspace, &f.provider, &evidence, 20)
            .await,
        Err(Error::AccountRequired)
    ));
    assert_eq!(f.store.reads(), reads);
}

// Catches: a changed reference that pending work could still use.
#[tokio::test]
async fn changing_a_reference_replaces_the_authorization() {
    let f = shared().await;
    f.save(reference("team", PATH, "token")).await.unwrap();
    let pending = f.evidence().await;
    f.store.set("team/other", "token", "other-canary");

    f.save(reference("team", "team/other", "token"))
        .await
        .unwrap();

    assert!(matches!(
        f.read(&pending).await,
        Err(Error::AccountRequired)
    ));
    assert_eq!(f.read(&f.evidence().await).await.unwrap(), "other-canary");
}

// Catches: a User, who could name a path holding another person's secret, setting
// a reference on their own Account, or an Account changed by the refusal.
#[tokio::test]
async fn a_users_own_account_refuses_a_reference() {
    let f = shared().await;
    let provider = crate::tests::publish_provider(
        &f.providers,
        &f.owner,
        &f.workspace,
        CREDENTIAL_REQUIRED_OPENAPI,
        "personal",
        crate::access::CredentialRoute::Caller,
    )
    .await;
    let context = change_context(f.member.clone(), 11, 'c');
    let started = f
        .accounts
        .start_user_authorization(
            &context,
            &f.workspace,
            provider.id(),
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    let reads = f.store.reads();
    let before = f
        .accounts
        .get_account(&context, &f.workspace, provider.id(), &AccountScope::User)
        .await
        .unwrap();

    assert!(matches!(
        f.accounts
            .submit_fields(
                &context,
                &f.workspace,
                started.id,
                reference("team", PATH, "token")
            )
            .await,
        Err(Error::SecretReferenceNotAllowed)
    ));

    assert_eq!(f.store.reads(), reads);
    assert_eq!(
        f.accounts
            .get_account(&context, &f.workspace, provider.id(), &AccountScope::User)
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        f.accounts
            .get_authorization(&context, &f.workspace, started.id)
            .await
            .unwrap()
            .state,
        AuthorizationState::Pending
    );
}

// Catches: OAuth taking a reference, whose refresh would have to write to the store.
#[tokio::test]
async fn an_oauth_account_refuses_a_reference() {
    let f = shared().await;
    let document = serde_json::json!({
        "openapi":"3.1.0", "info":{"title":"OAuth","version":"1"},
        "servers":[{"url":"https://oauth.example"}], "security":[{"oauth":["read"]}],
        "components":{"securitySchemes":{"oauth":{"type":"oauth2","flows":{"authorizationCode":{
            "authorizationUrl":"https://oauth.example/authorize",
            "tokenUrl":"https://oauth.example/token", "scopes":{"read":"Read"}
        }}}}},
        "paths":{"/data":{"get":{"operationId":"getData","responses":{"200":{"description":"Data"}}}}}
    });
    let provider = crate::tests::publish_provider(
        &f.providers,
        &f.owner,
        &f.workspace,
        &document.to_string(),
        "oauth",
        crate::access::CredentialRoute::Caller,
    )
    .await;
    let started = f
        .accounts
        .start_user_authorization(
            &f.owner,
            &f.workspace,
            provider.id(),
            authorization_request(),
            Some(
                super::OAuthClient::new(
                    "registered-client".into(),
                    "https://reef.example/callback".into(),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    let reads = f.store.reads();
    let before = f
        .accounts
        .get_account(&f.owner, &f.workspace, provider.id(), &AccountScope::User)
        .await
        .unwrap();

    assert!(matches!(
        f.accounts
            .submit_fields(
                &f.owner,
                &f.workspace,
                started.id,
                reference("team", PATH, "token")
            )
            .await,
        Err(Error::AccountInput)
    ));
    assert_eq!(f.store.reads(), reads);
    assert_eq!(
        f.accounts
            .get_account(&f.owner, &f.workspace, provider.id(), &AccountScope::User)
            .await
            .unwrap(),
        before
    );
}

// Catches: a disconnect that keeps references, or Coral touching the store.
#[tokio::test]
async fn disconnecting_removes_the_reference_and_leaves_the_store_alone() {
    let f = shared().await;
    f.save(reference("team", PATH, "token")).await.unwrap();
    let evidence = f.evidence().await;
    let reads = f.store.reads();

    f.accounts
        .disconnect(&f.owner, &f.workspace, &f.provider, f.account)
        .await
        .unwrap();

    assert!(matches!(
        f.read(&evidence).await,
        Err(Error::AccountRequired)
    ));
    assert_eq!(f.store.reads(), reads, "cleanup never reaches the store");
    let probe = SecretReference::new("team".into(), PATH.into(), Some("token".into())).unwrap();
    assert_eq!(
        coral_secret_store::SecretStore::read(&*f.store, &probe)
            .await
            .unwrap()
            .expose(),
        "stored-canary"
    );
    let cleared =
        f.db.execute_fixture_sql(&format!(
            "UPDATE provider_accounts SET secret_references = NULL \
             WHERE id = '{}' AND secret_references IS NULL",
            f.account
        ))
        .await
        .unwrap();
    assert_eq!(cleared, 1, "the reference went with the Account's values");
}

/// A connected `fixed` Account in the `shared()` Workspace that sends `email` and
/// `api_token` as HTTP Basic, Jira's shape.
async fn publish_basic(f: &Shared) -> (ProviderId, ProviderAccountId) {
    let setup = f
        .providers
        .prepare_openapi_fixture(
            &f.owner,
            &f.workspace,
            crate::OpenApiDocument::parse(CREDENTIAL_REQUIRED_OPENAPI).unwrap(),
        )
        .await
        .unwrap();
    let review = f
        .providers
        .review_setup_configuration(f.owner.actor(), &f.workspace, setup.id())
        .await
        .unwrap();
    let mut configuration = review.confirmed.clone().unwrap();
    let field = |label: &str, sensitive| coral_catalog::CredentialField {
        label: label.into(),
        description: String::new(),
        sensitive,
    };
    configuration.credential_fields = BTreeMap::from([
        ("email".to_owned(), field("Email", false)),
        ("api_token".to_owned(), field("API token", true)),
    ]);
    configuration.authentication = Some(coral_catalog::AuthenticationConfiguration::BasicAuth {
        username: coral_catalog::CredentialSource::Field {
            field: "email".into(),
        },
        password: coral_catalog::CredentialSource::Field {
            field: "api_token".into(),
        },
    });
    let confirmed = f
        .providers
        .confirm_setup_configuration(
            &f.owner,
            &f.workspace,
            setup.id(),
            review.revision,
            configuration,
        )
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty(), "{:?}", confirmed.errors);
    let secret = |value: &str| Secret::new(value.into()).unwrap();
    let grant = f
        .accounts
        .authorize_setup(
            &f.owner,
            &f.workspace,
            setup.id(),
            confirmed.revision,
            CredentialValues::new(BTreeMap::from([
                ("email".to_owned(), secret("typed@example.com")),
                ("api_token".to_owned(), secret("typed-token")),
            ]))
            .unwrap(),
        )
        .await
        .unwrap();
    let published = f
        .providers
        .publish_workspace_provider(
            &f.owner,
            crate::tests::publication_request(
                &f.workspace,
                setup.id(),
                "basic",
                crate::access::CredentialRoute::Fixed,
                "a0000000-0000-4000-8000-000000000098",
            )
            .with_account_grant(grant.provisional_grant_id.unwrap()),
        )
        .await
        .unwrap()
        .skip_search();
    let provider = published.published_provider().unwrap().id().clone();
    let account = f
        .accounts
        .get_account(&f.owner, &f.workspace, &provider, &AccountScope::Shared)
        .await
        .unwrap()
        .account
        .unwrap()
        .id;
    (provider, account)
}

// Catches: a typed field lost beside a reference, or a stored value that Basic
// would split at its colon, so the service reads another username.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one Account follows a refused field, a mixed save and a refused stored value"
)]
async fn one_account_mixes_a_typed_value_and_a_reference() {
    let f = shared().await;
    let (provider, account) = publish_basic(&f).await;
    let start = || async {
        f.accounts
            .start_reconnect(
                &f.owner,
                &f.workspace,
                &provider,
                account,
                authorization_request(),
                None,
            )
            .await
            .unwrap()
            .id
    };
    let fields = |key: &str| {
        BTreeMap::from([
            (
                "email".to_owned(),
                CredentialField::Value(Secret::new("typed@example.com".into()).unwrap()),
            ),
            (
                "api_token".to_owned(),
                CredentialField::Reference(
                    SecretReference::new("team".into(), PATH.into(), Some(key.into())).unwrap(),
                ),
            ),
        ])
    };

    let before = f
        .accounts
        .get_account(&f.owner, &f.workspace, &provider, &AccountScope::Shared)
        .await
        .unwrap();
    // A field the method does not name is no field at all.
    let extra = BTreeMap::from([(
        "other".to_owned(),
        CredentialField::Reference(
            SecretReference::new("team".into(), PATH.into(), Some("token".into())).unwrap(),
        ),
    )]);
    assert!(matches!(
        f.accounts
            .submit_fields(&f.owner, &f.workspace, start().await, extra)
            .await,
        Err(Error::AccountInput)
    ));
    assert_eq!(
        f.accounts
            .get_account(&f.owner, &f.workspace, &provider, &AccountScope::Shared)
            .await
            .unwrap(),
        before
    );

    f.accounts
        .submit_fields(&f.owner, &f.workspace, start().await, fields("token"))
        .await
        .unwrap();
    let evidence = crate::setup_tests::credential_evidence(
        &f.db.database(),
        &f.workspace,
        "basic",
        "getPrivateForecast",
        &crate::setup_tests::user_principal(f.owner.actor()),
    )
    .await
    .unwrap();
    let values = f
        .accounts
        .credentials(&f.workspace, &provider, &evidence, 20)
        .await
        .unwrap();
    assert_eq!(values.get("email").unwrap().expose(), "typed@example.com");
    assert_eq!(values.get("api_token").unwrap().expose(), "stored-canary");

    // The username field, held in the store, with a colon Basic would split at.
    f.store.set("team/email", "value", "a:b");
    let colon = BTreeMap::from([
        (
            "email".to_owned(),
            CredentialField::Reference(
                SecretReference::new("team".into(), "team/email".into(), Some("value".into()))
                    .unwrap(),
            ),
        ),
        (
            "api_token".to_owned(),
            CredentialField::Value(Secret::new("typed-token".into()).unwrap()),
        ),
    ]);
    assert!(matches!(
        f.accounts
            .submit_fields(&f.owner, &f.workspace, start().await, colon)
            .await,
        Err(Error::AccountInput)
    ));
    let connected = f
        .accounts
        .get_account(&f.owner, &f.workspace, &provider, &AccountScope::Shared)
        .await
        .unwrap();
    assert_eq!(
        connected.account.as_ref().unwrap().connected_at_unix_nanos,
        Some(10)
    );
    let values = f
        .accounts
        .credentials(&f.workspace, &provider, &evidence, 20)
        .await
        .unwrap();
    assert_eq!(values.get("email").unwrap().expose(), "typed@example.com");
}
