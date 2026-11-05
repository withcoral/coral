//! Accounts whose Credential Fields include a Secret Reference. Nothing writes
//! one yet, so these fixtures set the column directly. Storage treats every
//! scope alike, so a User's Account serves as the simplest one to retire.
use super::{AccountModule, AccountScope, AccountState, CredentialEvidence, ProviderAccountId};
use crate::Error;
use crate::access::CredentialRoute;
use crate::account_tests::{CREDENTIAL_REQUIRED_OPENAPI, authorization_request};
use crate::tests::{
    accounts_for, change_context, organization_user, publish_provider, test_modules,
};
use coral_credentials::{CredentialValues, Secret};
use coral_workspace::{ChangeContext, OrganizationRole, WorkspaceDisplayName, WorkspaceId};

const REFERENCE: &str = r#"{"app_key":{"store":"team","path":"kv/app","key":"value"}}"#;

struct AccountFixture {
    db: coral_test_support::TestDatabase,
    workspaces: coral_workspace::WorkspaceModule,
    accounts: AccountModule,
    _vault: tempfile::TempDir,
    administrator: ChangeContext,
    user: coral_workspace::AuthenticatedUser,
    context: ChangeContext,
    workspace: WorkspaceId,
    provider: crate::ProviderId,
    account: ProviderAccountId,
}

impl AccountFixture {
    async fn new() -> Self {
        let (db, workspaces, providers) = test_modules().await;
        let administrator = change_context(
            organization_user(db.database(), "user:administrator", OrganizationRole::Owner).await,
            10,
            'b',
        );
        let user = organization_user(db.database(), "user:holder", OrganizationRole::Member).await;
        let context = change_context(user.clone(), 10, 'a');
        let workspace = workspaces
            .create(&context, WorkspaceDisplayName::parse("Fields").unwrap())
            .await
            .unwrap();
        crate::tests::join_as_owner(&workspaces, administrator.actor(), workspace.id()).await;
        let workspace = workspace.id().clone();
        let provider = publish_provider(
            &providers,
            &context,
            &workspace,
            CREDENTIAL_REQUIRED_OPENAPI,
            "fields",
            CredentialRoute::Caller,
        )
        .await;
        let vault = tempfile::tempdir().unwrap();
        let accounts = accounts_for(&providers, vault.path());
        let started = accounts
            .start_user_authorization(
                &context,
                &workspace,
                provider.id(),
                authorization_request(),
                None,
            )
            .await
            .unwrap();
        let account = accounts
            .submit_secret(&context, &workspace, started.id, token("typed-canary"))
            .await
            .unwrap()
            .account_id
            .unwrap();
        Self {
            db,
            workspaces,
            accounts,
            _vault: vault,
            administrator,
            user,
            context,
            workspace,
            provider: provider.id().clone(),
            account,
        }
    }

    async fn set_references(&self, sql_value: &str) {
        let changed = self
            .db
            .execute_fixture_sql(&format!(
                "UPDATE provider_accounts SET secret_references = {sql_value} WHERE id = '{}'",
                self.account
            ))
            .await
            .unwrap();
        assert_eq!(changed, 1);
    }

    async fn evidence(&self) -> CredentialEvidence {
        crate::setup_tests::credential_evidence(
            &self.db.database(),
            &self.workspace,
            "fields",
            "getPrivateForecast",
            &crate::setup_tests::user_principal(&self.user),
        )
        .await
        .unwrap()
    }

    /// What both dispatch paths return for the current Account.
    async fn read(&self) -> [Result<String, Error>; 2] {
        let evidence = self.evidence().await;
        let exposed = |values: CredentialValues| {
            values
                .get(CredentialValues::TOKEN)
                .unwrap()
                .expose()
                .to_owned()
        };
        let outside = self
            .accounts
            .credentials(&self.workspace, &self.provider, &evidence, 20)
            .await
            .map(exposed);
        let database = self.db.database();
        let mut tx = database.begin().await.unwrap();
        let held = self
            .accounts
            .dispatch_credentials_in_transaction(
                &mut tx,
                &self.workspace,
                &self.provider,
                &evidence,
                20,
            )
            .await
            .map(|values| exposed(values.expect("static material needs no refresh")));
        tx.commit().await.unwrap();
        [outside, held]
    }

    async fn view(&self) -> super::AccountView {
        self.accounts
            .get_account(
                &self.context,
                &self.workspace,
                &self.provider,
                &AccountScope::User,
            )
            .await
            .unwrap()
    }
}

fn token(value: &str) -> CredentialValues {
    CredentialValues::token(Secret::new(value.into()).unwrap())
}

/// Coral holds no Secret Store yet, so any reference leaves a typed-in Account
/// unusable. It never falls back to the fields it can read.
#[tokio::test]
async fn an_account_with_a_reference_fails_closed_and_changes_nothing() {
    let account = AccountFixture::new().await;
    let before = account.view().await;
    for references in [
        // A reference beside the typed field.
        REFERENCE,
        // A reference to the field that also has a typed value.
        r#"{"token":{"store":"team","path":"kv/app"}}"#,
        // Malformed references: not a record, an unknown field, an empty path.
        "not json",
        r#"{"app_key":{"store":"team","path":"kv/app","version":2}}"#,
        r#"{"app_key":{"store":"team","path":""}}"#,
    ] {
        account.set_references(&format!("'{references}'")).await;
        let [outside, held] = account.read().await;
        assert!(
            matches!(outside, Err(Error::AccountRequired)),
            "{references}"
        );
        assert!(matches!(held, Err(Error::AccountRequired)), "{references}");
        assert_eq!(account.view().await, before, "{references}");
    }
}

/// A row from before the column existed, with its sealed record alone.
#[tokio::test]
async fn an_account_with_no_references_reads_its_sealed_fields() {
    let account = AccountFixture::new().await;
    let [outside, held] = account.read().await;
    assert_eq!(outside.unwrap(), "typed-canary");
    assert_eq!(held.unwrap(), "typed-canary");

    account.set_references(&format!("'{REFERENCE}'")).await;
    account.set_references("NULL").await;
    assert_eq!(account.read().await[0].as_ref().unwrap(), "typed-canary");
}

/// The schema rejects a reference on a scrubbed Account, so each of these
/// fails unless its write clears the references too.
#[tokio::test]
async fn scrubbing_an_account_removes_its_references() {
    #[derive(Debug, Clone, Copy)]
    enum Scrub {
        Disconnect,
        Retire,
    }
    for scrub in [Scrub::Disconnect, Scrub::Retire] {
        let account = AccountFixture::new().await;
        account.set_references(&format!("'{REFERENCE}'")).await;
        match scrub {
            Scrub::Disconnect => {
                account
                    .accounts
                    .disconnect(
                        &account.context,
                        &account.workspace,
                        &account.provider,
                        account.account,
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    account.view().await.account.unwrap().state,
                    AccountState::Disconnected
                );
            }
            Scrub::Retire => {
                coral_administration::AccessLifecycleModule::new(
                    account.db.database(),
                    account.workspaces.clone(),
                )
                .remove_organization_member(&account.administrator, account.user.user_id(), 1, None)
                .await
                .unwrap();
            }
        }
    }
}

/// Replacing an Account replaces every field, so old references do not survive.
#[tokio::test]
async fn replacing_an_account_replaces_its_references() {
    let account = AccountFixture::new().await;
    account.set_references(&format!("'{REFERENCE}'")).await;
    let started = account
        .accounts
        .start_reconnect(
            &account.context,
            &account.workspace,
            &account.provider,
            account.account,
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    account
        .accounts
        .submit_secret(
            &account.context,
            &account.workspace,
            started.id,
            token("replacement-canary"),
        )
        .await
        .unwrap();
    let [outside, held] = account.read().await;
    assert_eq!(outside.unwrap(), "replacement-canary");
    assert_eq!(held.unwrap(), "replacement-canary");
}
