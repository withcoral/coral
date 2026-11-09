//! Provider-owned totals for anonymous usage data. Each read joins the
//! caller's snapshot and returns counts alone: never a name, a URL or a
//! credential. The catalog's persistence spellings and decoders classify
//! each row.

use coral_catalog::{AuthenticationKind, ProviderCredentialContract, ProviderSetupInputKind};
use coral_db::Transaction;
use sea_query::{Alias, Asterisk, Expr, ExprTrait as _, JoinType, Query};

use crate::Error;

/// How many Providers and Accounts exist now.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProviderTotals {
    /// Providers whose active Generation came from an OpenAPI document.
    pub openapi: u64,
    /// Providers whose active Generation came from an MCP server.
    pub mcp: u64,
    /// Current Accounts that connect with OAuth.
    pub oauth_accounts: u64,
    /// Current Accounts that connect with an entered secret.
    pub secret_accounts: u64,
}

/// Counts the Providers and current Accounts in every Workspace. The
/// Built-in Provider is not one a user made, and an Account with no
/// credential counts as neither kind. The caller authorizes the read.
///
/// # Errors
///
/// Returns a database failure, or corrupt data for an Account contract Coral
/// did not write.
pub async fn provider_totals(tx: &mut Transaction<'_>) -> Result<ProviderTotals, Error> {
    let (provider, generation) = (Alias::new("p"), Alias::new("g"));
    let input_kind = (generation.clone(), Alias::new("input_kind"));
    let kinds: Vec<(String, i64)> = tx
        .fetch_all(
            Query::select()
                .column(input_kind.clone())
                .expr(Expr::col(Asterisk).count())
                .from_as(Alias::new("provider_records"), provider.clone())
                .join_as(
                    JoinType::InnerJoin,
                    Alias::new("provider_generations"),
                    generation.clone(),
                    Expr::col((generation, Alias::new("id")))
                        .equals((provider, Alias::new("active_generation_id"))),
                )
                .group_by_col(input_kind)
                .to_owned(),
        )
        .await?;
    let mut totals = ProviderTotals::default();
    for (kind, providers) in kinds {
        let providers = u64::try_from(providers).unwrap_or_default();
        if kind == ProviderSetupInputKind::OpenApi.as_str() {
            totals.openapi += providers;
        } else if kind == ProviderSetupInputKind::Mcp.as_str() {
            totals.mcp += providers;
        }
    }
    let contracts: Vec<(String,)> = tx
        .fetch_all(
            Query::select()
                .column(Alias::new("contract"))
                .from(Alias::new("provider_accounts"))
                .and_where(Expr::col(Alias::new("retired")).eq(0_i64))
                .to_owned(),
        )
        .await?;
    for (contract,) in contracts {
        match ProviderCredentialContract::from_storage(&contract)?
            .authentication()
            .kind()
        {
            AuthenticationKind::None => {}
            AuthenticationKind::OAuthDeviceCode | AuthenticationKind::OAuthAuthorizationCode => {
                totals.oauth_accounts += 1;
            }
            AuthenticationKind::Bearer
            | AuthenticationKind::HeaderCredentials
            | AuthenticationKind::BasicAuth
            | AuthenticationKind::QueryCredentials => totals.secret_accounts += 1,
        }
    }
    Ok(totals)
}

#[cfg(test)]
mod tests {
    use coral_catalog::{
        AuthenticationConfiguration, OAuthScopeFormat, ProviderCredentialContract,
    };
    use coral_credentials::{CredentialValues, Secret};
    use coral_workspace::{OrganizationRole, WorkspaceDisplayName};

    use super::{ProviderTotals, provider_totals};
    use crate::access::CredentialRoute;
    use crate::account_tests::{CREDENTIAL_REQUIRED_OPENAPI, authorization_request};
    use crate::search_tests::MIXED_MCP;
    use crate::tests::{
        accounts_for, change_context, organization_user, publish_provider, test_modules,
    };

    coral_test_support::provider_setup_fixtures!(crate);

    /// Publishes an OpenAPI and an MCP Provider in a new Workspace, and
    /// connects one secret Account to the OpenAPI one. Returns the Account's
    /// ID.
    async fn two_providers_and_a_secret_account(
        db: &coral_test_support::TestDatabase,
        workspaces: &coral_workspace::WorkspaceModule,
        providers: &crate::ProviderModule,
    ) -> String {
        let owner =
            organization_user(db.database(), "user:usage-owner", OrganizationRole::Owner).await;
        let context = change_context(owner, 10, 'a');
        let workspace = workspaces
            .create(
                &context,
                WorkspaceDisplayName::parse("Usage").expect("name"),
            )
            .await
            .expect("Workspace");
        let weather = publish_provider(
            providers,
            &context,
            workspace.id(),
            CREDENTIAL_REQUIRED_OPENAPI,
            "weather",
            CredentialRoute::Caller,
        )
        .await;
        let setup = providers
            .prepare_mcp_fixture(
                &context,
                workspace.id(),
                crate::McpDiscoverySnapshot::parse(MIXED_MCP).expect("snapshot"),
            )
            .await
            .expect("MCP Setup");
        providers
            .publish_workspace_provider(
                &context,
                crate::WorkspaceProviderPublicationRequest::new(
                    crate::ManagementRequestId::parse(&coral_test_support::fixture_request_id())
                        .expect("request"),
                    workspace.id().clone(),
                    setup.id().clone(),
                    crate::ProviderNamespace::parse("mixed").expect("namespace"),
                    CredentialRoute::None,
                ),
            )
            .await
            .expect("publish MCP Provider")
            .skip_search();
        let dir = tempfile::tempdir().expect("credential dir");
        let accounts = accounts_for(providers, dir.path());
        let authorization = accounts
            .start_user_authorization(
                &context,
                workspace.id(),
                weather.access().id(),
                authorization_request(),
                None,
            )
            .await
            .expect("authorization");
        accounts
            .submit_secret(
                &context,
                workspace.id(),
                authorization.id,
                CredentialValues::token(Secret::new("usage-secret".into()).expect("secret")),
            )
            .await
            .expect("secret Account")
            .account_id
            .expect("Account")
            .to_string()
    }

    /// A Provider counts by its active Generation's input. A current Account
    /// counts by the method its contract names; one with no credential or a
    /// retired one does not count.
    #[tokio::test]
    async fn totals_count_providers_by_input_and_accounts_by_method() {
        let (db, workspaces, providers) = test_modules().await;
        let database = db.database();
        let totals = || async {
            let mut tx = database.begin_read_snapshot().await.expect("snapshot");
            let totals = provider_totals(&mut tx).await.expect("totals");
            tx.commit().await.expect("commit");
            totals
        };
        let before = totals().await;
        let account = two_providers_and_a_secret_account(&db, &workspaces, &providers).await;
        let delta = |totals: ProviderTotals| ProviderTotals {
            openapi: totals.openapi - before.openapi,
            mcp: totals.mcp - before.mcp,
            oauth_accounts: totals.oauth_accounts - before.oauth_accounts,
            secret_accounts: totals.secret_accounts - before.secret_accounts,
        };
        let providers_only = ProviderTotals {
            openapi: 1,
            mcp: 1,
            ..ProviderTotals::default()
        };
        assert_eq!(
            delta(totals().await),
            ProviderTotals {
                secret_accounts: 1,
                ..providers_only
            }
        );

        // The same Account under other contracts, as the catalog encodes them.
        let oauth =
            ProviderCredentialContract::from(AuthenticationConfiguration::OAuthAuthorizationCode {
                authorization_url: "https://auth.example/authorize".into(),
                token_url: "https://auth.example/token".into(),
                scopes: Vec::new(),
                scope_format: OAuthScopeFormat::default(),
                resource: None,
                issuer: None,
                client: None,
            });
        let none = ProviderCredentialContract::from(AuthenticationConfiguration::None);
        let bearer = ProviderCredentialContract::from(AuthenticationConfiguration::Bearer);
        for (set, expected) in [
            (
                format!("contract = '{}'", oauth.to_storage().expect("OAuth")),
                ProviderTotals {
                    oauth_accounts: 1,
                    ..providers_only
                },
            ),
            (
                format!("contract = '{}'", none.to_storage().expect("none")),
                providers_only,
            ),
            (
                format!(
                    "contract = '{}', retired = 1, encrypted_credentials = '', \
                        requires_authorization = 1",
                    bearer.to_storage().expect("bearer")
                ),
                providers_only,
            ),
        ] {
            db.execute_fixture_sql(&format!(
                "UPDATE provider_accounts SET {set} WHERE id = '{account}'"
            ))
            .await
            .expect(&set);
            assert_eq!(delta(totals().await), expected, "{set}");
        }
    }
}
