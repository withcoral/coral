//! Search and describe apply the Provider Module's per-Operation reach rule.
//! The rule's full Principal table lives with its owner in coral-provider.
use super::*;
use coral_workspace::WorkspaceRole;

// Catches: ranking, listing or describing a Built-in Operation that the
// Principal does not reach, or hiding one it does.
#[tokio::test]
async fn search_and_describe_show_each_builtin_operation_to_the_principals_it_reaches() {
    let fixture = TestModules::new().await;
    let database = fixture.search.database.clone();
    let owner = organization_user(database.clone(), "user:builtin-owner").await;
    let workspace = fixture.create_workspace(&owner).await;
    let context = change(owner.clone(), 20, '2');
    let mut transaction = database.begin().await.unwrap();
    fixture
        .providers
        .install_builtin_in_transaction(&mut transaction, &context, &workspace)
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let member = organization_user(database.clone(), "user:builtin-member").await;
    fixture
        .workspaces
        .set_membership(&context, &workspace, &member, WorkspaceRole::Member)
        .await
        .unwrap();
    fixture.converge().await;

    for (principal, reached) in [
        (
            AuthenticatedPrincipal::user(owner),
            &["createProvider", "listProviders"][..],
        ),
        (AuthenticatedPrincipal::user(member), &["listProviders"][..]),
    ] {
        for (key, terms) in [
            ("createProvider", "create provider"),
            ("listProviders", "list providers"),
        ] {
            let reaches = reached.contains(&key);
            let call_path = format!("coral.workspace.{key}");
            for request in [
                SearchRequest::list(),
                SearchRequest::query(SearchQuery::parse(terms).unwrap()),
            ] {
                let page = fixture
                    .search
                    .search(&principal, &workspace, &request)
                    .await
                    .unwrap();
                let found = page
                    .hits()
                    .iter()
                    .any(|hit| hit.call_path().as_str() == call_path);
                assert_eq!(found, reaches, "{principal:?} search {request:?}");
            }
            let path = CallPath::parse(&call_path).unwrap();
            let described = fixture
                .search
                .describe(&principal, &workspace, &path, &[])
                .await;
            if reaches {
                assert_eq!(
                    described.unwrap().authoring().call_path(),
                    &path,
                    "{principal:?} describe"
                );
            } else {
                assert!(
                    matches!(described, Err(Error::AuthoringDescriptionNotFound)),
                    "{principal:?} describe: {described:?}"
                );
            }
        }
    }
}
