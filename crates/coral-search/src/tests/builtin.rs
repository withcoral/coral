//! Search and describe apply the Provider Module's Built-in discovery rule. The
//! rule's full Principal table lives with its owner in coral-provider.
use super::*;
use coral_workspace::WorkspaceRole;

const CREATE_PROVIDER: &str = "coral.admin.createProvider";

#[tokio::test]
async fn search_and_describe_hide_builtin_operations_from_a_member() {
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

    let path = CallPath::parse(CREATE_PROVIDER).unwrap();
    for (principal, discovers) in [
        (AuthenticatedPrincipal::user(owner), true),
        (AuthenticatedPrincipal::user(member), false),
    ] {
        for request in [
            SearchRequest::list(),
            SearchRequest::query(SearchQuery::parse("create provider").unwrap()),
        ] {
            let page = fixture
                .search
                .search(&principal, &workspace, &request)
                .await
                .unwrap();
            let found = page
                .hits()
                .iter()
                .any(|hit| hit.call_path().as_str() == CREATE_PROVIDER);
            assert_eq!(found, discovers, "{principal:?} search {request:?}");
        }
        let described = fixture
            .search
            .describe(&principal, &workspace, &path, &[])
            .await;
        if !discovers {
            assert!(
                matches!(described, Err(Error::AuthoringDescriptionNotFound)),
                "{principal:?} describe: {described:?}"
            );
            continue;
        }
        assert_eq!(
            described.unwrap().authoring().call_path(),
            &path,
            "{principal:?} describe"
        );
    }
}
