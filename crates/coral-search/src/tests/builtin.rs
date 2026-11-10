//! Search and describe apply the Provider Module's per-Tool reach rule.
//! The rule's full Principal table lives with its owner in coral-provider.
use super::*;
use coral_workspace::WorkspaceRole;

// Catches: ranking, listing or describing a Built-in Tool that the
// Principal does not reach, or hiding one it does.
#[tokio::test]
async fn search_and_describe_show_each_builtin_tool_to_the_principals_it_reaches() {
    let fixture = TestModules::new().await;
    let database = fixture.search.database.clone();
    let owner = organization_user(database.clone(), "user:builtin-owner").await;
    let workspace = fixture.create_workspace(&owner).await;
    let context = change(owner.clone(), 20, '2');
    let mut transaction = database.begin().await.unwrap();
    fixture
        .providers
        .install_coral_defined_in_transaction(
            &mut transaction,
            &context,
            &workspace,
            coral_provider::CoralDefinedProvider::Builtin,
        )
        .await
        .unwrap()
        .skip_search();
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
            &[
                "addProvider",
                "listProviders",
                "getPolicy",
                "updatePolicy",
                "search",
            ][..],
        ),
        (
            AuthenticatedPrincipal::user(member),
            &["listProviders", "getPolicy", "search"][..],
        ),
    ] {
        for (key, terms) in [
            ("addProvider", "add provider"),
            ("listProviders", "list providers"),
            ("getPolicy", "provider policy"),
            ("updatePolicy", "update policy"),
            ("search", "search tools"),
        ] {
            let reaches = reached.contains(&key);
            let call_path = format!("coral.{key}");
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

// Catches: coral.search's hit claiming to be complete while its
// line shows only the query overload and hides the paths modes.
#[tokio::test]
async fn the_search_tool_hit_is_not_complete_while_its_line_shows_one_overload() {
    let fixture = TestModules::new().await;
    let database = fixture.search.database.clone();
    let owner = organization_user(database.clone(), "user:builtin-overloads").await;
    let workspace = fixture.create_workspace(&owner).await;
    let context = change(owner.clone(), 20, '3');
    let mut transaction = database.begin().await.unwrap();
    fixture
        .providers
        .install_coral_defined_in_transaction(
            &mut transaction,
            &context,
            &workspace,
            coral_provider::CoralDefinedProvider::Builtin,
        )
        .await
        .unwrap()
        .skip_search();
    transaction.commit().await.unwrap();
    fixture.converge().await;

    let page = fixture
        .search
        .search(
            &AuthenticatedPrincipal::user(owner),
            &workspace,
            &SearchRequest::list(),
        )
        .await
        .unwrap();
    let hit = page
        .hits()
        .iter()
        .find(|hit| hit.call_path().as_str() == "coral.search")
        .expect("search hit");

    assert!(
        hit.signature_line().starts_with("search(input: { ")
            && hit.signature_line().contains("query?: string")
            && !hit.signature_line().contains("paths"),
        "the line shows the first overload: {}",
        hit.signature_line()
    );
    assert!(
        !hit.complete(),
        "the paths overloads are hidden: {}",
        hit.signature_line()
    );
}
