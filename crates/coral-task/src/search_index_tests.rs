//! Indexing a Saved Function through `SearchIndex`.
//!
//! Every test saves, deletes, or removes access through the real modules.
//! The Task command's save notifies the fixture's index; a test awaits the
//! index's writes, or lets a reconcile pass find the function, and observes
//! the index through a test-only read of the search-owned tables.

use super::*;
use sea_query::{Alias, Expr, ExprTrait as _, Order, Query};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use tracing_subscriber::layer::SubscriberExt as _;

const INJECTED_FAULT: &str = "injected index fault";

#[tokio::test]
async fn one_reconcile_pass_indexes_a_function_a_notification_missed_and_a_second_changes_nothing()
{
    let fixture = Fixture::new().await;
    let index = &fixture.index;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Save a function");
    let saved = fixture
        .commands
        .save_function(&call, "triage.openIssues", "Find open issues", SOURCE)
        .await
        .unwrap();
    index.settled().await;
    lose_function_rows(&fixture).await;

    index.reconcile().await;

    assert_eq!(
        function_rows(&fixture)
            .await
            .into_iter()
            .map(|row| (row.doc_id, row.fingerprint))
            .collect::<Vec<_>>(),
        [(saved.id().to_string(), "1.1".to_string())]
    );
    let stamps = indexed_at(&fixture).await;
    index.reconcile().await;
    assert_eq!(
        indexed_at(&fixture).await,
        stamps,
        "a second pass changes nothing"
    );
}

#[tokio::test]
async fn a_pass_reindexes_a_function_whose_fingerprint_differs_from_its_revision() {
    // Notifications that land out of order leave revision 9 indexed over
    // revision 10. As text, "9" sorts after "10", so only a comparison for
    // inequality finds the row stale.
    let fixture = Fixture::new().await;
    let index = &fixture.index;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Save a function");
    let mut entries = Vec::new();
    for revision in 1..=10 {
        let saved = fixture
            .commands
            .save_function(&call, "answer", &format!("Revision {revision}"), SOURCE)
            .await
            .unwrap();
        entries.push(coral_functions::SavedFunctionEntry::from_saved(&saved));
    }
    let [.., ninth, tenth] = entries.as_slice() else {
        panic!("ten revisions");
    };
    assert_eq!((ninth.revision(), tenth.revision()), (9, 10));
    index.index_function(tenth.clone());
    index.index_function(ninth.clone());
    index.settled().await;

    index.reconcile().await;

    assert_eq!(
        function_rows(&fixture)
            .await
            .into_iter()
            .map(|row| (row.description, row.fingerprint))
            .collect::<Vec<_>>(),
        [("Revision 10".to_string(), "10.1".to_string())]
    );
}

#[tokio::test]
async fn a_pass_retries_a_failed_function_once_its_retry_is_due() {
    // A failed attempt leaves no row. A failure in one process and a success
    // in another can commit together and leave a current row beside a
    // `failed` state row; the same rule applies.
    for row_beside_failure in [false, true] {
        let fixture = Fixture::new().await;
        let index = &fixture.index;
        let goal = fixture.goal().await;
        let call = fixture.call(&goal, "Save a function");
        if row_beside_failure {
            let saved = fixture
                .commands
                .save_function(&call, "answer", "Answer", SOURCE)
                .await
                .unwrap();
            index.settled().await;
            let attempted_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let retry_after = attempted_at + std::time::Duration::from_mins(5).as_nanos();
            fixture
                .test_database
                .execute_fixture_sql(&format!(
                    "INSERT INTO search_index_states (workspace_id, target_id, kind, \
                     saved_function_id, principal_id, principal_kind, state, \
                     attempted_at_unix_nanos, error, failed_attempts, \
                     retry_after_unix_nanos, failed_coral_version, \
                     failed_saved_function_revision) \
                     VALUES ('{workspace}', '{function}', 'saved_function', '{function}', \
                     '{principal}', 'user', 'failed', {attempted_at}, '{INJECTED_FAULT}', 1, \
                     {retry_after}, '{version}', {revision})",
                    workspace = fixture.workspace,
                    function = saved.id(),
                    principal = fixture.actor.principal().id(),
                    version = env!("CARGO_PKG_VERSION"),
                    revision = saved.revision(),
                ))
                .await
                .expect("leave a failed state beside the row");
        } else {
            reject_function_rows(&fixture).await;
            fixture
                .commands
                .save_function(&call, "answer", "Answer", SOURCE)
                .await
                .expect("a failed index write never fails the save");
            index.settled().await;
            allow_function_rows(&fixture).await;
        }
        let failed = function_states(&fixture).await;
        assert_eq!(
            failed
                .iter()
                .map(|state| state.state.as_str())
                .collect::<Vec<_>>(),
            ["failed"]
        );
        let paths = indexed_paths(&fixture).await;

        index.reconcile().await;
        assert_eq!(
            (
                function_states(&fixture).await,
                indexed_paths(&fixture).await
            ),
            (failed, paths),
            "a failure whose retry is not due is left alone; row beside failure: \
             {row_beside_failure}"
        );

        make_retries_due(&fixture).await;
        index.reconcile().await;
        assert_eq!(indexed_paths(&fixture).await, ["answer"]);
        assert!(function_states(&fixture).await.is_empty());
    }
}

#[tokio::test]
async fn a_function_whose_write_overran_its_limit_waits_for_a_change() {
    // A newer Coral version, a row written before Coral recorded one, or a
    // failure of another revision, such as one whose save notification was
    // lost, each make the failure due.
    for change in [
        "failed_coral_version = '0.0.0'",
        "failed_coral_version = NULL",
        "failed_saved_function_revision = 0",
    ] {
        let fixture = Fixture::new().await;
        let goal = fixture.goal().await;
        let call = fixture.call(&goal, "Save a function");
        let saved = fixture
            .commands
            .save_function(&call, "answer", "Answer", SOURCE)
            .await
            .unwrap();
        fixture.index.settled().await;
        let stalled = fixture
            .index
            .clone()
            .with_write_limit(std::time::Duration::from_millis(100))
            .with_stall_after_lock(std::time::Duration::from_hours(1));
        stalled.index_function(coral_functions::SavedFunctionEntry::from_saved(&saved));
        stalled.settled().await;
        let failed = function_states(&fixture).await;
        let [state] = failed.as_slice() else {
            panic!("one state row: {failed:?}");
        };
        assert_eq!(state.state, "failed");
        assert!(state.error.contains("ran past its"), "{}", state.error);

        make_retries_due(&fixture).await;
        fixture.index.reconcile().await;
        assert_eq!(
            (
                function_states(&fixture).await,
                indexed_paths(&fixture).await
            ),
            (failed, Vec::<String>::new()),
            "a pass under the same Coral version leaves the failure"
        );

        fixture
            .test_database
            .execute_fixture_sql(&format!("UPDATE search_index_states SET {change}"))
            .await
            .expect("change what the failure recorded");
        fixture.index.reconcile().await;
        assert_eq!(indexed_paths(&fixture).await, ["answer"], "{change}");
        assert!(function_states(&fixture).await.is_empty(), "{change}");
    }
}

#[tokio::test]
async fn the_task_commands_save_indexes_the_function_and_a_save_over_keeps_one_row() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Save a function");

    let first = fixture
        .commands
        .save_function(&call, "triage.openIssues", "Find open issues", SOURCE)
        .await
        .unwrap();
    fixture.index.settled().await;
    let user = fixture
        .actor
        .authenticated_user()
        .unwrap()
        .user_id()
        .to_string();
    assert_eq!(
        function_rows(&fixture).await,
        [FunctionRow {
            doc_id: first.id().to_string(),
            saved_function_id: first.id().to_string(),
            principal_id: user.clone(),
            principal_kind: "user".to_string(),
            call_path: "coral.functions.triage.openIssues".to_string(),
            qualified_name: "triage.openIssues".to_string(),
            title: String::new(),
            description: "Find open issues".to_string(),
            provider_name: String::new(),
            searchable_text: "input label string result find number triageopenissues".to_string(),
            deprecated: false,
            fingerprint: "1.1".to_string(),
        }]
    );
    let found = fixture
        .commands
        .search(
            &fixture.call(&goal, "Find the function"),
            coral_search::SearchInput {
                query: Some("open issues".into()),
                ..coral_search::SearchInput::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        found
            .hits()
            .iter()
            .map(|hit| hit.call_path().as_str())
            .collect::<Vec<_>>(),
        ["coral.functions.triage.openIssues"],
        "search finds the save with no reconcile pass"
    );

    let second = fixture
        .commands
        .save_function(&call, "triage.openIssues", "Find open bugs", SOURCE)
        .await
        .unwrap();
    assert_eq!(second.id(), first.id(), "a save over keeps the identity");
    fixture.index.settled().await;
    assert_eq!(
        function_rows(&fixture)
            .await
            .into_iter()
            .map(|row| (row.doc_id, row.description, row.fingerprint))
            .collect::<Vec<_>>(),
        [(
            first.id().to_string(),
            "Find open bugs".to_string(),
            "2.1".to_string()
        )]
    );
}

#[tokio::test]
async fn a_failed_index_write_keeps_the_save_and_leaves_only_a_failed_state_until_a_later_save() {
    // The save commits and answers whatever the index does. A first attempt
    // rolls back its own row; a failed attempt over an indexed function also
    // removes the earlier row, because the search table holds only what
    // indexed.
    for indexed_before in [false, true] {
        let fixture = Fixture::new().await;
        let index = &fixture.index;
        let goal = fixture.goal().await;
        let call = fixture.call(&goal, "Save a function");
        let path = coral_functions::SavedFunctionPath::parse("answer").unwrap();
        if indexed_before {
            fixture
                .commands
                .save_function(&call, path.as_str(), "Answer", SOURCE)
                .await
                .unwrap();
            index.settled().await;
            assert_eq!(function_rows(&fixture).await.len(), 1);
        }
        reject_function_rows(&fixture).await;

        let logs = CapturedLogs::default();
        let saved = {
            let _subscriber = tracing::subscriber::set_default(
                tracing_subscriber::Registry::default().with(logs.clone()),
            );
            fixture
                .commands
                .save_function(&call, path.as_str(), "Answer", SOURCE)
                .await
        }
        .expect("a failed index write never fails the save");
        index.settled().await;

        assert_eq!(
            fixture
                .functions
                .get(&fixture.actor, &fixture.workspace, &path)
                .await
                .unwrap(),
            saved,
            "the save committed"
        );
        assert!(
            function_rows(&fixture).await.is_empty(),
            "indexed before: {indexed_before}"
        );
        let [state] = function_states(&fixture)
            .await
            .try_into()
            .expect("one state row");
        let user = fixture
            .actor
            .authenticated_user()
            .unwrap()
            .user_id()
            .to_string();
        assert_eq!(
            (
                state.saved_function_id.as_str(),
                state.principal_id.as_str(),
                state.principal_kind.as_str(),
                state.state.as_str()
            ),
            (
                saved.id().to_string().as_str(),
                user.as_str(),
                "user",
                "failed"
            )
        );
        assert!(state.error.contains(INJECTED_FAULT), "{}", state.error);
        let [event] = logs.events().try_into().expect("the write's error");
        assert_eq!(event.level, tracing::Level::ERROR);
        for (field, value) in [
            ("coral.workspace.id", fixture.workspace.to_string()),
            ("coral.principal.id", user.clone()),
            ("coral.saved_function.id", saved.id().to_string()),
            ("error.type", "\"database\"".to_string()),
        ] {
            assert_eq!(event.fields.get(field), Some(&value), "{field}");
        }

        allow_function_rows(&fixture).await;
        fixture
            .commands
            .save_function(&call, path.as_str(), "Answer", SOURCE)
            .await
            .unwrap();
        index.settled().await;
        assert_eq!(function_rows(&fixture).await.len(), 1);
        assert!(function_states(&fixture).await.is_empty());
    }
}

#[tokio::test]
async fn deleting_a_function_through_the_task_command_removes_its_row() {
    let fixture = Fixture::new().await;
    let index = &fixture.index;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Save and delete");
    for path in ["answer", "kept"] {
        fixture
            .commands
            .save_function(&call, path, "Answer", SOURCE)
            .await
            .unwrap();
    }
    index.settled().await;

    fixture
        .commands
        .delete_function(&call, "answer")
        .await
        .unwrap();

    assert_eq!(indexed_paths(&fixture).await, ["kept"]);
}

#[tokio::test]
async fn removing_access_removes_the_principals_rows_with_their_functions() {
    let fixture = Fixture::new().await;
    let index = &fixture.index;
    let owner = fixture.actor.authenticated_user().unwrap().clone();
    let change = ChangeContext::new(owner, 30, coral_test_support::trace('1'));
    let member = provision_user(
        fixture.tasks.database.clone(),
        "user:indexed-member",
        OrganizationRole::Member,
    )
    .await;
    fixture
        .tasks
        .workspaces
        .set_membership(
            &change,
            &fixture.workspace,
            &member,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let issued = fixture
        .tasks
        .workspaces
        .create_agent(
            &change,
            &fixture.workspace,
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = fixture
        .tasks
        .workspaces
        .authenticate_agent(&fixture.workspace, issued.credential.expose(), 40)
        .await
        .unwrap();
    let mut calls = Vec::new();
    for (actor, path) in [
        (fixture.actor.clone(), "owned"),
        (AuthenticatedPrincipal::user(member.clone()), "membered"),
        (agent.clone(), "automated"),
    ] {
        let call = principal_call(&fixture, actor).await;
        fixture
            .commands
            .save_function(&call, path, "Private work", SOURCE)
            .await
            .unwrap();
        calls.push(call);
    }
    index.settled().await;
    // Each Principal also owns a function whose last attempt failed, so its
    // state row must go with its functions too.
    reject_function_rows(&fixture).await;
    for call in &calls {
        fixture
            .commands
            .save_function(call, "failing", "Private work", SOURCE)
            .await
            .unwrap();
    }
    index.settled().await;
    let owner_id = fixture.actor.principal().id().to_string();
    let (member_id, agent_id) = (
        member.user_id().to_string(),
        agent.principal().id().to_string(),
    );
    let mut failed_principals = vec![owner_id.clone(), member_id, agent_id.clone()];
    failed_principals.sort();
    assert_eq!(
        indexed_paths(&fixture).await,
        ["automated", "membered", "owned"]
    );
    assert_eq!(state_principals(&fixture).await, failed_principals);

    let lifecycle = coral_administration::AccessLifecycleModule::new(
        fixture.tasks.database.clone(),
        fixture.tasks.workspaces.clone(),
    );
    lifecycle
        .remove_workspace_member(&change, &fixture.workspace, member.user_id())
        .await
        .unwrap();
    assert_eq!(indexed_paths(&fixture).await, ["automated", "owned"]);
    let mut remaining = vec![owner_id.clone(), agent_id];
    remaining.sort();
    assert_eq!(state_principals(&fixture).await, remaining);

    lifecycle
        .delete_agent(&change, &fixture.workspace, agent.principal().id())
        .await
        .unwrap();
    assert_eq!(indexed_paths(&fixture).await, ["owned"]);
    assert_eq!(state_principals(&fixture).await, [owner_id]);
}

#[tokio::test]
async fn a_function_without_projected_declaration_fields_is_indexed_by_path_and_description() {
    let fixture = Fixture::new().await;
    let index = &fixture.index;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Save a large function");
    // More declaration fields than the projection bound allows: the save is
    // accepted and its projection fails.
    let mut fields = String::new();
    for field in 0..4_200 {
        write!(fields, "a{field}:0;").expect("write field fixture");
    }
    let saved = fixture
        .commands
        .save_function(
            &call,
            "largeProjection",
            "Large searchable metadata",
            &format!(
                "interface X{{{fields}}} export default function f(input:X):number{{return 1}}"
            ),
        )
        .await
        .unwrap();
    assert!(matches!(
        saved.declaration_field_projection(),
        coral_functions::SavedFunctionDeclarationFieldProjection::Failed(_)
    ));
    index.settled().await;

    // A path without separators has no compact variant to add.
    assert_eq!(
        function_rows(&fixture)
            .await
            .into_iter()
            .map(|row| (row.qualified_name, row.description, row.searchable_text))
            .collect::<Vec<_>>(),
        [(
            "largeProjection".to_string(),
            "Large searchable metadata".to_string(),
            String::new()
        )]
    );
}

#[tokio::test]
async fn postgres_statistics_follow_each_function_write() {
    coral_test_support::database_only!(Postgres);
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Save a function");
    fixture
        .commands
        .save_function(&call, "triage.openIssues", "Find open issues", SOURCE)
        .await
        .unwrap();
    fixture.index.settled().await;
    assert!(has_term(&fixture, "triageopenissues").await);

    // A failed attempt removes the row and, with it, the row's terms.
    reject_function_rows(&fixture).await;
    fixture
        .commands
        .save_function(&call, "triage.openIssues", "Find open bugs", SOURCE)
        .await
        .unwrap();
    fixture.index.settled().await;
    assert!(!has_term(&fixture, "triageopenissues").await);
}

const SOURCE: &str = "export default function find(input: { label: string }): number { return 1; }";

type FunctionColumns = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    bool,
    String,
);

#[derive(Debug, PartialEq, Eq)]
struct FunctionRow {
    doc_id: String,
    saved_function_id: String,
    principal_id: String,
    principal_kind: String,
    call_path: String,
    qualified_name: String,
    title: String,
    description: String,
    provider_name: String,
    searchable_text: String,
    deprecated: bool,
    fingerprint: String,
}

/// Whether the fixture Workspace's Postgres term statistics hold `term`.
async fn has_term(fixture: &Fixture, term: &str) -> bool {
    let row: Option<(String,)> = fixture
        .tasks
        .database
        .fetch_optional(
            Query::select()
                .column(Alias::new("term"))
                .from(Alias::new("search_terms"))
                .and_where(Expr::col(Alias::new("workspace_id")).eq(fixture.workspace.to_string()))
                .and_where(Expr::col(Alias::new("term")).eq(term))
                .to_owned(),
        )
        .await
        .expect("read the term statistics");
    row.is_some()
}

/// A call attributed to `actor` under a Task of its own.
async fn principal_call(fixture: &Fixture, actor: AuthenticatedPrincipal) -> AttributedCommand {
    let goal = fixture
        .tasks
        .start(
            &actor,
            &fixture.workspace,
            TaskIntent::parse("Save work").unwrap(),
            50,
        )
        .await
        .unwrap();
    AttributedCommand::new(
        actor,
        fixture.workspace.clone(),
        goal.id().clone(),
        TaskIntent::parse("Save a function").unwrap(),
        coral_test_support::trace('1'),
    )
}

/// The Principals of the fixture Workspace's Saved Function state rows.
async fn state_principals(fixture: &Fixture) -> Vec<String> {
    let mut principals = function_states(fixture)
        .await
        .into_iter()
        .map(|state| state.principal_id)
        .collect::<Vec<_>>();
    principals.sort();
    principals
}

/// The qualified names of the fixture Workspace's Saved Function rows.
async fn indexed_paths(fixture: &Fixture) -> Vec<String> {
    let mut paths = function_rows(fixture)
        .await
        .into_iter()
        .map(|row| row.qualified_name)
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

/// The fixture Workspace's Saved Function rows, in document order.
async fn function_rows(fixture: &Fixture) -> Vec<FunctionRow> {
    let rows: Vec<FunctionColumns> = fixture
        .tasks
        .database
        .fetch_all(
            Query::select()
                .columns(
                    [
                        "doc_id",
                        "saved_function_id",
                        "principal_id",
                        "principal_kind",
                        "call_path",
                        "qualified_name",
                        "title",
                        "description",
                        "provider_name",
                        "searchable_text",
                        "deprecated",
                        "fingerprint",
                    ]
                    .map(Alias::new),
                )
                .from(Alias::new("search_entities"))
                .and_where(Expr::col(Alias::new("workspace_id")).eq(fixture.workspace.to_string()))
                .and_where(Expr::col(Alias::new("kind")).eq("saved_function"))
                .order_by(Alias::new("doc_id"), Order::Asc)
                .to_owned(),
        )
        .await
        .expect("read the Saved Function rows");
    rows.into_iter()
        .map(
            |(
                doc_id,
                saved_function_id,
                principal_id,
                principal_kind,
                call_path,
                qualified_name,
                title,
                description,
                provider_name,
                searchable_text,
                deprecated,
                fingerprint,
            )| FunctionRow {
                doc_id,
                saved_function_id,
                principal_id,
                principal_kind,
                call_path,
                qualified_name,
                title,
                description,
                provider_name,
                searchable_text,
                deprecated,
                fingerprint,
            },
        )
        .collect()
}

#[derive(Debug, PartialEq, Eq)]
struct FunctionState {
    saved_function_id: String,
    principal_id: String,
    principal_kind: String,
    state: String,
    error: String,
}

/// The fixture Workspace's Saved Function state rows.
async fn function_states(fixture: &Fixture) -> Vec<FunctionState> {
    let rows: Vec<(String, String, String, String, String)> = fixture
        .tasks
        .database
        .fetch_all(
            Query::select()
                .columns(
                    [
                        "saved_function_id",
                        "principal_id",
                        "principal_kind",
                        "state",
                        "error",
                    ]
                    .map(Alias::new),
                )
                .from(Alias::new("search_index_states"))
                .and_where(Expr::col(Alias::new("workspace_id")).eq(fixture.workspace.to_string()))
                .and_where(Expr::col(Alias::new("kind")).eq("saved_function"))
                .order_by(Alias::new("target_id"), Order::Asc)
                .to_owned(),
        )
        .await
        .expect("read the Saved Function states");
    rows.into_iter()
        .map(
            |(saved_function_id, principal_id, principal_kind, state, error)| FunctionState {
                saved_function_id,
                principal_id,
                principal_kind,
                state,
                error,
            },
        )
        .collect()
}

/// Makes every Saved Function row fail to write, insert or upsert alike.
async fn reject_function_rows(fixture: &Fixture) {
    let sql = match fixture.tasks.database.system() {
        coral_db::DatabaseSystem::Sqlite => format!(
            "CREATE TRIGGER inject_index_fault BEFORE INSERT ON search_entities \
             WHEN NEW.kind = 'saved_function' \
             BEGIN SELECT RAISE(ABORT, '{INJECTED_FAULT}'); END;"
        ),
        coral_db::DatabaseSystem::Postgres => format!(
            "CREATE FUNCTION inject_index_fault() RETURNS trigger LANGUAGE plpgsql \
             AS $$ BEGIN RAISE EXCEPTION '{INJECTED_FAULT}'; END $$; \
             CREATE TRIGGER inject_index_fault BEFORE INSERT ON search_entities \
             FOR EACH ROW WHEN (NEW.kind = 'saved_function') \
             EXECUTE FUNCTION inject_index_fault();"
        ),
    };
    fixture
        .test_database
        .execute_fixture_sql(&sql)
        .await
        .expect("install the index fault");
}

/// Deletes every Saved Function row, as a notification that never arrived
/// leaves the index.
async fn lose_function_rows(fixture: &Fixture) {
    fixture
        .test_database
        .execute_fixture_sql("DELETE FROM search_entities WHERE kind = 'saved_function'")
        .await
        .expect("delete the Saved Function rows");
}

/// Moves every transient failure's retry time into the past.
async fn make_retries_due(fixture: &Fixture) {
    fixture
        .test_database
        .execute_fixture_sql(
            "UPDATE search_index_states SET retry_after_unix_nanos = 0 \
             WHERE retry_after_unix_nanos IS NOT NULL",
        )
        .await
        .expect("make the retries due");
}

/// Removes the fault [`reject_function_rows`] installed.
async fn allow_function_rows(fixture: &Fixture) {
    let sql = match fixture.tasks.database.system() {
        coral_db::DatabaseSystem::Sqlite => "DROP TRIGGER inject_index_fault",
        coral_db::DatabaseSystem::Postgres => "DROP TRIGGER inject_index_fault ON search_entities",
    };
    fixture
        .test_database
        .execute_fixture_sql(sql)
        .await
        .expect("remove the index fault");
}

/// When each of the fixture Workspace's Saved Function rows was last
/// written, by document id.
async fn indexed_at(fixture: &Fixture) -> Vec<(String, i64)> {
    fixture
        .tasks
        .database
        .fetch_all(
            Query::select()
                .columns(["doc_id", "indexed_at_unix_nanos"].map(Alias::new))
                .from(Alias::new("search_entities"))
                .and_where(Expr::col(Alias::new("workspace_id")).eq(fixture.workspace.to_string()))
                .and_where(Expr::col(Alias::new("kind")).eq("saved_function"))
                .order_by(Alias::new("doc_id"), Order::Asc)
                .to_owned(),
        )
        .await
        .expect("read the Saved Function row stamps")
}

/// Records every event of the search crate with its level and fields.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<CapturedEvent>>>);

#[derive(Debug)]
struct CapturedEvent {
    level: tracing::Level,
    fields: BTreeMap<String, String>,
}

impl CapturedLogs {
    fn events(&self) -> Vec<CapturedEvent> {
        std::mem::take(&mut *self.0.lock().expect("captured logs"))
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CapturedLogs {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if !event.metadata().target().starts_with("coral_search") {
            return;
        }
        let mut fields = FieldRecorder::default();
        event.record(&mut fields);
        self.0.lock().expect("captured logs").push(CapturedEvent {
            level: *event.metadata().level(),
            fields: fields.0,
        });
    }
}

#[derive(Default)]
struct FieldRecorder(BTreeMap<String, String>);

impl tracing::field::Visit for FieldRecorder {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}
