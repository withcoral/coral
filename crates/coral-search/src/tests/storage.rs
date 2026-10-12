//! Search-owned storage at boot: the backend stream, its ledger, and drift.

use coral_db::{Database, DatabaseSystem};
use coral_functions::{SavedFunctionChangeContext, SavedFunctionPath};
use coral_test_support::TestDatabase;
use coral_workspace::{AuthenticatedPrincipal, AuthenticatedUser, WorkspaceId};
use sea_query::{Alias, Expr, ExprTrait as _, InsertStatement, OnConflict, Order, Query};
use wiremock::MockServer;

use super::{TestModules, organization_user, trace};
use crate::{StorageError, prepare_storage};

#[tokio::test]
async fn a_stream_step_runs_once_across_two_boots() {
    let fixture = TestDatabase::new(&super::test_migrator()).await.unwrap();
    let database = fixture.database();

    prepare_storage(&database).await.expect("first boot");
    prepare_storage(&database)
        .await
        .expect("second boot must not run the step again");

    assert_eq!(ledger(&database).await, every_version(&database));
}

#[tokio::test]
async fn a_recorded_stream_version_newer_than_the_binary_refuses_boot() {
    let fixture = TestDatabase::new(&super::test_migrator()).await.unwrap();
    let database = fixture.database();
    prepare_storage(&database).await.expect("first boot");
    let supported = supported_version(&database);
    let newer = supported + 1;
    fixture
        .execute_fixture_sql(&format!(
            "INSERT INTO search_migrations (stream, version, applied_at_unix_nanos) \
             VALUES ('{}', {newer}, 0)",
            stream(&database)
        ))
        .await
        .expect("record a newer stream version");

    let error = prepare_storage(&database)
        .await
        .expect_err("a newer search schema must refuse boot");

    assert!(
        matches!(
            error,
            StorageError::UnsupportedStreamVersion { recorded, supported: refused_at, .. }
                if recorded == newer && refused_at == supported
        ),
        "unexpected error: {error}"
    );
    let message = error.to_string();
    assert!(message.contains(&format!("version {newer}")), "{message}");
    assert!(
        message.contains(&format!("version {supported}")),
        "{message}"
    );
    let mut expected = every_version(&database);
    expected.push((stream(&database), newer));
    assert_eq!(
        ledger(&database).await,
        expected,
        "a refused boot records nothing"
    );
}

#[tokio::test]
async fn an_object_that_exists_before_the_stream_fails_the_step_and_records_nothing() {
    let fixture = TestDatabase::new(&super::test_migrator()).await.unwrap();
    let database = fixture.database();
    let drift = match database.system() {
        DatabaseSystem::Sqlite => "search_entities_fts",
        DatabaseSystem::Postgres => "search_terms",
    };
    fixture
        .execute_fixture_sql(&format!("CREATE TABLE {drift} (id TEXT NOT NULL)"))
        .await
        .expect("create an object the stream owns");

    let error = prepare_storage(&database)
        .await
        .expect_err("an existing stream object must fail the step, not be recorded");

    assert!(
        matches!(error, StorageError::StreamStep { version: 1, .. }),
        "unexpected error: {error}"
    );
    assert!(
        error.to_string().contains("already exists"),
        "the step must fail on the existing object: {error}"
    );
    assert_eq!(ledger(&database).await, Vec::new());

    fixture
        .execute_fixture_sql(&format!("DROP TABLE {drift}"))
        .await
        .expect("remove the drifted object");
    prepare_storage(&database)
        .await
        .expect("the failed step left nothing behind, so it now runs in full");
    assert_eq!(ledger(&database).await, every_version(&database));
}

#[tokio::test]
async fn sqlite_triggers_keep_the_text_index_in_step_with_search_entities() {
    coral_test_support::database_only!(Sqlite);
    let upstream = MockServer::start().await;
    let tracer = Tracer::publish(&upstream).await;
    let database = tracer.modules.search.database.clone();
    let doc = tracer.operation_doc();

    tracer.upsert_operation_row("Daily outlook").await;
    assert_eq!(
        text_matches(&database, &tracer.workspace, "outlook").await,
        std::slice::from_ref(&doc)
    );
    assert_eq!(
        text_index_provider_name(&database, &doc).await,
        "weather Weather Tracer",
        "the Provider name is name-like and loses its dots"
    );

    tracer.upsert_operation_row("Weekly summary").await;
    assert!(
        text_matches(&database, &tracer.workspace, "outlook")
            .await
            .is_empty()
    );
    assert_eq!(
        text_matches(&database, &tracer.workspace, "summary").await,
        std::slice::from_ref(&doc)
    );

    database
        .execute(
            Query::delete()
                .from_table(Alias::new("search_entities"))
                .and_where(Expr::col(Alias::new("doc_id")).eq(doc))
                .to_owned(),
        )
        .await
        .expect("delete the Operation row");
    assert!(
        text_matches(&database, &tracer.workspace, "summary")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn sqlite_text_index_follows_a_saved_function_delete_cascade() {
    coral_test_support::database_only!(Sqlite);
    let upstream = MockServer::start().await;
    let tracer = Tracer::publish(&upstream).await;
    let database = tracer.modules.search.database.clone();
    let principal = AuthenticatedPrincipal::user(tracer.user.clone());
    let saved = tracer
        .modules
        .save_tracer(
            &principal,
            &tracer.workspace,
            "Format one weather forecast",
            40,
        )
        .await;
    let doc = saved.id().to_string();
    database
        .execute(saved_function_row(&tracer.workspace, &doc, Some("user")))
        .await
        .expect("insert the Saved Function row");
    assert_eq!(
        text_matches(&database, &tracer.workspace, "forecast").await,
        [doc]
    );

    tracer
        .modules
        .saved_functions
        .delete(
            &SavedFunctionChangeContext::new(principal, 50, trace('5')),
            &tracer.workspace,
            &SavedFunctionPath::parse("internal.formatForecast").unwrap(),
        )
        .await
        .expect("delete the Saved Function");

    assert!(
        text_matches(&database, &tracer.workspace, "forecast")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn saved_function_rows_without_a_principal_kind_fail_their_check() {
    let upstream = MockServer::start().await;
    let tracer = Tracer::publish(&upstream).await;
    let database = tracer.modules.search.database.clone();
    let saved = tracer
        .modules
        .save_tracer(
            &AuthenticatedPrincipal::user(tracer.user.clone()),
            &tracer.workspace,
            "Format one weather forecast",
            40,
        )
        .await;
    let doc = saved.id().to_string();

    for (table, without_kind, with_kind) in [
        (
            "search_entities",
            saved_function_row(&tracer.workspace, &doc, None),
            saved_function_row(&tracer.workspace, &doc, Some("user")),
        ),
        (
            "search_index_states",
            failed_function_state(&tracer.workspace, &doc, None),
            failed_function_state(&tracer.workspace, &doc, Some("user")),
        ),
    ] {
        let error = database
            .execute(without_kind)
            .await
            .expect_err("a Saved Function row needs its Principal kind");
        assert!(
            matches!(
                &error,
                coral_db::Error::Sqlx(sqlx::Error::Database(error))
                    if error.kind() == sqlx::error::ErrorKind::CheckViolation
            ),
            "{table}: expected a check violation, got {error}"
        );
        database
            .execute(with_kind)
            .await
            .unwrap_or_else(|error| panic!("{table}: the same row with a kind is valid: {error}"));
    }
}

#[tokio::test]
async fn postgres_vector_weights_each_field_and_splits_names_by_dots_and_camel_case() {
    coral_test_support::database_only!(Postgres);
    let upstream = MockServer::start().await;
    let tracer = Tracer::publish(&upstream).await;
    let database = tracer.modules.search.database.clone();
    tracer.upsert_operation_row("GitHub Outlook.report").await;

    for (query, matches) in [
        ("daily:A", true),
        ("high:A", true),
        ("report:B", true),
        ("linear.issues:C", true),
        ("tracer:C", true),
        ("city:D", true),
        ("daily:B", false),
        ("issues", false),
        // camelCase splits only in the name: `GitHub` stays one word in the
        // title, the description and the schema text.
        ("github:B", true),
        ("github:C", true),
        ("githubtoken:D", true),
        ("hub", false),
    ] {
        assert_eq!(
            vector_matches(&database, &tracer.operation_doc(), query).await,
            matches,
            "{query}"
        );
    }
}

/// A camelCase name and a name of one word.
const NAMES: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Tracker", "version": "1.0.0" },
  "servers": [{ "url": "https://tracker.example" }],
  "security": [],
  "paths": {
    "/c": { "get": { "operationId": "listComments", "summary": "Read the notes",
      "responses": { "200": { "description": "OK" } } } },
    "/r": { "get": { "operationId": "create", "summary": "Add a note",
      "responses": { "200": { "description": "OK" } } } }
  }
}"#;

#[tokio::test]
async fn postgres_step_two_splits_indexed_names_and_restates_lengths_and_statistics() {
    coral_test_support::database_only!(Postgres);
    let modules = TestModules::with_migrator(&super::test_migrator()).await;
    let database = modules.search.database.clone();
    crate::storage::prepare_storage_through(&database, 1)
        .await
        .expect("boot at stream version 1");
    let user = organization_user(database.clone(), USER).await;
    let workspace = modules.create_workspace(&user).await;
    let provider = modules
        .publish_openapi(&user, &workspace, NAMES, "tracker", 20)
        .await;
    modules.converge().await;
    let generation = active_generation(&database, &provider).await;
    let doc = |key: &str| format!("{generation}:{key}");
    assert!(!vector_matches(&database, &doc("listComments"), "comments:A").await);
    let indexed = Statistics::read(&database, &workspace).await;

    prepare_storage(&database).await.expect("apply step 2");

    assert!(vector_matches(&database, &doc("listComments"), "comments:A").await);
    let upgraded = Statistics::read(&database, &workspace).await;
    assert_eq!(
        upgraded.length(&doc("listComments")) - indexed.length(&doc("listComments")),
        1,
        "`listComments` is two name words now"
    );
    assert_eq!(
        upgraded.length(&doc("create")),
        indexed.length(&doc("create"))
    );

    // Index the Generation again: the indexer writes lengths and statistics
    // for the new vectors by its own rules.
    let mut transaction = database.begin_read_snapshot().await.unwrap();
    let entries = coral_provider::prepare_active_generation_entries_in_transaction(
        &mut transaction,
        &workspace,
        &provider,
    )
    .await
    .unwrap()
    .expect("active Generation");
    transaction.rollback().await.unwrap();
    modules.index.index_generation(std::sync::Arc::new(entries));
    modules.index.settled().await;
    assert_eq!(
        Statistics::read(&database, &workspace).await,
        upgraded,
        "the step leaves what the indexer writes"
    );
}

/// One Workspace's Postgres row lengths and ranking statistics.
#[derive(Debug, PartialEq)]
struct Statistics {
    lengths: Vec<(String, i32)>,
    terms: Vec<(String, i32)>,
    totals: Vec<(i64, f64)>,
}

impl Statistics {
    async fn read(database: &Database, workspace: &WorkspaceId) -> Self {
        let workspace = workspace.to_string();
        let lengths = database
            .fetch_all(
                Query::select()
                    .columns([Alias::new("doc_id"), Alias::new("doc_len")])
                    .from(Alias::new("search_entities"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(workspace.clone()))
                    .order_by(Alias::new("doc_id"), Order::Asc)
                    .to_owned(),
            )
            .await
            .expect("read row lengths");
        let terms = database
            .fetch_all(
                Query::select()
                    .columns([Alias::new("term"), Alias::new("ndoc")])
                    .from(Alias::new("search_terms"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(workspace.clone()))
                    .order_by(Alias::new("term"), Order::Asc)
                    .to_owned(),
            )
            .await
            .expect("read term statistics");
        let totals = database
            .fetch_all(
                Query::select()
                    .columns([Alias::new("total_docs"), Alias::new("avgdl")])
                    .from(Alias::new("search_stats"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(workspace))
                    .to_owned(),
            )
            .await
            .expect("read Workspace statistics");
        Self {
            lengths,
            terms,
            totals,
        }
    }

    fn length(&self, doc: &str) -> i32 {
        self.lengths
            .iter()
            .find(|(id, _length)| id == doc)
            .map_or_else(|| panic!("a row {doc}"), |(_id, length)| *length)
    }
}

async fn active_generation(database: &Database, provider: &coral_provider::ProviderId) -> String {
    let (generation,): (String,) = database
        .fetch_optional(
            Query::select()
                .column(Alias::new("active_generation_id"))
                .from(Alias::new("provider_records"))
                .and_where(Expr::col(Alias::new("id")).eq(provider.to_string()))
                .to_owned(),
        )
        .await
        .unwrap()
        .expect("published Provider");
    generation
}

const USER: &str = "user:search-storage";

/// A Workspace with one published Provider, whose rows the tests write by hand
/// because no indexer exists yet.
struct Tracer {
    modules: TestModules,
    user: AuthenticatedUser,
    workspace: WorkspaceId,
    provider_id: String,
    generation_id: String,
}

impl Tracer {
    async fn publish(upstream: &MockServer) -> Self {
        let modules = TestModules::new().await;
        let user = organization_user(modules.search.database.clone(), USER).await;
        let workspace = modules.create_workspace(&user).await;
        let published = modules
            .publish_tracer(&user, &workspace, &upstream.uri())
            .await;
        let provider_id = published.provider_id.to_string();
        let (generation_id,): (String,) = modules
            .search
            .database
            .fetch_optional(
                Query::select()
                    .column(Alias::new("active_generation_id"))
                    .from(Alias::new("provider_records"))
                    .and_where(Expr::col(Alias::new("id")).eq(provider_id.clone()))
                    .to_owned(),
            )
            .await
            .unwrap()
            .expect("published Provider");
        Self {
            modules,
            user,
            workspace,
            provider_id,
            generation_id,
        }
    }

    fn operation_doc(&self) -> String {
        format!("{}:forecast", self.generation_id)
    }

    /// Writes the Operation row the way the indexer will: an upsert by key.
    async fn upsert_operation_row(&self, title: &str) {
        self.modules
            .search
            .database
            .execute(
                Query::insert()
                    .into_table(Alias::new("search_entities"))
                    .columns(row_columns())
                    .values_panic([
                        self.workspace.to_string().into(),
                        self.operation_doc().into(),
                        "operation".into(),
                        Some(self.provider_id.clone()).into(),
                        Some(self.generation_id.clone()).into(),
                        Some("forecast".to_string()).into(),
                        Option::<String>::None.into(),
                        Option::<String>::None.into(),
                        Option::<String>::None.into(),
                        "coral.providers.weather.forecast.daily".into(),
                        "forecast.dailyHigh".into(),
                        title.into(),
                        "Mirrors linear.issues from GitHub".into(),
                        "weather Weather.Tracer".into(),
                        "input.city\ninput.gitHubToken".into(),
                        false.into(),
                        self.generation_id.clone().into(),
                        1_i64.into(),
                    ])
                    .on_conflict(
                        OnConflict::columns([Alias::new("workspace_id"), Alias::new("doc_id")])
                            .update_columns([Alias::new("title")])
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await
            .expect("upsert the Operation row");
    }
}

fn saved_function_row(
    workspace: &WorkspaceId,
    doc: &str,
    principal_kind: Option<&str>,
) -> InsertStatement {
    Query::insert()
        .into_table(Alias::new("search_entities"))
        .columns(row_columns())
        .values_panic([
            workspace.to_string().into(),
            doc.into(),
            "saved_function".into(),
            Option::<String>::None.into(),
            Option::<String>::None.into(),
            Option::<String>::None.into(),
            Some(doc.to_string()).into(),
            Some(USER.to_string()).into(),
            principal_kind.map(str::to_string).into(),
            "coral.functions.internal.formatForecast".into(),
            "internal.formatForecast".into(),
            "".into(),
            "Format one weather forecast".into(),
            "".into(),
            "value.temperatureC".into(),
            false.into(),
            "1".into(),
            1_i64.into(),
        ])
        .to_owned()
}

fn failed_function_state(
    workspace: &WorkspaceId,
    doc: &str,
    principal_kind: Option<&str>,
) -> InsertStatement {
    Query::insert()
        .into_table(Alias::new("search_index_states"))
        .columns(
            [
                "workspace_id",
                "target_id",
                "kind",
                "saved_function_id",
                "principal_id",
                "principal_kind",
                "state",
                "attempted_at_unix_nanos",
                "error",
            ]
            .map(Alias::new),
        )
        .values_panic([
            workspace.to_string().into(),
            doc.into(),
            "saved_function".into(),
            Some(doc.to_string()).into(),
            Some(USER.to_string()).into(),
            principal_kind.map(str::to_string).into(),
            "failed".into(),
            1_i64.into(),
            Some("indexing failed".to_string()).into(),
        ])
        .to_owned()
}

fn row_columns() -> [Alias; 18] {
    [
        "workspace_id",
        "doc_id",
        "kind",
        "provider_id",
        "provider_generation_id",
        "operation_key",
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
        "indexed_at_unix_nanos",
    ]
    .map(Alias::new)
}

/// Rows of `workspace` whose FTS5 copy matches `phrase`.
async fn text_matches(database: &Database, workspace: &WorkspaceId, phrase: &str) -> Vec<String> {
    let rows: Vec<(String,)> = database
        .fetch_all(
            Query::select()
                .column(Alias::new("doc_id"))
                .from(Alias::new("search_entities_fts"))
                .and_where(Expr::col(Alias::new("workspace_id")).eq(workspace.to_string()))
                .and_where(Expr::cust_with_values(
                    "search_entities_fts MATCH ?",
                    [format!("\"{phrase}\"")],
                ))
                .to_owned(),
        )
        .await
        .expect("query the FTS5 table");
    rows.into_iter().map(|(doc,)| doc).collect()
}

async fn text_index_provider_name(database: &Database, doc: &str) -> String {
    let (provider_name,): (String,) = database
        .fetch_optional(
            Query::select()
                .column(Alias::new("provider_name"))
                .from(Alias::new("search_entities_fts"))
                .and_where(Expr::col(Alias::new("doc_id")).eq(doc))
                .to_owned(),
        )
        .await
        .unwrap()
        .expect("FTS5 copy of the row");
    provider_name
}

async fn vector_matches(database: &Database, doc: &str, query: &str) -> bool {
    let (matches,): (bool,) = database
        .fetch_optional(
            Query::select()
                .expr(Expr::cust_with_values(
                    "tsv @@ to_tsquery('simple', $1)",
                    [query],
                ))
                .from(Alias::new("search_entities"))
                .and_where(Expr::col(Alias::new("doc_id")).eq(doc))
                .to_owned(),
        )
        .await
        .unwrap()
        .expect("the Operation row");
    matches
}

/// The newest step of each backend's stream.
fn supported_version(database: &Database) -> i64 {
    match database.system() {
        DatabaseSystem::Sqlite => 1,
        DatabaseSystem::Postgres => 2,
    }
}

/// The ledger rows of a stream applied in full.
fn every_version(database: &Database) -> Vec<(String, i64)> {
    (1..=supported_version(database))
        .map(|version| (stream(database), version))
        .collect()
}

fn stream(database: &Database) -> String {
    match database.system() {
        DatabaseSystem::Sqlite => "sqlite".to_string(),
        DatabaseSystem::Postgres => "postgres".to_string(),
    }
}

async fn ledger(database: &Database) -> Vec<(String, i64)> {
    database
        .fetch_all(
            Query::select()
                .columns([Alias::new("stream"), Alias::new("version")])
                .from(Alias::new("search_migrations"))
                .order_by(Alias::new("stream"), Order::Asc)
                .order_by(Alias::new("version"), Order::Asc)
                .to_owned(),
        )
        .await
        .expect("read the search stream ledger")
}
