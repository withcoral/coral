//! The search steps of the migration chain: the text index each backend keeps
//! beside `search_entities`, and the upgrade of a Postgres index built before
//! names split camelCase.

use coral_db::Database;
use coral_functions::{SavedFunctionChangeContext, SavedFunctionPath};
use coral_workspace::{AuthenticatedPrincipal, AuthenticatedUser, WorkspaceId};
use sea_query::{Alias, Expr, ExprTrait as _, InsertStatement, OnConflict, Order, Query};
use wiremock::MockServer;

use super::{TestModules, organization_user, trace};

#[tokio::test]
async fn sqlite_triggers_keep_the_text_index_in_step_with_search_entities() {
    coral_test_support::database_only!(Sqlite);
    let upstream = MockServer::start().await;
    let tracer = Tracer::publish(&upstream).await;
    let database = tracer.modules.search.database.clone();
    let doc = tracer.tool_doc();

    tracer.upsert_tool_row("Daily outlook").await;
    assert_eq!(
        text_matches(&database, &tracer.workspace, "outlook").await,
        std::slice::from_ref(&doc)
    );
    assert_eq!(
        text_index_provider_name(&database, &doc).await,
        "weather Weather Tracer",
        "the Provider name is name-like and loses its dots"
    );

    tracer.upsert_tool_row("Weekly summary").await;
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
        .expect("delete the Tool row");
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
async fn sqlite_key_step_keeps_one_copy_of_each_row_indexed_before_it() {
    coral_test_support::database_only!(Sqlite);
    // The steps before 39, the one under test. Later steps rename what the
    // indexer writes, so the rows are written as an indexer before 39 wrote
    // them, through the text index triggers of step 30.
    let before_step = coral_test_support::migration_plan_before(39);
    let fixture = coral_test_support::TestDatabase::new(&before_step)
        .await
        .unwrap();
    let database = fixture.database();
    let workspace = WorkspaceId::parse("7d3c1a52-8f0e-4b6a-9c41-2e5d8b0f6a13").unwrap();
    let generation = "generation";
    let comments = format!("{generation}:listComments");
    let create = format!("{generation}:create");
    index_two_rows_before_key_step(&fixture, &workspace, generation).await;
    assert_eq!(
        text_index_docs(&database, &workspace).await,
        [create.clone(), comments.clone()]
    );
    // An `INSERT OR REPLACE` leaves a second copy beside the row's own, and
    // a copy can outlive its row. The step keeps one copy of each row.
    fixture
        .execute_fixture_sql(&format!(
            "INSERT INTO search_entities_fts (workspace_id, doc_id, title) \
             VALUES ('{workspace}', '{comments}', 'Read the stale notes'), \
             ('{workspace}', '{generation}:gone', 'Read the gone notes')"
        ))
        .await
        .expect("add a second copy and a copy without a row");

    database
        .migrate(coral_db::migration_plan())
        .await
        .expect("apply the key step");

    database
        .execute(
            Query::update()
                .table(Alias::new("search_entities"))
                .value(Alias::new("title"), "Read the zebras")
                .and_where(Expr::col(Alias::new("doc_id")).eq(comments.clone()))
                .to_owned(),
        )
        .await
        .expect("update the row indexed before the step");
    assert_eq!(
        text_matches(&database, &workspace, "zebras").await,
        std::slice::from_ref(&comments)
    );
    assert!(
        text_matches(&database, &workspace, "stale")
            .await
            .is_empty()
    );
    assert_eq!(
        text_index_docs(&database, &workspace).await,
        [create.clone(), comments.clone()],
        "the update replaced the row's copy"
    );

    let rows = fixture
        .execute_fixture_sql(&format!(
            "CREATE TABLE removed AS SELECT * FROM search_entities; \
             DELETE FROM search_entities WHERE workspace_id = '{workspace}'"
        ))
        .await
        .expect("remove the rows");
    assert_eq!(rows, 2);
    assert!(
        text_index_docs(&database, &workspace).await.is_empty(),
        "the removal deleted every copy"
    );

    fixture
        .execute_fixture_sql("INSERT INTO search_entities SELECT * FROM removed")
        .await
        .expect("index the rows again");
    assert_eq!(
        text_index_docs(&database, &workspace).await,
        [create, comments],
        "the removal left no key to block the rows' return"
    );
}

/// Writes a Generation's two Search Entities as an indexer before step 39
/// wrote them, so step 30's triggers copy each into the text index.
async fn index_two_rows_before_key_step(
    fixture: &coral_test_support::TestDatabase,
    workspace: &WorkspaceId,
    generation: &str,
) {
    let entity = |key: &str, title: &str| {
        format!(
            "('{workspace}', '{generation}:{key}', 'operation', 'provider', '{generation}', \
             '{key}', 'coral.providers.tracker.{key}', 'tracker.{key}', '{title}', '{title}', \
             'tracker', '{title}', FALSE, '{key}', 1)"
        )
    };
    fixture
        .execute_fixture_sql(&format!(
            "INSERT INTO workspace_records (id, display_name, state, lifecycle_version, \
             created_at_unix_nanos, updated_at_unix_nanos) \
             VALUES ('{workspace}', 'Search', 'active', 1, 1, 1); \
             INSERT INTO provider_generations (id, input_kind, pinned_input, input_digest, \
             credential_contract, schema_graph_json, typescript_view, materializer_version, \
             created_at_unix_nanos) VALUES ('{generation}', 'openapi', '{{}}', \
             '{digest}', 'none', '{{}}', 'declare', 'v1', 1); \
             INSERT INTO search_entities (workspace_id, doc_id, kind, provider_id, \
             provider_generation_id, operation_key, call_path, qualified_name, title, \
             description, provider_name, searchable_text, deprecated, fingerprint, \
             indexed_at_unix_nanos) VALUES {listed}, {created}",
            digest = "0".repeat(64),
            listed = entity("listComments", "List the comments"),
            created = entity("create", "Create an issue"),
        ))
        .await
        .expect("index two rows");
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
    tracer.upsert_tool_row("GitHub Outlook.report").await;

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
            vector_matches(&database, &tracer.tool_doc(), query).await,
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
async fn postgres_camel_case_step_splits_indexed_names_and_restates_lengths_and_statistics() {
    coral_test_support::database_only!(Postgres);
    // Every step but 31 and 32, the two under test. This binary publishes
    // the Provider, and its repositories need every column a later step
    // outside Universal Search adds, so the plan skips only the steps the
    // test applies later. Step 33 drops the stream's ledger, which 31 and 32
    // do not read.
    let before_camel_case = coral_test_support::edited_migration_plan(|migrations| {
        migrations
            .into_iter()
            .filter(|migration| !(31..=32).contains(&migration.version))
            .collect()
    });
    let modules = TestModules::with_plan(&before_camel_case).await;
    let database = modules.search.database.clone();
    // This binary's statistics count also writes the watermark that step 32
    // adds. The columns exist only while it indexes, so the search tables
    // then have the shape a binary at step 30 left.
    modules
        .fixture
        .execute_fixture_sql(
            "ALTER TABLE search_stats \
             ADD COLUMN counted_rows BIGINT, ADD COLUMN counted_max_indexed_at BIGINT",
        )
        .await
        .expect("add the watermark columns");
    let user = organization_user(database.clone(), USER).await;
    let workspace = modules.create_workspace(&user).await;
    let provider = modules
        .publish_openapi(&user, &workspace, NAMES, "tracker", 20)
        .await;
    modules.converge().await;
    modules
        .fixture
        .execute_fixture_sql(
            "ALTER TABLE search_stats \
             DROP COLUMN counted_rows, DROP COLUMN counted_max_indexed_at",
        )
        .await
        .expect("drop the watermark columns");
    let generation = active_generation(&database, &provider).await;
    let doc = |key: &str| format!("{generation}:{key}");
    assert!(!vector_matches(&database, &doc("listComments"), "comments:A").await);
    let indexed = Statistics::read(&database, &workspace).await;

    database
        .migrate(coral_db::migration_plan())
        .await
        .expect("apply the later steps");

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

    // Index the Generation again from nothing: the indexer writes lengths
    // and statistics for the new vectors by its own rules. A rewrite over
    // the kept rows would change no row, so it would prove nothing.
    modules
        .index
        .remove_generation(coral_catalog::ProviderGenerationId::parse(&generation).unwrap());
    modules.index.settled().await;
    assert!(
        Statistics::read(&database, &workspace)
            .await
            .lengths
            .is_empty(),
        "the removal left no row"
    );
    modules
        .index
        .index_active_generation(workspace.clone(), provider.clone());
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

    fn tool_doc(&self) -> String {
        format!("{}:forecast", self.generation_id)
    }

    /// Writes the Tool row the way the indexer will: an upsert by key.
    async fn upsert_tool_row(&self, title: &str) {
        self.modules
            .search
            .database
            .execute(
                Query::insert()
                    .into_table(Alias::new("search_entities"))
                    .columns(row_columns())
                    .values_panic([
                        self.workspace.to_string().into(),
                        self.tool_doc().into(),
                        "provider_tool".into(),
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
            .expect("upsert the Tool row");
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
        "tool_key",
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

/// The doc ID of each FTS5 copy in `workspace`, sorted, with repeats.
async fn text_index_docs(database: &Database, workspace: &WorkspaceId) -> Vec<String> {
    let rows: Vec<(String,)> = database
        .fetch_all(
            Query::select()
                .column(Alias::new("doc_id"))
                .from(Alias::new("search_entities_fts"))
                .and_where(Expr::col(Alias::new("workspace_id")).eq(workspace.to_string()))
                .order_by(Alias::new("doc_id"), Order::Asc)
                .to_owned(),
        )
        .await
        .expect("read the FTS5 copies");
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
        .expect("the Tool row");
    matches
}
