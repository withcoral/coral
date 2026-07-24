//! Relational database backend registration through `datafusion-table-providers`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use coral_spec::backends::database::{
    DatabaseConnectionSpec, DatabaseSourceManifest, MySqlConnectionSpec, PostgresConnectionSpec,
    SqliteConnectionSpec,
};
use coral_spec::{ParsedTemplate, SourceManifestCommon};
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::TableType;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion::sql::unparser::dialect::{Dialect, MySqlDialect, PostgreSqlDialect, SqliteDialect};
use datafusion_table_providers::UnsupportedTypeAction;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::query_arrow;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPoolFactory;
use datafusion_table_providers::sql::db_connection_pool::{DbConnectionPool, Mode};
use datafusion_table_providers::sql::sql_provider_datafusion::SqlTable;
use datafusion_table_providers::util::secrets::to_secret_map;
use futures::TryStreamExt as _;

use crate::backends::shared::template::{RenderContext, render_template};
use crate::backends::{
    BackendCatalogRegistration, BackendCompileRequest, BackendRegistration,
    BackendRegistrationContext, CompiledBackendSource, RegisteredSource, RegisteredTable,
    SourceQualifiedName, build_registered_inputs,
};

const REMOTE_DATABASE_REGISTRATION_TIMEOUT: Duration = Duration::from_secs(10);
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) fn compile_manifest(
    manifest: &DatabaseSourceManifest,
    request: &BackendCompileRequest<'_>,
) -> Box<dyn CompiledBackendSource> {
    Box::new(CompiledDatabaseSource {
        manifest: manifest.clone(),
        source_secrets: request.source_secrets.clone(),
        source_variables: request.source_variables.clone(),
    })
}

struct CompiledDatabaseSource {
    manifest: DatabaseSourceManifest,
    source_secrets: BTreeMap<String, String>,
    source_variables: BTreeMap<String, String>,
}

#[async_trait]
trait DatabaseCatalogStrategy: Send + Sync {
    fn provider_name(&self) -> &'static str;

    fn registration_timeout(&self) -> Option<Duration> {
        Some(REMOTE_DATABASE_REGISTRATION_TIMEOUT)
    }

    async fn build_catalog(&self, context: &RenderContext<'_>)
    -> DataFusionResult<DatabaseCatalog>;
}

fn database_strategy(connection: &DatabaseConnectionSpec) -> &dyn DatabaseCatalogStrategy {
    match connection {
        DatabaseConnectionSpec::Postgres(connection) => connection,
        DatabaseConnectionSpec::MySql(connection) => connection,
        DatabaseConnectionSpec::Sqlite(connection) => connection,
    }
}

#[async_trait]
impl CompiledBackendSource for CompiledDatabaseSource {
    fn qualified_name(&self) -> SourceQualifiedName {
        SourceQualifiedName::Catalog(self.manifest.common.name.clone())
    }

    fn source_name(&self) -> &str {
        &self.manifest.common.name
    }

    fn validate_runtime_capabilities(&self) -> DataFusionResult<()> {
        Ok(())
    }

    async fn register(
        &self,
        _ctx: &SessionContext,
        _registration: &BackendRegistrationContext,
    ) -> DataFusionResult<BackendRegistration> {
        let resolved_inputs = coral_spec::resolve_inputs(
            &self.manifest.declared_inputs,
            &self.source_secrets,
            &self.source_variables,
        );
        let context = RenderContext::source_scoped(&resolved_inputs);
        let strategy = database_strategy(&self.manifest.connection);
        let provider_name = strategy.provider_name();
        let registration_timeout = strategy.registration_timeout();
        let registration = async {
            let database_catalog = strategy.build_catalog(&context).await?;
            let source = registered_source_for_catalog(
                &self.manifest.common,
                &self.manifest.declared_inputs,
                &self.source_secrets,
                &self.source_variables,
                &database_catalog.relations,
            );

            Ok(BackendRegistration {
                schemas: Vec::new(),
                catalogs: vec![BackendCatalogRegistration {
                    catalog: database_catalog.provider,
                    source,
                }],
            })
        };

        match registration_timeout {
            Some(timeout) => {
                timed_database_registration(
                    &self.manifest.common.name,
                    provider_name,
                    timeout,
                    registration,
                )
                .await
            }
            None => registration.await,
        }
    }
}

async fn timed_database_registration<T>(
    source_name: &str,
    provider: &str,
    timeout: Duration,
    registration: impl Future<Output = DataFusionResult<T>>,
) -> DataFusionResult<T> {
    tokio::time::timeout(timeout, registration)
        .await
        .map_err(|_elapsed| {
            DataFusionError::Execution(format!(
                "database source '{source_name}' ({provider}) registration timed out after {} seconds",
                timeout.as_secs()
            ))
        })?
}

#[async_trait]
impl DatabaseCatalogStrategy for PostgresConnectionSpec {
    fn provider_name(&self) -> &'static str {
        "postgres"
    }

    async fn build_catalog(
        &self,
        context: &RenderContext<'_>,
    ) -> DataFusionResult<DatabaseCatalog> {
        let mut params = render_connection_params(
            [
                ("host", &self.host),
                ("port", &self.port),
                ("db", &self.database),
                ("user", &self.user),
                ("pass", &self.password),
            ],
            context,
        )?;
        if let Some(sslmode) = self.sslmode.as_ref() {
            params.insert("sslmode".to_string(), render_template(sslmode, context)?);
        }
        let pool = PostgresConnectionPool::new(to_secret_map(params))
            .await
            .map_err(provider_error)?
            // The MySQL adapter has no equivalent unsupported-type policy.
            .with_unsupported_type_action(UnsupportedTypeAction::String);
        database_catalog(
            Arc::new(pool),
            POSTGRES_RELATIONS_SQL,
            Arc::new(PostgreSqlDialect {}),
        )
        .await
    }
}

#[async_trait]
impl DatabaseCatalogStrategy for MySqlConnectionSpec {
    fn provider_name(&self) -> &'static str {
        "mysql"
    }

    async fn build_catalog(
        &self,
        context: &RenderContext<'_>,
    ) -> DataFusionResult<DatabaseCatalog> {
        let mut params = render_connection_params(
            [
                ("host", &self.host),
                ("db", &self.database),
                ("user", &self.user),
                ("pass", &self.password),
            ],
            context,
        )?;
        let raw_port = render_template(&self.port, context)?;
        let tcp_port = match raw_port.trim().parse::<u16>() {
            Ok(port) if port != 0 => port,
            _ => {
                return Err(DataFusionError::Execution(
                    "MySQL connection.port is invalid; expected an integer between 1 and 65535"
                        .to_string(),
                ));
            }
        };
        params.insert("tcp_port".to_string(), tcp_port.to_string());
        let pool = MySQLConnectionPool::new(to_secret_map(params))
            .await
            .map_err(provider_error)?;
        database_catalog(
            Arc::new(pool),
            MYSQL_RELATIONS_SQL,
            Arc::new(MySqlDialect {}),
        )
        .await
    }
}

fn render_connection_params<const N: usize>(
    fields: [(&str, &ParsedTemplate); N],
    context: &RenderContext<'_>,
) -> DataFusionResult<HashMap<String, String>> {
    fields
        .into_iter()
        .map(|(key, template)| Ok((key.to_string(), render_template(template, context)?)))
        .collect()
}

#[async_trait]
impl DatabaseCatalogStrategy for SqliteConnectionSpec {
    fn provider_name(&self) -> &'static str {
        "sqlite"
    }

    fn registration_timeout(&self) -> Option<Duration> {
        None
    }

    async fn build_catalog(
        &self,
        context: &RenderContext<'_>,
    ) -> DataFusionResult<DatabaseCatalog> {
        let path = render_template(&self.path, context)?;
        let pool = SqliteConnectionPoolFactory::new(&path, Mode::File, SQLITE_BUSY_TIMEOUT)
            .build()
            .await
            .map_err(boxed_provider_error)?;
        database_catalog(
            Arc::new(pool),
            SQLITE_RELATIONS_SQL,
            Arc::new(SqliteDialect {}),
        )
        .await
    }
}

const POSTGRES_RELATIONS_SQL: &str = "
SELECT table_schema AS schema_name,
       table_name,
       table_type AS relation_type
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')";

const MYSQL_RELATIONS_SQL: &str = "
SELECT TABLE_SCHEMA AS schema_name,
       TABLE_NAME AS table_name,
       TABLE_TYPE AS relation_type
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')";

const SQLITE_RELATIONS_SQL: &str = "
SELECT 'main' AS schema_name,
       name AS table_name,
       CASE type WHEN 'view' THEN 'VIEW' ELSE 'BASE TABLE' END AS relation_type
FROM sqlite_master
WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'";

type Pool<T, P> = Arc<dyn DbConnectionPool<T, P> + Send + Sync>;

struct DatabaseCatalog {
    provider: Arc<dyn CatalogProvider>,
    relations: Vec<DatabaseRelation>,
}

#[derive(Clone, Debug)]
struct DatabaseRelation {
    schema_name: String,
    table_name: String,
    table_type: TableType,
}

type SqlDialect = Arc<dyn Dialect + Send + Sync>;

async fn database_catalog<T: 'static, P: 'static>(
    pool: Pool<T, P>,
    inventory_sql: &str,
    dialect: SqlDialect,
) -> DataFusionResult<DatabaseCatalog> {
    let relations = load_database_inventory(&pool, inventory_sql).await?;
    let provider = Arc::new(CoralDatabaseCatalogProvider::new(
        &pool, &relations, &dialect,
    )) as Arc<dyn CatalogProvider>;
    Ok(DatabaseCatalog {
        provider,
        relations,
    })
}

async fn load_database_inventory<T: 'static, P: 'static>(
    pool: &Pool<T, P>,
    inventory_sql: &str,
) -> DataFusionResult<Vec<DatabaseRelation>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("relation_type", DataType::Utf8, false),
    ]));
    let connection = pool.connect().await.map_err(boxed_provider_error)?;
    let batches = query_arrow(connection, inventory_sql.to_string(), Some(schema))
        .await
        .map_err(provider_error)?
        .try_collect::<Vec<_>>()
        .await?;
    let mut relations = Vec::new();
    for batch in batches {
        let schema_names = database_inventory_column(&batch, "schema_name")?;
        let table_names = database_inventory_column(&batch, "table_name")?;
        let relation_types = database_inventory_column(&batch, "relation_type")?;
        for row in 0..batch.num_rows() {
            relations.push(DatabaseRelation {
                schema_name: schema_names.value(row).to_string(),
                table_name: table_names.value(row).to_string(),
                table_type: relation_table_type(relation_types.value(row)),
            });
        }
    }
    relations.sort_by(|left, right| {
        (&left.schema_name, &left.table_name).cmp(&(&right.schema_name, &right.table_name))
    });
    relations.dedup_by(|left, right| {
        left.schema_name == right.schema_name && left.table_name == right.table_name
    });
    Ok(relations)
}

fn database_inventory_column<'a>(
    batch: &'a datafusion::arrow::record_batch::RecordBatch,
    name: &str,
) -> DataFusionResult<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            DataFusionError::Execution(format!("database inventory column '{name}' is not Utf8"))
        })
}

fn relation_table_type(value: &str) -> TableType {
    if value.eq_ignore_ascii_case("VIEW") || value.eq_ignore_ascii_case("MATERIALIZED VIEW") {
        TableType::View
    } else {
        TableType::Base
    }
}

struct CoralDatabaseCatalogProvider {
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl CoralDatabaseCatalogProvider {
    fn new<T: 'static, P: 'static>(
        pool: &Pool<T, P>,
        relations: &[DatabaseRelation],
        dialect: &SqlDialect,
    ) -> Self {
        let mut by_schema = BTreeMap::<String, BTreeMap<String, TableType>>::new();
        for relation in relations {
            by_schema
                .entry(relation.schema_name.clone())
                .or_default()
                .insert(relation.table_name.clone(), relation.table_type);
        }
        let schemas = by_schema
            .into_iter()
            .map(|(schema_name, tables)| {
                let provider = CoralDatabaseSchemaProvider {
                    schema_name: schema_name.clone(),
                    tables,
                    pool: Arc::clone(pool),
                    dialect: Arc::clone(dialect),
                };
                (schema_name, Arc::new(provider) as Arc<dyn SchemaProvider>)
            })
            .collect();
        Self { schemas }
    }
}

impl fmt::Debug for CoralDatabaseCatalogProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoralDatabaseCatalogProvider")
            .field("schemas", &self.schemas.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl CatalogProvider for CoralDatabaseCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}

struct CoralDatabaseSchemaProvider<T: 'static, P: 'static> {
    schema_name: String,
    tables: BTreeMap<String, TableType>,
    pool: Pool<T, P>,
    dialect: SqlDialect,
}

impl<T: 'static, P: 'static> fmt::Debug for CoralDatabaseSchemaProvider<T, P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoralDatabaseSchemaProvider")
            .field("schema_name", &self.schema_name)
            .field("tables", &self.tables)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<T: 'static, P: 'static> SchemaProvider for CoralDatabaseSchemaProvider<T, P> {
    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        if !self.tables.contains_key(name) {
            return Ok(None);
        }
        SqlTable::new(
            &self.schema_name,
            &self.pool,
            TableReference::partial(self.schema_name.clone(), name.to_string()),
        )
        .await
        .map(|table| {
            let table = table.with_dialect(Arc::clone(&self.dialect));
            Some(Arc::new(table) as Arc<dyn TableProvider>)
        })
        .map_err(provider_error)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    async fn table_type(&self, name: &str) -> DataFusionResult<Option<TableType>> {
        Ok(self.tables.get(name).copied())
    }
}

fn registered_source_for_catalog(
    common: &SourceManifestCommon,
    declared_inputs: &[coral_spec::ManifestInputSpec],
    source_secrets: &BTreeMap<String, String>,
    source_variables: &BTreeMap<String, String>,
    relations: &[DatabaseRelation],
) -> RegisteredSource {
    let secret_keys = source_secrets.keys().cloned().collect::<BTreeSet<_>>();
    RegisteredSource {
        qualified_name: SourceQualifiedName::Catalog(common.name.clone()),
        tables: database_relation_inventory(relations),
        table_functions: Vec::new(),
        inputs: build_registered_inputs(declared_inputs, source_variables, &secret_keys),
    }
}

/// Project the Coral-owned relation inventory into public catalog metadata
/// without constructing table providers or fetching column schemas.
fn database_relation_inventory(relations: &[DatabaseRelation]) -> Vec<RegisteredTable> {
    relations
        .iter()
        .map(|relation| RegisteredTable {
            schema_name: Some(relation.schema_name.clone()),
            table_name: relation.table_name.clone(),
            description: String::new(),
            guide: String::new(),
            columns: Vec::new(),
            filters: Vec::new(),
            required_filters: Vec::new(),
            search_limits: None,
        })
        .collect()
}

fn provider_error(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn boxed_provider_error(error: Box<dyn std::error::Error + Send + Sync>) -> DataFusionError {
    DataFusionError::External(error)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use coral_spec::{
        DatabaseConnectionSpec, DatabaseSourceManifest, MySqlConnectionSpec, ParsedTemplate,
        SourceManifestCommon, SqliteConnectionSpec,
    };

    use super::DatabaseCatalogStrategy;
    use crate::backends::shared::template::RenderContext;
    use crate::{
        CoralQuery, QueryRuntimeConfig, QuerySource, RuntimeSourceComponent, RuntimeSourcePackage,
        SourceDecorator, SourceDecoratorError, SourceFailurePolicy, SourceTables,
    };

    struct AbortOnSourceFailureDecorator;

    impl SourceDecorator for AbortOnSourceFailureDecorator {
        fn name(&self) -> &'static str {
            "abort-on-source-failure"
        }

        fn decorate_source(
            &mut self,
            _source: &QuerySource,
            tables: SourceTables,
        ) -> Result<SourceTables, SourceDecoratorError> {
            Ok(tables)
        }

        fn source_failed(
            &mut self,
            _source: &QuerySource,
            _error: &crate::CoreError,
        ) -> Result<SourceFailurePolicy, SourceDecoratorError> {
            Ok(SourceFailurePolicy::Abort)
        }
    }

    fn template(value: &str) -> ParsedTemplate {
        ParsedTemplate::parse(value.to_string()).expect("template")
    }

    fn mysql_connection(port: &str) -> MySqlConnectionSpec {
        MySqlConnectionSpec {
            host: template("localhost"),
            port: template(port),
            database: template("coral"),
            user: template("root"),
            password: template("password"),
        }
    }

    fn sqlite_source(path: String) -> QuerySource {
        let database = DatabaseSourceManifest {
            common: SourceManifestCommon {
                dsl_version: 4,
                name: "coral_db".to_string(),
                version: String::new(),
                description: "Coral test database".to_string(),
                test_queries: Vec::new(),
            },
            connection: DatabaseConnectionSpec::Sqlite(SqliteConnectionSpec {
                path: ParsedTemplate::parse(path).expect("sqlite path template"),
            }),
            declared_inputs: Vec::new(),
        };
        QuerySource::from_runtime_components(
            RuntimeSourcePackage {
                source_name: "coral_db".to_string(),
                authored_version: None,
                description: String::new(),
                declared_inputs: Vec::new(),
                test_queries: Vec::new(),
                components: vec![RuntimeSourceComponent::Database(database)],
            },
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .expect("database runtime source")
    }

    #[tokio::test]
    async fn mysql_strategy_rejects_ports_that_provider_would_default() {
        let cases = [
            ("not-a-port", BTreeMap::new()),
            ("70000", BTreeMap::new()),
            ("0", BTreeMap::new()),
            (
                "{{input.DB_PORT}}",
                BTreeMap::from([("DB_PORT".to_string(), "not-a-port".to_string())]),
            ),
        ];

        for (port, resolved_inputs) in cases {
            let connection = mysql_connection(port);
            let context = RenderContext::source_scoped(&resolved_inputs);
            let Err(error) = connection.build_catalog(&context).await else {
                panic!("invalid MySQL port should fail before provider fallback");
            };
            let message = error.to_string();
            assert!(
                message.contains("MySQL connection.port is invalid"),
                "unexpected error: {message}"
            );
            assert!(
                message.contains("between 1 and 65535"),
                "unexpected error: {message}"
            );
        }
    }

    #[tokio::test]
    async fn sqlite_database_source_registers_catalog_and_queries_three_part_table() {
        let temp = tempfile::tempdir().expect("temp dir");
        let db_path = temp.path().join("coral.sqlite");
        let conn = rusqlite::Connection::open(&db_path).expect("sqlite db");
        conn.execute_batch(
            "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
            INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 'Lin');
            ",
        )
        .expect("sqlite fixture");
        drop(conn);

        let sources = vec![sqlite_source(db_path.to_string_lossy().into_owned())];

        let tables = CoralQuery::list_tables(
            &sources,
            QueryRuntimeConfig::default(),
            Some("coral_db"),
            Some("main"),
            Some("users"),
        )
        .await
        .expect("list tables");
        assert_eq!(tables.len(), 1);
        let table = tables.first().expect("table metadata");
        assert_eq!(table.catalog_name.as_deref(), Some("coral_db"));
        assert_eq!(table.schema_name, "main");
        assert_eq!(table.table_name, "users");
        assert_eq!(
            table
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "name"],
            "database columns flow through coral.columns"
        );

        let result = CoralQuery::execute_sql(
            &sources,
            QueryRuntimeConfig::default(),
            "SELECT id FROM coral_db.main.users WHERE name = 'Lin'",
        )
        .await
        .expect("query sqlite table");
        assert_eq!(result.row_count(), 1);
        let scanned_sources = result
            .provenance()
            .tables()
            .iter()
            .map(|usage| usage.source_name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            scanned_sources,
            ["coral_db"],
            "scan attributes to the source"
        );

        let catalog_result = CoralQuery::execute_sql(
            &sources,
            QueryRuntimeConfig::default(),
            "SELECT catalog_name, schema_name, table_name FROM coral.tables \
             WHERE catalog_name = 'coral_db' AND schema_name = 'main' AND table_name = 'users'",
        )
        .await
        .expect("query coral tables");
        assert_eq!(catalog_result.row_count(), 1);

        CoralQuery::execute_sql(
            &sources,
            QueryRuntimeConfig::default(),
            "SELECT * FROM coral._columns_static",
        )
        .await
        .expect_err("the internal columns staging table must not be queryable");

        let source = sources.first().expect("source");
        CoralQuery::validate_source(source, QueryRuntimeConfig::default(), &[])
            .await
            .expect("database source validates");
    }

    #[tokio::test]
    async fn database_catalog_registration_rejects_source_decorators() {
        let temp = tempfile::tempdir().expect("temp dir");
        let db_path = temp.path().join("coral.sqlite");
        drop(rusqlite::Connection::open(&db_path).expect("sqlite db"));
        let sources = vec![sqlite_source(db_path.to_string_lossy().into_owned())];

        let mut decorated_config = QueryRuntimeConfig::default();
        decorated_config
            .extensions
            .source_decorators
            .push(Box::new(AbortOnSourceFailureDecorator));
        let error = CoralQuery::list_tables(
            &sources,
            decorated_config,
            Some("coral_db"),
            Some("main"),
            Some("users"),
        )
        .await
        .expect_err("catalog registrations must reject source decorators");
        assert!(
            error
                .to_string()
                .contains("registers database catalogs, which do not support source decorators"),
            "unexpected error: {error}"
        );
    }
}
