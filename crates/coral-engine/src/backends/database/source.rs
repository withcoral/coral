//! Database source compilation and provider-specific catalog registration.
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use coral_spec::backends::database::{
    DatabaseConnectionSpec, DatabaseSourceManifest, MySqlConnectionSpec, PostgresConnectionSpec,
    SqliteConnectionSpec,
};
use coral_spec::{ParsedTemplate, SourceManifestCommon};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::prelude::SessionContext;
use datafusion::sql::unparser::dialect::{MySqlDialect, PostgreSqlDialect, SqliteDialect};
use datafusion_table_providers::UnsupportedTypeAction;
use datafusion_table_providers::sql::db_connection_pool::Mode;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPoolFactory;
use datafusion_table_providers::util::secrets::to_secret_map;

use super::catalog::{DatabaseCatalog, DatabaseRelation, build_database_catalog, provider_error};
use super::columns::{MYSQL_COLUMNS_SQL, POSTGRES_COLUMNS_SQL, SQLITE_COLUMNS_SQL};
use super::registry::{DatabasePool, DatabasePoolRegistry};
use crate::backends::shared::template::{RenderContext, render_template};
use crate::backends::{
    BackendCatalogRegistration, BackendCompileRequest, BackendRegistration,
    BackendRegistrationContext, CompiledBackendSource, RegisteredSource, RegisteredTable,
    SourceQualifiedName, build_registered_inputs,
};

/// Budget for building or obtaining a remote pool and loading its inventory.
const REMOTE_DATABASE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_REMOTE_DATABASE_REGISTRATION_ATTEMPTS: usize = 2;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) fn compile_manifest(
    manifest: &DatabaseSourceManifest,
    request: &BackendCompileRequest<'_>,
) -> Box<dyn CompiledBackendSource> {
    Box::new(CompiledDatabaseSource {
        manifest: manifest.clone(),
        source_secrets: request.source_secrets.clone(),
        source_variables: request.source_variables.clone(),
        pool_registry: Arc::clone(&request.database_pool_registry),
    })
}

struct CompiledDatabaseSource {
    manifest: DatabaseSourceManifest,
    source_secrets: BTreeMap<String, String>,
    source_variables: BTreeMap<String, String>,
    pool_registry: Arc<DatabasePoolRegistry>,
}

#[async_trait]
trait DatabaseCatalogStrategy: Send + Sync {
    fn provider_name(&self) -> &'static str;

    fn registration_timeout(&self) -> Option<Duration> {
        Some(REMOTE_DATABASE_ATTEMPT_TIMEOUT)
    }

    async fn build_catalog(
        &self,
        catalog_name: &str,
        context: &RenderContext<'_>,
        pool_registry: &DatabasePoolRegistry,
    ) -> DataFusionResult<DatabaseCatalog>;
}

fn database_strategy(connection: &DatabaseConnectionSpec) -> &dyn DatabaseCatalogStrategy {
    match connection {
        DatabaseConnectionSpec::Postgres(connection) => connection,
        DatabaseConnectionSpec::MySql(connection) => connection,
        DatabaseConnectionSpec::Sqlite(connection) => connection,
    }
}

async fn register_database_catalog(
    strategy: &dyn DatabaseCatalogStrategy,
    catalog_name: &str,
    context: &RenderContext<'_>,
    pool_registry: &DatabasePoolRegistry,
) -> DataFusionResult<DatabaseCatalog> {
    let Some(timeout) = strategy.registration_timeout() else {
        return strategy
            .build_catalog(catalog_name, context, pool_registry)
            .await;
    };
    let registration = || strategy.build_catalog(catalog_name, context, pool_registry);
    for _attempt in 0..MAX_REMOTE_DATABASE_REGISTRATION_ATTEMPTS {
        match tokio::time::timeout(timeout, registration()).await {
            Ok(result) => return result,
            Err(_elapsed) => pool_registry.remove_catalog(catalog_name),
        }
    }
    Err(DataFusionError::Execution(format!(
        "database source '{catalog_name}' ({}) registration timed out after \
         {MAX_REMOTE_DATABASE_REGISTRATION_ATTEMPTS} {}-second attempts",
        strategy.provider_name(),
        timeout.as_secs()
    )))
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
        let catalog_name = &self.manifest.common.name;
        let database_catalog =
            register_database_catalog(strategy, catalog_name, &context, &self.pool_registry)
                .await?;
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
                column_fetcher: database_catalog.column_fetcher,
            }],
        })
    }
}

#[async_trait]
impl DatabaseCatalogStrategy for PostgresConnectionSpec {
    fn provider_name(&self) -> &'static str {
        "postgres"
    }

    async fn build_catalog(
        &self,
        catalog_name: &str,
        context: &RenderContext<'_>,
        pool_registry: &DatabasePoolRegistry,
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
        let pool = pool_registry
            .get_or_create(catalog_name, async move {
                Ok(DatabasePool::Postgres(Arc::new(
                    PostgresConnectionPool::new(to_secret_map(params))
                        .await
                        .map_err(provider_error)?
                        .with_unsupported_type_action(UnsupportedTypeAction::String),
                )))
            })
            .await?;
        let DatabasePool::Postgres(pool) = pool else {
            return Err(DataFusionError::Internal(format!(
                "database catalog '{catalog_name}' resolved to a non-Postgres pool"
            )));
        };
        build_database_catalog(
            catalog_name,
            pool,
            None,
            POSTGRES_RELATIONS_SQL,
            POSTGRES_COLUMNS_SQL,
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
        catalog_name: &str,
        context: &RenderContext<'_>,
        pool_registry: &DatabasePoolRegistry,
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
        let pool = pool_registry
            .get_or_create(catalog_name, async move {
                Ok(DatabasePool::MySql(Arc::new(
                    MySQLConnectionPool::new(to_secret_map(params))
                        .await
                        .map_err(provider_error)?,
                )))
            })
            .await?;
        let DatabasePool::MySql(pool) = pool else {
            return Err(DataFusionError::Internal(format!(
                "database catalog '{catalog_name}' resolved to a non-MySQL pool"
            )));
        };
        let inventory_sql = mysql_relations_sql();
        build_database_catalog(
            catalog_name,
            pool,
            Some(MYSQL_INVENTORY_SESSION_SQL),
            &inventory_sql,
            MYSQL_COLUMNS_SQL,
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
        catalog_name: &str,
        context: &RenderContext<'_>,
        _pool_registry: &DatabasePoolRegistry,
    ) -> DataFusionResult<DatabaseCatalog> {
        let path = render_template(&self.path, context)?;
        let pool = SqliteConnectionPoolFactory::new(&path, Mode::File, SQLITE_BUSY_TIMEOUT)
            .build()
            .await
            .map_err(provider_error)?;
        build_database_catalog(
            catalog_name,
            Arc::new(pool),
            None,
            SQLITE_RELATIONS_SQL,
            SQLITE_COLUMNS_SQL,
            Arc::new(SqliteDialect {}),
        )
        .await
    }
}

const POSTGRES_RELATIONS_SQL: &str = "
SELECT CAST(table_schema AS TEXT) AS schema_name,
       CAST(table_name AS TEXT) AS table_name,
       CAST(table_type AS TEXT) AS relation_type,
       CAST(NULL AS TEXT) AS unrecognized_columns
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
  AND table_type IN ('BASE TABLE', 'VIEW')";

// Keep unrecognized-column diagnostics intact for the widest valid MySQL tables.
const MYSQL_INVENTORY_SESSION_SQL: &str = "SET SESSION group_concat_max_len = 1048576";

// `datafusion-table-providers-mysql` 0.13.0 keeps its MySQL type mapper private. This list is an
// advisory approximation used only to diagnose unfamiliar information_schema DATA_TYPE values;
// lazy provider schema discovery remains authoritative for queryability. Re-audit this list on
// upgrades, but never use it to exclude a relation from the catalog.
const MYSQL_INVENTORY_RECOGNIZED_DATA_TYPES: &[&str] = &[
    "decimal",
    "numeric",
    "newdecimal",
    "tinyint",
    "smallint",
    "int",
    "integer",
    "bigint",
    "mediumint",
    "float",
    "double",
    "null",
    "timestamp",
    "time",
    "datetime",
    "date",
    "year",
    "bit",
    "json",
    "enum",
    "set",
    "tinyblob",
    "tinytext",
    "mediumblob",
    "mediumtext",
    "longblob",
    "longtext",
    "blob",
    "text",
    "varchar",
    "varbinary",
    "char",
    "binary",
];

fn mysql_relations_sql() -> String {
    let recognized_types = MYSQL_INVENTORY_RECOGNIZED_DATA_TYPES
        .iter()
        .map(|data_type| format!("'{data_type}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "
SELECT tables.TABLE_SCHEMA AS schema_name,
       tables.TABLE_NAME AS table_name,
       tables.TABLE_TYPE AS relation_type,
       GROUP_CONCAT(
           CASE
               WHEN LOWER(columns.DATA_TYPE) NOT IN ({recognized_types})
               THEN CONCAT(columns.COLUMN_NAME, ' (', columns.DATA_TYPE, ')')
           END
           ORDER BY columns.ORDINAL_POSITION
           SEPARATOR ', '
       ) AS unrecognized_columns
FROM INFORMATION_SCHEMA.TABLES AS tables
LEFT JOIN INFORMATION_SCHEMA.COLUMNS AS columns
  ON columns.TABLE_SCHEMA = tables.TABLE_SCHEMA
 AND columns.TABLE_NAME = tables.TABLE_NAME
WHERE tables.TABLE_SCHEMA NOT IN (
    'information_schema', 'mysql', 'performance_schema', 'sys'
)
GROUP BY tables.TABLE_SCHEMA, tables.TABLE_NAME, tables.TABLE_TYPE"
    )
}

const SQLITE_RELATIONS_SQL: &str = "
SELECT 'main' AS schema_name,
       name AS table_name,
       CASE type WHEN 'view' THEN 'VIEW' ELSE 'BASE TABLE' END AS relation_type,
       CAST(NULL AS TEXT) AS unrecognized_columns
FROM sqlite_master
WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'";

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
            // Discovered from the remote database rather than authored, so
            // there is no guide to require reading.
            require_guide_read: false,
            columns: Vec::new(),
            filters: Vec::new(),
            required_filters: Vec::new(),
            search_limits: None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use coral_spec::{
        DatabaseConnectionSpec, MySqlConnectionSpec, ParsedTemplate, SqliteConnectionSpec,
    };

    use super::{
        DatabaseCatalogStrategy, MYSQL_INVENTORY_RECOGNIZED_DATA_TYPES,
        MYSQL_INVENTORY_SESSION_SQL, mysql_relations_sql,
    };
    use crate::backends::shared::template::RenderContext;
    use crate::{
        CoralQuery, DatabaseRuntimeBackend, QueryRuntimeConfig, QuerySource, RuntimeCatalog,
        RuntimeSourcePackage, SourceDecorator, SourceDecoratorError, SourceFailurePolicy,
        SourceTables,
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

    #[test]
    fn mysql_inventory_query_preserves_advisory_type_diagnostics() {
        let recognized_types = MYSQL_INVENTORY_RECOGNIZED_DATA_TYPES
            .iter()
            .map(|data_type| format!("'{data_type}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = mysql_relations_sql();

        assert!(sql.contains(&format!("NOT IN ({recognized_types})")));
        assert!(sql.contains("GROUP_CONCAT("));
        assert!(sql.contains("LEFT JOIN INFORMATION_SCHEMA.COLUMNS"));
        assert!(sql.contains("AS unrecognized_columns"));
        assert!(sql.contains("GROUP BY tables.TABLE_SCHEMA, tables.TABLE_NAME, tables.TABLE_TYPE"));
        assert!(
            !MYSQL_INVENTORY_RECOGNIZED_DATA_TYPES.contains(&"geometry"),
            "geometry is recognized by the provider parser but lacks Arrow conversion"
        );
    }

    #[test]
    fn mysql_inventory_session_preserves_complete_type_diagnostics() {
        assert_eq!(
            MYSQL_INVENTORY_SESSION_SQL,
            "SET SESSION group_concat_max_len = 1048576"
        );
    }

    fn sqlite_source(path: String) -> QuerySource {
        let backend = DatabaseRuntimeBackend::new(
            4,
            DatabaseConnectionSpec::Sqlite(SqliteConnectionSpec {
                path: ParsedTemplate::parse(path).expect("sqlite path template"),
            }),
        );
        QuerySource::from_runtime_components(
            RuntimeSourcePackage {
                source_name: "coral_db".to_string(),
                authored_version: None,
                description: String::new(),
                declared_inputs: Vec::new(),
                test_queries: Vec::new(),
                identity_requirements: None,
                catalogs: vec![
                    RuntimeCatalog::try_database_provider_discovered("coral_db", backend)
                        .expect("database catalog"),
                ],
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
            let registry = super::DatabasePoolRegistry::new();
            let Err(error) = connection
                .build_catalog("coral_db", &context, &registry)
                .await
            else {
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
        let id = table.columns.first().expect("id column metadata");
        assert_eq!(id.name, "id");
        assert_eq!(id.data_type, "INTEGER");
        assert_eq!(id.ordinal_position, 0);
        let name = table.columns.get(1).expect("name column metadata");
        assert_eq!(name.name, "name");
        assert_eq!(name.data_type, "TEXT");
        assert!(!name.nullable);
        assert_eq!(name.ordinal_position, 1);

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
    async fn provider_discovered_catalog_rejects_unsupported_source_decorators() {
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
        .expect_err("provider-discovered catalogs must reject unsupported decorators");
        assert!(
            error.to_string().contains(
                "registers provider-discovered catalogs, which source decorator \
                     'abort-on-source-failure' does not support"
            ),
            "unexpected error: {error}"
        );
    }
}
