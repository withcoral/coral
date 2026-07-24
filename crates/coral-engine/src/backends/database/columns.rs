//! Lazy column-metadata inventory for relational database sources.
//!
//! Each provider answers column metadata from its own catalog
//! (`information_schema.columns`, `pragma_table_xinfo`) in a single round
//! trip, instead of probing every table's Arrow schema individually.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion_table_providers::sql::db_connection_pool::dbconnection::query_arrow;
use futures::TryStreamExt as _;

use super::catalog::{Pool, inventory_column, provider_error};
use crate::backends::{ColumnInventoryFilter, DatabaseColumnFetcher, DatabaseColumnRow};

/// Normalized inventory queries. Every provider projects the same six Utf8
/// columns so one Arrow schema and one row parser serve all of them:
/// `schema_name`, `table_name`, `ordinal_position` (zero-based, as text,
/// matching the `coral.columns` contract),
/// `column_name`, `data_type` (provider-native name), `is_nullable`
/// ('true'/'false').
pub(super) const POSTGRES_COLUMNS_SQL: &str = "
SELECT table_schema AS schema_name,
       table_name,
       CAST(ordinal_position - 1 AS TEXT) AS ordinal_position,
       column_name,
       data_type,
       CASE WHEN is_nullable = 'YES' THEN 'true' ELSE 'false' END AS is_nullable
FROM information_schema.columns
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')";

pub(super) const MYSQL_COLUMNS_SQL: &str = "
SELECT TABLE_SCHEMA AS schema_name,
       TABLE_NAME AS table_name,
       CAST(ORDINAL_POSITION - 1 AS CHAR) AS ordinal_position,
       COLUMN_NAME AS column_name,
       DATA_TYPE AS data_type,
       CASE WHEN IS_NULLABLE = 'YES' THEN 'true' ELSE 'false' END AS is_nullable
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')";

pub(super) const SQLITE_COLUMNS_SQL: &str = "
SELECT 'main' AS schema_name,
       m.name AS table_name,
       CAST(p.cid AS TEXT) AS ordinal_position,
       p.name AS column_name,
       CASE WHEN p.type = '' THEN 'unknown' ELSE p.type END AS data_type,
       CASE
         WHEN p.\"notnull\" <> 0 THEN 'false'
         WHEN p.pk > 0
          AND UPPER(TRIM(p.type)) = 'INTEGER'
          AND (SELECT COUNT(*) FROM pragma_table_xinfo(m.name) AS key_column WHERE key_column.pk > 0) = 1
         THEN 'false'
         ELSE 'true'
       END AS is_nullable
FROM sqlite_master AS m
JOIN pragma_table_xinfo(m.name) AS p
WHERE m.type IN ('table', 'view')
  AND m.name NOT LIKE 'sqlite_%'
  AND p.hidden <> 1";

/// [`DatabaseColumnFetcher`] backed by a connection pool and one of the
/// provider inventory queries above.
pub(super) struct DatabaseColumnInventoryFetcher<T: 'static, P: 'static> {
    pool: Pool<T, P>,
    base_sql: &'static str,
}

impl<T: 'static, P: 'static> DatabaseColumnInventoryFetcher<T, P> {
    pub(super) fn new(pool: &Pool<T, P>, base_sql: &'static str) -> Arc<Self> {
        Arc::new(Self {
            pool: Arc::clone(pool),
            base_sql,
        })
    }
}

impl<T: 'static, P: 'static> fmt::Debug for DatabaseColumnInventoryFetcher<T, P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DatabaseColumnInventoryFetcher")
            .field("base_sql", &self.base_sql)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<T: 'static, P: 'static> DatabaseColumnFetcher for DatabaseColumnInventoryFetcher<T, P> {
    async fn fetch_columns(
        &self,
        filter: &ColumnInventoryFilter,
    ) -> DataFusionResult<Vec<DatabaseColumnRow>> {
        if filter_matches_nothing(filter) {
            return Ok(Vec::new());
        }
        let sql = compose_columns_sql(self.base_sql, filter);
        let schema = Arc::new(Schema::new(vec![
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("is_nullable", DataType::Utf8, false),
        ]));
        let connection = self.pool.connect().await.map_err(provider_error)?;
        let batches = query_arrow(connection, sql, Some(schema))
            .await
            .map_err(provider_error)?
            .try_collect::<Vec<_>>()
            .await?;

        let mut rows = Vec::new();
        for batch in batches {
            let schema_names = inventory_column(&batch, "schema_name")?;
            let table_names = inventory_column(&batch, "table_name")?;
            let ordinals = inventory_column(&batch, "ordinal_position")?;
            let column_names = inventory_column(&batch, "column_name")?;
            let data_types = inventory_column(&batch, "data_type")?;
            let nullables = inventory_column(&batch, "is_nullable")?;
            for row in 0..batch.num_rows() {
                rows.push(DatabaseColumnRow {
                    schema_name: schema_names.value(row).to_string(),
                    table_name: table_names.value(row).to_string(),
                    ordinal_position: parse_ordinal(ordinals.value(row))?,
                    column_name: column_names.value(row).to_string(),
                    data_type: data_types.value(row).to_string(),
                    is_nullable: nullables.value(row) == "true",
                });
            }
        }
        rows.sort_by(|left, right| {
            (&left.schema_name, &left.table_name, left.ordinal_position).cmp(&(
                &right.schema_name,
                &right.table_name,
                right.ordinal_position,
            ))
        });
        Ok(rows)
    }
}

fn parse_ordinal(value: &str) -> DataFusionResult<i32> {
    let ordinal = value.trim().parse::<i32>().map_err(|_parse_error| {
        DataFusionError::Execution(format!(
            "database column inventory returned non-integer ordinal_position '{value}'"
        ))
    })?;
    if ordinal < 0 {
        return Err(DataFusionError::Execution(format!(
            "database column inventory returned negative ordinal_position '{value}'"
        )));
    }
    Ok(ordinal)
}

fn filter_matches_nothing(filter: &ColumnInventoryFilter) -> bool {
    filter.schemas.as_ref().is_some_and(Vec::is_empty)
        || filter.tables.as_ref().is_some_and(Vec::is_empty)
}

/// Wraps the base inventory query with the caller's schema/table restriction.
/// The subquery aliases every provider's columns to a common shape, so the
/// outer predicates are provider-independent.
///
/// Restrictions are pruning-only (the engine re-applies exact predicates
/// above the scan), so a dimension whose values cannot be embedded verbatim
/// is fetched unfiltered instead of escaped: dropping a whole restriction
/// fetches more rows and stays correct, while embedding a hostile value
/// would hand attacker-controlled SQL to the remote database (`MySQL`'s
/// default `sql_mode` treats `\` as a string escape, so quote-doubling
/// alone is not a safe quoting strategy there).
fn compose_columns_sql(base_sql: &str, filter: &ColumnInventoryFilter) -> String {
    let mut predicates = Vec::new();
    if let Some(schemas) = filter.schemas.as_ref()
        && values_embed_safely(schemas)
    {
        predicates.push(format!("schema_name IN ({})", sql_string_list(schemas)));
    }
    if let Some(tables) = filter.tables.as_ref()
        && values_embed_safely(tables)
    {
        predicates.push(format!("table_name IN ({})", sql_string_list(tables)));
    }
    if predicates.is_empty() {
        return base_sql.to_string();
    }
    format!(
        "SELECT * FROM ({base_sql}\n) AS columns_inventory WHERE {}",
        predicates.join(" AND ")
    )
}

/// Whether every value can sit inside a single-quoted SQL literal without any
/// escaping, across all supported providers' string-literal dialects.
fn values_embed_safely(values: &[String]) -> bool {
    values
        .iter()
        .all(|value| !value.contains('\'') && !value.contains('\\') && !value.contains('\0'))
}

fn sql_string_list(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("'{value}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use datafusion_table_providers::sql::db_connection_pool::Mode;
    use datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPoolFactory;

    use super::*;

    #[test]
    fn parse_ordinal_rejects_negative_values() {
        let error = parse_ordinal("-1").expect_err("negative ordinal must fail");
        assert!(error.to_string().contains("negative ordinal_position '-1'"));
    }

    #[test]
    fn compose_columns_sql_without_filter_returns_base() {
        let filter = ColumnInventoryFilter::default();
        assert_eq!(
            compose_columns_sql("SELECT 1", &filter),
            "SELECT 1".to_string()
        );
    }

    #[test]
    fn compose_columns_sql_combines_safe_predicates() {
        let filter = ColumnInventoryFilter {
            schemas: Some(vec!["main".to_string()]),
            tables: Some(vec!["users".to_string(), "orders".to_string()]),
        };
        let sql = compose_columns_sql("SELECT 1", &filter);
        assert_eq!(
            sql,
            "SELECT * FROM (SELECT 1\n) AS columns_inventory \
             WHERE schema_name IN ('main') AND table_name IN ('users', 'orders')"
        );
    }

    #[test]
    fn compose_columns_sql_drops_dimensions_it_cannot_embed_verbatim() {
        // Values with quote/backslash/NUL never reach the remote SQL text —
        // the whole dimension falls back to an unfiltered fetch (pruning-only
        // semantics keep results correct), so injection shapes like a
        // trailing backslash (MySQL escape) cannot smuggle SQL.
        for hostile in ["o'brien", "x\\", "x' UNION SELECT 1 --", "x\0"] {
            let filter = ColumnInventoryFilter {
                schemas: Some(vec!["main".to_string()]),
                tables: Some(vec!["users".to_string(), hostile.to_string()]),
            };
            let sql = compose_columns_sql("SELECT 1", &filter);
            assert_eq!(
                sql,
                "SELECT * FROM (SELECT 1\n) AS columns_inventory \
                 WHERE schema_name IN ('main')",
                "hostile table value {hostile:?} must drop the table restriction"
            );
        }

        let filter = ColumnInventoryFilter {
            schemas: Some(vec!["ma'in".to_string()]),
            tables: None,
        };
        assert_eq!(
            compose_columns_sql("SELECT 1", &filter),
            "SELECT 1",
            "hostile schema value with no other predicate returns the base query"
        );
    }

    async fn sqlite_fetcher(db_path: &std::path::Path) -> Arc<dyn DatabaseColumnFetcher> {
        let pool = SqliteConnectionPoolFactory::new(
            &db_path.to_string_lossy(),
            Mode::File,
            Duration::from_secs(5),
        )
        .build()
        .await
        .expect("sqlite pool");
        let pool: Pool<_, _> = Arc::new(pool);
        DatabaseColumnInventoryFetcher::new(&pool, SQLITE_COLUMNS_SQL)
    }

    fn sqlite_fixture(dir: &std::path::Path) -> std::path::PathBuf {
        let db_path = dir.join("columns.sqlite");
        let conn = rusqlite::Connection::open(&db_path).expect("sqlite db");
        conn.execute_batch(
            "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE orders (order_id INTEGER PRIMARY KEY, user_id INTEGER, note TEXT);
            CREATE VIEW named_users AS SELECT name FROM users;
            ",
        )
        .expect("sqlite fixture");
        db_path
    }

    #[tokio::test]
    async fn sqlite_inventory_fetches_all_columns_in_one_query() {
        let temp = tempfile::tempdir().expect("temp dir");
        let fetcher = sqlite_fetcher(&sqlite_fixture(temp.path())).await;

        let rows = fetcher
            .fetch_columns(&ColumnInventoryFilter::default())
            .await
            .expect("fetch all columns");

        let described = rows
            .iter()
            .map(|row| {
                format!(
                    "{}.{}.{}:{}",
                    row.schema_name, row.table_name, row.column_name, row.ordinal_position
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            described,
            [
                "main.named_users.name:0",
                "main.orders.order_id:0",
                "main.orders.user_id:1",
                "main.orders.note:2",
                "main.users.id:0",
                "main.users.name:1",
            ],
            "inventory should cover tables and views with zero-based ordinals"
        );

        let name = rows
            .iter()
            .find(|row| row.table_name == "users" && row.column_name == "name")
            .expect("users.name row");
        assert_eq!(name.data_type, "TEXT");
        assert!(!name.is_nullable, "NOT NULL column reports non-nullable");
        let user_id = rows
            .iter()
            .find(|row| row.table_name == "orders" && row.column_name == "user_id")
            .expect("orders.user_id row");
        assert!(user_id.is_nullable);
    }

    #[tokio::test]
    async fn sqlite_inventory_applies_schema_and_table_filters() {
        let temp = tempfile::tempdir().expect("temp dir");
        let fetcher = sqlite_fetcher(&sqlite_fixture(temp.path())).await;

        let rows = fetcher
            .fetch_columns(&ColumnInventoryFilter {
                schemas: Some(vec!["main".to_string()]),
                tables: Some(vec!["users".to_string()]),
            })
            .await
            .expect("fetch filtered columns");
        assert_eq!(
            rows.iter()
                .map(|row| row.column_name.as_str())
                .collect::<Vec<_>>(),
            ["id", "name"]
        );

        let rows = fetcher
            .fetch_columns(&ColumnInventoryFilter {
                schemas: Some(vec!["other".to_string()]),
                tables: None,
            })
            .await
            .expect("fetch mismatched schema");
        assert!(rows.is_empty(), "non-'main' schema filter matches nothing");

        let rows = fetcher
            .fetch_columns(&ColumnInventoryFilter {
                schemas: None,
                tables: Some(Vec::new()),
            })
            .await
            .expect("fetch with empty table list");
        assert!(rows.is_empty(), "empty restriction short-circuits");
    }
}
