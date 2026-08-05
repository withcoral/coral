//! `DataFusion` catalog adapter for a normalized relational database inventory.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::TableType;
use datafusion::sql::TableReference;
use datafusion::sql::unparser::dialect::Dialect;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::query_arrow;
use datafusion_table_providers::sql::sql_provider_datafusion::SqlTable;
use futures::TryStreamExt as _;

type Pool<T, P> = Arc<dyn DbConnectionPool<T, P> + Send + Sync>;
type SqlDialect = Arc<dyn Dialect + Send + Sync>;

pub(super) struct DatabaseCatalog {
    pub(super) provider: Arc<dyn CatalogProvider>,
    pub(super) relations: Vec<DatabaseRelation>,
}

#[derive(Clone, Debug)]
pub(super) struct DatabaseRelation {
    pub(super) schema_name: String,
    pub(super) table_name: String,
    table_type: TableType,
}

pub(super) async fn build_database_catalog<T: 'static, P: 'static>(
    pool: Pool<T, P>,
    inventory_sql: &str,
    dialect: SqlDialect,
) -> DataFusionResult<DatabaseCatalog> {
    let relations = load_database_inventory(&pool, inventory_sql).await?;
    let provider = Arc::new(MemoryCatalogProvider::new());
    register_database_schemas(provider.as_ref(), &pool, &relations, &dialect)?;
    Ok(DatabaseCatalog {
        provider,
        relations,
    })
}

fn register_database_schemas<T: 'static, P: 'static>(
    catalog: &MemoryCatalogProvider,
    pool: &Pool<T, P>,
    relations: &[DatabaseRelation],
    dialect: &SqlDialect,
) -> DataFusionResult<()> {
    let mut relations_by_schema = BTreeMap::<String, BTreeMap<String, TableType>>::new();
    for relation in relations {
        relations_by_schema
            .entry(relation.schema_name.clone())
            .or_default()
            .insert(relation.table_name.clone(), relation.table_type);
    }
    for (schema_name, tables) in relations_by_schema {
        let schema = LazyDatabaseSchemaProvider {
            schema_name: schema_name.clone(),
            tables,
            pool: Arc::clone(pool),
            dialect: Arc::clone(dialect),
        };
        catalog.register_schema(&schema_name, Arc::new(schema))?;
    }
    Ok(())
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
        let schema_names = inventory_column(&batch, "schema_name")?;
        let table_names = inventory_column(&batch, "table_name")?;
        let relation_types = inventory_column(&batch, "relation_type")?;
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

fn inventory_column<'a>(
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

struct LazyDatabaseSchemaProvider<T: 'static, P: 'static> {
    schema_name: String,
    tables: BTreeMap<String, TableType>,
    pool: Pool<T, P>,
    dialect: SqlDialect,
}

impl<T: 'static, P: 'static> fmt::Debug for LazyDatabaseSchemaProvider<T, P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LazyDatabaseSchemaProvider")
            .field("schema_name", &self.schema_name)
            .field("tables", &self.tables)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<T: 'static, P: 'static> SchemaProvider for LazyDatabaseSchemaProvider<T, P> {
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

pub(super) fn provider_error(
    error: impl std::error::Error + Send + Sync + 'static,
) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

pub(super) fn boxed_provider_error(
    error: Box<dyn std::error::Error + Send + Sync>,
) -> DataFusionError {
    DataFusionError::External(error)
}
