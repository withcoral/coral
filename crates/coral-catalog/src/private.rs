//! The Private Provider's catalog. Coral defines it, and it holds no
//! materialized Tools: each Principal's Private Tools are rows that
//! coral-functions keeps and declares from their own source.

use crate::schema::SchemaGraphAccumulator;
use crate::{Error, PreparedProviderCatalog, SchemaSemantics};

/// The display name of every Workspace's Private Provider.
pub const PRIVATE_PROVIDER_DISPLAY_NAME: &str = "Private";

/// The input every Private Provider Generation pins.
pub(crate) const PRIVATE_CATALOG_IDENTITY: &str = "coral.private";

/// The materializer that wrote a Private Provider Generation.
pub(crate) const MATERIALIZER_VERSION: &str = "coral-private-v1";

/// The source of the catalog's one schema, any JSON value.
const JSON_VALUE_SCHEMA_SOURCE: &str = "coral.private#/json";

/// The catalog's TypeScript view: an empty module, since the catalog
/// declares no Tool of its own.
const TYPESCRIPT_VIEW: &str = "export {};";

/// The Private Provider's catalog, with no Tools. Its schema graph holds the
/// one schema every Private Tool's arguments and result meet: a JSON value.
pub(crate) fn prepared() -> Result<PreparedProviderCatalog, Error> {
    let invalid = |diagnostic: crate::CatalogDiagnostic| {
        Error::InvalidInput(diagnostic.message().to_string())
    };
    let mut schemas = SchemaGraphAccumulator::new(SchemaSemantics::JsonSchema2020_12);
    schemas
        .add_embedded_schema(JSON_VALUE_SCHEMA_SOURCE, &serde_json::json!({}))
        .map_err(invalid)?;
    let schema_graph = schemas.finish().map_err(invalid)?;
    let schema_graph_json = schema_graph.to_storage_json()?;
    Ok(PreparedProviderCatalog::without_tools(
        PRIVATE_PROVIDER_DISPLAY_NAME.to_string(),
        schema_graph,
        schema_graph_json,
        TYPESCRIPT_VIEW.to_string(),
    ))
}
