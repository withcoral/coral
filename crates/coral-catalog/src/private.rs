//! The Private Provider's catalog. Coral defines it, and it holds no
//! materialized Tools: each Principal's Private Tools are rows that
//! coral-functions keeps and declares from their own source.

use crate::schema::SchemaGraphAccumulator;
use crate::{
    Error, ExecutionBinding, MaterializedTool, PreparedProviderCatalog, SchemaGraph, SchemaId,
    SchemaSemantics, ToolHints, ToolKey, ToolPresentation,
};

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

/// A Private Tool's Execution Binding. Coral performs the Tool by running the
/// definition it names, so it carries no destination, credential, or
/// routing data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeExecutionBinding {
    definition: String,
    revision: u64,
}

impl CodeExecutionBinding {
    /// The binding of revision `revision` of the Private Tool whose stable
    /// identity is `definition`.
    #[must_use]
    pub const fn new(definition: String, revision: u64) -> Self {
        Self {
            definition,
            revision,
        }
    }

    /// Returns the stable identity of the Private Tool whose definition
    /// Coral performs.
    #[must_use]
    pub fn definition(&self) -> &str {
        &self.definition
    }

    /// Returns the revision of that definition.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// The Private Provider's schema graph and its one schema, which every
/// Private Tool's arguments and result meet: a JSON value.
fn json_value_schema() -> Result<(SchemaGraph, SchemaId), Error> {
    let invalid = |diagnostic: crate::CatalogDiagnostic| {
        Error::InvalidInput(diagnostic.message().to_string())
    };
    let mut schemas = SchemaGraphAccumulator::new(SchemaSemantics::JsonSchema2020_12);
    let schema = schemas
        .add_embedded_schema(JSON_VALUE_SCHEMA_SOURCE, &serde_json::json!({}))
        .map_err(invalid)?;
    Ok((schemas.finish().map_err(invalid)?, schema))
}

/// The Tool an Invocation of a Private Tool retains: the Private Tool at
/// `relative_path` under `coral.private`, keyed by its stable identity and
/// performed by running the definition `binding` names. Its author declared
/// `declared_default` for that revision, which applies when no exact rule
/// matches.
///
/// # Errors
///
/// Returns an invalid-input failure when the binding names no identity.
pub fn private_tool(
    relative_path: &str,
    binding: CodeExecutionBinding,
    declared_default: crate::ToolAccessState,
) -> Result<MaterializedTool, Error> {
    let (_graph, input_schema) = json_value_schema()?;
    let key = ToolKey::parse(binding.definition())?;
    let description = format!("The Private Tool coral.private.{relative_path}.");
    Ok(MaterializedTool::new(
        key,
        relative_path.to_owned(),
        description.clone(),
        ToolPresentation::new(None, description, false, Vec::new(), None),
        ToolHints::default(),
        ExecutionBinding::Code(binding),
        // No catalog stores a code binding.
        String::new(),
        input_schema,
        "{}".to_owned(),
        "{}".to_owned(),
        String::new(),
        Vec::new(),
        Vec::new(),
        "[]".to_owned(),
    )
    .declaring_default(declared_default))
}

/// The Private Provider's catalog, with no Tools. Its schema graph holds the
/// one schema every Private Tool's arguments and result meet: a JSON value.
pub(crate) fn prepared() -> Result<PreparedProviderCatalog, Error> {
    let (schema_graph, _schema) = json_value_schema()?;
    let schema_graph_json = schema_graph.to_storage_json()?;
    Ok(PreparedProviderCatalog::without_tools(
        PRIVATE_PROVIDER_DISPLAY_NAME.to_string(),
        schema_graph,
        schema_graph_json,
        TYPESCRIPT_VIEW.to_string(),
    ))
}
