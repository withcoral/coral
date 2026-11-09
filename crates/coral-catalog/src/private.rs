//! The catalogs of the two Providers whose Tools Coral performs from
//! authored source: the Private Provider and the Shared Provider. Coral
//! defines each, and neither holds a materialized Tool: each Principal's
//! Private Tools and the Workspace's Shared Tools are rows that coral-tools
//! keeps and declares from their own source.

use crate::schema::SchemaGraphAccumulator;
use crate::{
    Error, ExecutionBinding, MaterializedTool, PreparedProviderCatalog, SchemaGraph, SchemaId,
    SchemaSemantics, ToolHints, ToolKey, ToolPresentation,
};

/// The display name of every Workspace's Private Provider.
pub const PRIVATE_PROVIDER_DISPLAY_NAME: &str = "Private";

/// The display name of every Workspace's Shared Provider.
pub const SHARED_PROVIDER_DISPLAY_NAME: &str = "Shared";

/// One of the catalogs whose Tools have code Execution Bindings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodeCatalog {
    /// The Private Provider's, at `coral.private`.
    Private,
    /// The Shared Provider's, at `coral.shared`.
    Shared,
}

impl CodeCatalog {
    /// The catalog whose Generations store `input_kind`.
    pub(crate) fn from_input_kind(input_kind: &str) -> Option<Self> {
        [Self::Private, Self::Shared]
            .into_iter()
            .find(|catalog| catalog.input_kind() == input_kind)
    }

    /// The input kind its Generations store.
    pub(crate) const fn input_kind(self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::Shared => "shared",
        }
    }

    /// The input every one of its Generations pins: its Call Root.
    pub(crate) const fn identity(self) -> &'static str {
        match self {
            Self::Private => "coral.private",
            Self::Shared => "coral.shared",
        }
    }

    /// The materializer that wrote its Generations.
    pub(crate) const fn materializer_version(self) -> &'static str {
        match self {
            Self::Private => "coral-private-v1",
            Self::Shared => "coral-shared-v1",
        }
    }

    const fn display_name(self) -> &'static str {
        match self {
            Self::Private => PRIVATE_PROVIDER_DISPLAY_NAME,
            Self::Shared => SHARED_PROVIDER_DISPLAY_NAME,
        }
    }

    /// How a Tool Description names one of its Tools.
    const fn tool_kind(self) -> &'static str {
        match self {
            Self::Private => "Private Tool",
            Self::Shared => "Shared Tool",
        }
    }

    /// The source of the catalog's one schema, any JSON value.
    fn json_value_schema_source(self) -> String {
        format!("{}#/json", self.identity())
    }
}

/// The catalog's TypeScript view: an empty module, since the catalog
/// declares no Tool of its own.
const TYPESCRIPT_VIEW: &str = "export {};";

/// A Private or Shared Tool's Execution Binding. Coral performs the Tool by
/// running the definition it names, so it carries no destination,
/// credential, or routing data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeExecutionBinding {
    definition: String,
    revision: u64,
}

impl CodeExecutionBinding {
    /// The binding of revision `revision` of the Private or Shared Tool
    /// whose stable identity is `definition`.
    #[must_use]
    pub const fn new(definition: String, revision: u64) -> Self {
        Self {
            definition,
            revision,
        }
    }

    /// Returns the stable identity of the Private or Shared Tool whose
    /// definition Coral performs.
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

/// A code catalog's schema graph and its one schema, which every one of its
/// Tools' arguments and result meet: a JSON value.
fn json_value_schema(catalog: CodeCatalog) -> Result<(SchemaGraph, SchemaId), Error> {
    let invalid = |diagnostic: crate::CatalogDiagnostic| {
        Error::InvalidInput(diagnostic.message().to_string())
    };
    let mut schemas = SchemaGraphAccumulator::new(SchemaSemantics::JsonSchema2020_12);
    let schema = schemas
        .add_embedded_schema(&catalog.json_value_schema_source(), &serde_json::json!({}))
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
    code_tool(
        CodeCatalog::Private,
        relative_path,
        binding,
        declared_default,
    )
}

/// The Tool an Invocation of a Shared Tool retains: the Shared Tool at
/// `relative_path` under `coral.shared`, keyed by its stable identity and
/// performed by running the revision `binding` names. The Owner chose
/// `declared_default` when publishing that revision, which applies when no
/// exact rule matches.
///
/// # Errors
///
/// Returns an invalid-input failure when the binding names no identity.
pub fn shared_tool(
    relative_path: &str,
    binding: CodeExecutionBinding,
    declared_default: crate::ToolAccessState,
) -> Result<MaterializedTool, Error> {
    code_tool(
        CodeCatalog::Shared,
        relative_path,
        binding,
        declared_default,
    )
}

fn code_tool(
    catalog: CodeCatalog,
    relative_path: &str,
    binding: CodeExecutionBinding,
    declared_default: crate::ToolAccessState,
) -> Result<MaterializedTool, Error> {
    let (_graph, input_schema) = json_value_schema(catalog)?;
    let key = ToolKey::parse(binding.definition())?;
    let description = format!(
        "The {} {}.{relative_path}.",
        catalog.tool_kind(),
        catalog.identity()
    );
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
    .declaring_default(Some(declared_default)))
}

/// A code catalog, with no Tools. Its schema graph holds the one schema
/// every one of its Tools' arguments and result meet: a JSON value.
pub(crate) fn prepared(catalog: CodeCatalog) -> Result<PreparedProviderCatalog, Error> {
    let (schema_graph, _schema) = json_value_schema(catalog)?;
    let schema_graph_json = schema_graph.to_storage_json()?;
    Ok(PreparedProviderCatalog::without_tools(
        catalog.display_name().to_string(),
        schema_graph,
        schema_graph_json,
        TYPESCRIPT_VIEW.to_string(),
    ))
}
