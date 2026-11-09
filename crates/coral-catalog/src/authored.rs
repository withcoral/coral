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

/// The kind of a Tool that Coral performs from authored source: a Private
/// Tool or a Shared Tool. Each kind has one Provider in every Workspace,
/// whose catalog holds no materialized Tool and whose Tools have code
/// Execution Bindings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AuthoredToolKind {
    /// A Principal's own Private Tool, under `coral.private`.
    Private,
    /// The Workspace's Shared Tool, under `coral.shared`.
    Shared,
}

impl AuthoredToolKind {
    /// The Call Root of the kind's Provider, which every Call Path of the
    /// kind starts with. Its Provider's Generations pin it as their input.
    #[must_use]
    pub const fn call_root(self) -> &'static str {
        match self {
            Self::Private => "coral.private",
            Self::Shared => "coral.shared",
        }
    }

    /// How a message names a Tool of this kind: `Private Tool` or `Shared
    /// Tool`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Private => "Private Tool",
            Self::Shared => "Shared Tool",
        }
    }

    /// The complete Call Path of the Tool at `relative_path` under the
    /// kind's Call Root, such as `coral.shared.mail.remind`.
    #[must_use]
    pub fn call_path(self, relative_path: &str) -> String {
        format!("{}.{relative_path}", self.call_root())
    }

    /// How a message and a stack frame name the source of the Tool at
    /// `relative_path`, such as `Shared Tool coral.shared.mail.remind`.
    #[must_use]
    pub fn source_name(self, relative_path: &str) -> String {
        format!("{} {}", self.name(), self.call_path(relative_path))
    }

    /// The path under the kind's Call Root that `call_path` names; `None`
    /// when the kind's Call Root does not own it.
    #[must_use]
    pub fn relative_path(self, call_path: &str) -> Option<&str> {
        call_path
            .strip_prefix(self.call_root())?
            .strip_prefix('.')
            .filter(|relative| !relative.is_empty())
    }

    /// The kind whose Call Root owns `call_path`, if either does.
    #[must_use]
    pub fn of_call_path(call_path: &str) -> Option<Self> {
        [Self::Private, Self::Shared]
            .into_iter()
            .find(|kind| kind.relative_path(call_path).is_some())
    }

    /// The kind whose Provider's Generations store `input_kind`.
    pub(crate) fn from_input_kind(input_kind: &str) -> Option<Self> {
        [Self::Private, Self::Shared]
            .into_iter()
            .find(|kind| kind.input_kind() == input_kind)
    }

    /// The input kind its Provider's Generations store.
    pub(crate) const fn input_kind(self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::Shared => "shared",
        }
    }

    /// The materializer that wrote its Provider's Generations.
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

    /// The source of its catalog's one schema, any JSON value.
    fn json_value_schema_source(self) -> String {
        format!("{}#/json", self.call_root())
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
fn json_value_schema(catalog: AuthoredToolKind) -> Result<(SchemaGraph, SchemaId), Error> {
    let invalid = |diagnostic: crate::CatalogDiagnostic| {
        Error::InvalidInput(diagnostic.message().to_string())
    };
    let mut schemas = SchemaGraphAccumulator::new(SchemaSemantics::JsonSchema2020_12);
    let schema = schemas
        .add_embedded_schema(&catalog.json_value_schema_source(), &serde_json::json!({}))
        .map_err(invalid)?;
    Ok((schemas.finish().map_err(invalid)?, schema))
}

/// The Tool an Invocation of a Private or Shared Tool retains: the Tool of
/// `kind` at `relative_path` under its Call Root, keyed by its stable
/// identity and performed by running the definition `binding` names.
/// `declared_default` is the default Tool Access State of that revision,
/// which its author declared for a Private Tool and its publishing Owner
/// chose for a Shared Tool; it applies when no exact rule matches.
///
/// # Errors
///
/// Returns an invalid-input failure when the binding names no identity.
pub fn authored_tool(
    catalog: AuthoredToolKind,
    relative_path: &str,
    binding: CodeExecutionBinding,
    declared_default: crate::ToolAccessState,
) -> Result<MaterializedTool, Error> {
    let (_graph, input_schema) = json_value_schema(catalog)?;
    let key = ToolKey::parse(binding.definition())?;
    let description = format!("The {}.", catalog.source_name(relative_path));
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
pub(crate) fn prepared(catalog: AuthoredToolKind) -> Result<PreparedProviderCatalog, Error> {
    let (schema_graph, _schema) = json_value_schema(catalog)?;
    let schema_graph_json = schema_graph.to_storage_json()?;
    Ok(PreparedProviderCatalog::without_tools(
        catalog.display_name().to_string(),
        schema_graph,
        schema_graph_json,
        TYPESCRIPT_VIEW.to_string(),
    ))
}
