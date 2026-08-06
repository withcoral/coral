//! Closed backend-specific runtime catalog contracts.

#![allow(
    dead_code,
    reason = "The catalog contract lands before RuntimeSourcePackage switches to consuming it."
)]

use coral_spec::backends::database::DatabaseConnectionSpec;
use coral_spec::backends::file::{FileSourceManifest, FileTableSpec};
use coral_spec::backends::http::{AuthSpec, HttpSourceManifest, HttpTableSpec, RateLimitSpec};
use coral_spec::backends::mcp::{
    McpServerSpec, McpSourceManifest, McpTableFunctionSpec, McpTableSpec,
};
use coral_spec::{HeaderSpec, ParsedTemplate, SourceTableFunctionSpec, SqlObjectName};

/// Source-wide HTTP execution configuration without a relation inventory.
#[derive(Debug, Clone)]
pub struct HttpRuntimeBackend {
    dsl_version: u32,
    base_url: ParsedTemplate,
    auth: AuthSpec,
    request_headers: Vec<HeaderSpec>,
    rate_limit: RateLimitSpec,
}

/// Source-wide MCP execution configuration without a relation inventory.
#[derive(Debug, Clone)]
pub struct McpRuntimeBackend {
    dsl_version: u32,
    server: McpServerSpec,
}

/// Source-wide file execution configuration without a relation inventory.
#[derive(Debug, Clone, Copy)]
pub struct FileRuntimeBackend {
    dsl_version: u32,
}

/// Source-wide database execution configuration.
#[derive(Debug, Clone)]
pub struct DatabaseRuntimeBackend {
    dsl_version: u32,
    connection: DatabaseConnectionSpec,
}

/// One declared HTTP relation with a complete SQL identity.
#[derive(Debug, Clone)]
pub enum HttpRuntimeRelation {
    /// HTTP-backed table.
    Table(HttpRuntimeTableRelation),
    /// HTTP-backed table function.
    TableFunction(HttpRuntimeTableFunctionRelation),
}

/// Validated HTTP table payload paired with its SQL identity.
#[derive(Debug, Clone)]
pub struct HttpRuntimeTableRelation {
    sql_name: SqlObjectName,
    definition: HttpTableSpec,
}

/// Validated HTTP table-function payload paired with its SQL identity.
#[derive(Debug, Clone)]
pub struct HttpRuntimeTableFunctionRelation {
    sql_name: SqlObjectName,
    definition: SourceTableFunctionSpec,
}

/// One declared MCP relation with a complete SQL identity.
#[derive(Debug, Clone)]
pub enum McpRuntimeRelation {
    /// MCP-backed table.
    Table(McpRuntimeTableRelation),
    /// MCP-backed table function.
    TableFunction(McpRuntimeTableFunctionRelation),
}

/// Validated MCP table payload paired with its SQL identity.
#[derive(Debug, Clone)]
pub struct McpRuntimeTableRelation {
    sql_name: SqlObjectName,
    definition: McpTableSpec,
}

/// Validated MCP table-function payload paired with its SQL identity.
#[derive(Debug, Clone)]
pub struct McpRuntimeTableFunctionRelation {
    sql_name: SqlObjectName,
    definition: McpTableFunctionSpec,
}

/// One declared file relation with a complete SQL identity.
#[derive(Debug, Clone)]
pub enum FileRuntimeRelation {
    /// File-backed table.
    Table(FileRuntimeTableRelation),
}

/// Validated file table payload paired with its SQL identity.
#[derive(Debug, Clone)]
pub struct FileRuntimeTableRelation {
    sql_name: SqlObjectName,
    definition: FileTableSpec,
}

/// A declared runtime catalog whose complete relation inventory is known.
#[derive(Debug, Clone)]
pub enum DeclaredRuntimeCatalog {
    /// HTTP-backed declared catalog.
    Http(HttpDeclaredRuntimeCatalog),
    /// MCP-backed declared catalog.
    Mcp(McpDeclaredRuntimeCatalog),
    /// File-backed declared catalog.
    File(FileDeclaredRuntimeCatalog),
}

/// HTTP declared-catalog payload with private validated fields.
#[derive(Debug, Clone)]
pub struct HttpDeclaredRuntimeCatalog {
    catalog_name: String,
    backend: HttpRuntimeBackend,
    relations: Vec<HttpRuntimeRelation>,
}

/// MCP declared-catalog payload with private validated fields.
#[derive(Debug, Clone)]
pub struct McpDeclaredRuntimeCatalog {
    catalog_name: String,
    backend: McpRuntimeBackend,
    relations: Vec<McpRuntimeRelation>,
}

/// File declared-catalog payload with private validated fields.
#[derive(Debug, Clone)]
pub struct FileDeclaredRuntimeCatalog {
    catalog_name: String,
    backend: FileRuntimeBackend,
    relations: Vec<FileRuntimeRelation>,
}

/// A runtime catalog whose provider discovers its schemas and relations.
#[derive(Debug, Clone)]
pub enum ProviderDiscoveredRuntimeCatalog {
    /// Database-backed provider-discovered catalog.
    Database(DatabaseProviderDiscoveredRuntimeCatalog),
}

/// Database provider-discovered payload with private validated fields.
#[derive(Debug, Clone)]
pub struct DatabaseProviderDiscoveredRuntimeCatalog {
    catalog_name: String,
    backend: DatabaseRuntimeBackend,
}

/// Closed runtime catalog algebra accepted at the app-to-engine boundary.
#[derive(Debug, Clone)]
pub enum RuntimeCatalog {
    /// Complete, app-declared relation inventory.
    Declared(DeclaredRuntimeCatalog),
    /// Relation inventory discovered from the runtime provider.
    ProviderDiscovered(ProviderDiscoveredRuntimeCatalog),
}
impl HttpRuntimeBackend {
    /// Builds source-wide HTTP runtime configuration.
    #[must_use]
    pub fn new(
        dsl_version: u32,
        base_url: ParsedTemplate,
        auth: AuthSpec,
        request_headers: Vec<HeaderSpec>,
        rate_limit: RateLimitSpec,
    ) -> Self {
        Self {
            dsl_version,
            base_url,
            auth,
            request_headers,
            rate_limit,
        }
    }
}

impl McpRuntimeBackend {
    /// Builds source-wide MCP runtime configuration.
    #[must_use]
    pub fn new(dsl_version: u32, server: McpServerSpec) -> Self {
        Self {
            dsl_version,
            server,
        }
    }
}

impl FileRuntimeBackend {
    /// Builds source-wide file runtime configuration.
    #[must_use]
    pub fn new(dsl_version: u32) -> Self {
        Self { dsl_version }
    }
}

impl DatabaseRuntimeBackend {
    /// Builds source-wide database runtime configuration.
    #[must_use]
    pub fn new(dsl_version: u32, connection: DatabaseConnectionSpec) -> Self {
        Self {
            dsl_version,
            connection,
        }
    }
}

impl HttpRuntimeRelation {
    /// Builds an HTTP table relation after checking its leaf identity.
    pub fn try_table(
        sql_name: SqlObjectName,
        definition: HttpTableSpec,
    ) -> Result<Self, crate::CoreError> {
        validate_definition_name(&sql_name, definition.name(), "HTTP table")?;
        Ok(Self::Table(HttpRuntimeTableRelation {
            sql_name,
            definition,
        }))
    }

    /// Builds an HTTP table-function relation after checking its leaf identity.
    pub fn try_table_function(
        sql_name: SqlObjectName,
        definition: SourceTableFunctionSpec,
    ) -> Result<Self, crate::CoreError> {
        validate_definition_name(&sql_name, &definition.name, "HTTP table function")?;
        Ok(Self::TableFunction(HttpRuntimeTableFunctionRelation {
            sql_name,
            definition,
        }))
    }

    fn sql_name(&self) -> &SqlObjectName {
        match self {
            Self::Table(relation) => &relation.sql_name,
            Self::TableFunction(relation) => &relation.sql_name,
        }
    }
}

impl McpRuntimeRelation {
    /// Builds an MCP table relation after checking its leaf identity.
    pub fn try_table(
        sql_name: SqlObjectName,
        definition: McpTableSpec,
    ) -> Result<Self, crate::CoreError> {
        validate_definition_name(&sql_name, definition.name(), "MCP table")?;
        Ok(Self::Table(McpRuntimeTableRelation {
            sql_name,
            definition,
        }))
    }

    /// Builds an MCP table-function relation after checking its leaf identity.
    pub fn try_table_function(
        sql_name: SqlObjectName,
        definition: McpTableFunctionSpec,
    ) -> Result<Self, crate::CoreError> {
        validate_definition_name(&sql_name, &definition.common.name, "MCP table function")?;
        Ok(Self::TableFunction(McpRuntimeTableFunctionRelation {
            sql_name,
            definition,
        }))
    }

    fn sql_name(&self) -> &SqlObjectName {
        match self {
            Self::Table(relation) => &relation.sql_name,
            Self::TableFunction(relation) => &relation.sql_name,
        }
    }
}

impl FileRuntimeRelation {
    /// Builds a file table relation after checking its leaf identity.
    pub fn try_table(
        sql_name: SqlObjectName,
        definition: FileTableSpec,
    ) -> Result<Self, crate::CoreError> {
        validate_definition_name(&sql_name, definition.name(), "file table")?;
        Ok(Self::Table(FileRuntimeTableRelation {
            sql_name,
            definition,
        }))
    }

    fn sql_name(&self) -> &SqlObjectName {
        match self {
            Self::Table(relation) => &relation.sql_name,
        }
    }
}

impl RuntimeCatalog {
    /// Builds a declared HTTP catalog.
    pub fn try_http_declared(
        catalog_name: impl Into<String>,
        backend: HttpRuntimeBackend,
        relations: Vec<HttpRuntimeRelation>,
    ) -> Result<Self, crate::CoreError> {
        let catalog_name = catalog_name.into();
        validate_declared_catalog(
            &catalog_name,
            relations.iter().map(HttpRuntimeRelation::sql_name),
        )?;
        Ok(Self::Declared(DeclaredRuntimeCatalog::Http(
            HttpDeclaredRuntimeCatalog {
                catalog_name,
                backend,
                relations,
            },
        )))
    }

    /// Builds a declared MCP catalog.
    pub fn try_mcp_declared(
        catalog_name: impl Into<String>,
        backend: McpRuntimeBackend,
        relations: Vec<McpRuntimeRelation>,
    ) -> Result<Self, crate::CoreError> {
        let catalog_name = catalog_name.into();
        validate_declared_catalog(
            &catalog_name,
            relations.iter().map(McpRuntimeRelation::sql_name),
        )?;
        Ok(Self::Declared(DeclaredRuntimeCatalog::Mcp(
            McpDeclaredRuntimeCatalog {
                catalog_name,
                backend,
                relations,
            },
        )))
    }

    /// Builds a declared file catalog.
    pub fn try_file_declared(
        catalog_name: impl Into<String>,
        backend: FileRuntimeBackend,
        relations: Vec<FileRuntimeRelation>,
    ) -> Result<Self, crate::CoreError> {
        let catalog_name = catalog_name.into();
        validate_declared_catalog(
            &catalog_name,
            relations.iter().map(FileRuntimeRelation::sql_name),
        )?;
        Ok(Self::Declared(DeclaredRuntimeCatalog::File(
            FileDeclaredRuntimeCatalog {
                catalog_name,
                backend,
                relations,
            },
        )))
    }

    /// Builds a provider-discovered database catalog.
    pub fn try_database_provider_discovered(
        catalog_name: impl Into<String>,
        backend: DatabaseRuntimeBackend,
    ) -> Result<Self, crate::CoreError> {
        let catalog_name = catalog_name.into();
        validate_catalog_name(&catalog_name)?;
        Ok(Self::ProviderDiscovered(
            ProviderDiscoveredRuntimeCatalog::Database(DatabaseProviderDiscoveredRuntimeCatalog {
                catalog_name,
                backend,
            }),
        ))
    }

    /// Adapts one validated v3 HTTP manifest to the canonical default catalog.
    pub fn try_from_default_catalog_http_manifest(
        manifest: HttpSourceManifest,
    ) -> Result<Self, crate::CoreError> {
        let schema_name = manifest.common.name.clone();
        let backend = HttpRuntimeBackend::new(
            manifest.common.dsl_version,
            manifest.base_url,
            manifest.auth,
            manifest.request_headers,
            manifest.rate_limit,
        );
        let mut relations = Vec::with_capacity(manifest.tables.len() + manifest.functions.len());
        for table in manifest.tables {
            let sql_name = default_catalog_sql_name(&schema_name, table.name())?;
            relations.push(HttpRuntimeRelation::try_table(sql_name, table)?);
        }
        for function in manifest.functions {
            let sql_name = default_catalog_sql_name(&schema_name, &function.name)?;
            relations.push(HttpRuntimeRelation::try_table_function(sql_name, function)?);
        }
        Self::try_http_declared("datafusion", backend, relations)
    }

    /// Adapts one validated v3 MCP manifest to the canonical default catalog.
    pub fn try_from_default_catalog_mcp_manifest(
        manifest: McpSourceManifest,
    ) -> Result<Self, crate::CoreError> {
        let schema_name = manifest.common.name.clone();
        let backend = McpRuntimeBackend::new(manifest.common.dsl_version, manifest.server);
        let mut relations = Vec::with_capacity(manifest.tables.len() + manifest.functions.len());
        for table in manifest.tables {
            let sql_name = default_catalog_sql_name(&schema_name, table.name())?;
            relations.push(McpRuntimeRelation::try_table(sql_name, table)?);
        }
        for function in manifest.functions {
            let sql_name = default_catalog_sql_name(&schema_name, &function.common.name)?;
            relations.push(McpRuntimeRelation::try_table_function(sql_name, function)?);
        }
        Self::try_mcp_declared("datafusion", backend, relations)
    }

    /// Adapts one validated v3 file manifest to the canonical default catalog.
    pub fn try_from_default_catalog_file_manifest(
        manifest: FileSourceManifest,
    ) -> Result<Self, crate::CoreError> {
        let schema_name = manifest.common.name.clone();
        let backend = FileRuntimeBackend::new(manifest.common.dsl_version);
        let relations = manifest
            .tables
            .into_iter()
            .map(|table| {
                let sql_name = default_catalog_sql_name(&schema_name, table.name())?;
                FileRuntimeRelation::try_table(sql_name, table)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::try_file_declared("datafusion", backend, relations)
    }
}

fn default_catalog_sql_name(
    schema_name: &str,
    name: &str,
) -> Result<SqlObjectName, crate::CoreError> {
    SqlObjectName::try_new("datafusion", schema_name, name)
        .map_err(|error| crate::CoreError::InvalidInput(error.to_string()))
}

fn validate_definition_name(
    sql_name: &SqlObjectName,
    definition_name: &str,
    label: &str,
) -> Result<(), crate::CoreError> {
    if sql_name.name() == definition_name {
        return Ok(());
    }
    Err(crate::CoreError::InvalidInput(format!(
        "{label} definition name '{definition_name}' does not match SQL name '{}'",
        sql_name.name()
    )))
}

fn validate_catalog_name(catalog_name: &str) -> Result<(), crate::CoreError> {
    SqlObjectName::try_new(catalog_name, "public", "relation")
        .map(|_| ())
        .map_err(|error| crate::CoreError::InvalidInput(error.to_string()))
}

fn validate_declared_catalog<'a>(
    catalog_name: &str,
    sql_names: impl Iterator<Item = &'a SqlObjectName>,
) -> Result<(), crate::CoreError> {
    validate_catalog_name(catalog_name)?;
    let mut seen = std::collections::BTreeSet::new();
    for sql_name in sql_names {
        if sql_name.catalog_name() != catalog_name {
            return Err(crate::CoreError::InvalidInput(format!(
                "declared runtime catalog '{catalog_name}' contains relation '{sql_name}' from catalog '{}'",
                sql_name.catalog_name()
            )));
        }
        if !seen.insert(sql_name) {
            return Err(crate::CoreError::InvalidInput(format!(
                "declared runtime catalog '{catalog_name}' contains duplicate relation '{sql_name}'"
            )));
        }
    }
    Ok(())
}
