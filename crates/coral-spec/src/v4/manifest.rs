use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::backends::http::{AuthSpec, RateLimitSpec};
use crate::backends::mcp::{McpServerSpec, validate_mcp_server};
use crate::inputs::{
    collect_declared_inputs, validate_input_references,
    validate_oauth_endpoint_templates_with_scope,
};
use crate::{
    DatabaseConnectionSpec, DatabaseProvider, HeaderSpec, ManifestError, ManifestInputSpec,
    MySqlConnectionSpec, ParsedTemplate, PostgresConnectionSpec, Result, SqliteConnectionSpec,
    TemplateNamespace, validate_source_name, validate_test_queries,
};

#[derive(Debug, Clone)]
pub struct V4SourceManifest {
    pub common: V4SourceCommon,
    pub surface: V4Surface,
    pub declared_inputs: Vec<ManifestInputSpec>,
}

#[derive(Debug, Clone)]
pub struct V4SourceCommon {
    pub dsl_version: u32,
    pub name: String,
    pub description: String,
    pub test_queries: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct V4Surface {
    pub surface_type: SurfaceType,
    pub descriptor: SurfaceDescriptor,
    pub runtime: SurfaceRuntimeConfig,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SurfaceType {
    OpenApi,
    Mcp,
    Database,
}

#[derive(Debug, Clone)]
pub enum SurfaceDescriptor {
    Url { url: String },
    File { file: PathBuf },
    McpServer { location: String },
    Database { provider: DatabaseProvider },
}

impl SurfaceDescriptor {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Url { .. } => "url",
            Self::File { .. } => "file",
            Self::McpServer { .. } => "mcp_server",
            Self::Database { .. } => "database",
        }
    }

    pub fn location(&self) -> String {
        match self {
            Self::Url { url, .. } => url.clone(),
            Self::File { file, .. } => file.display().to_string(),
            Self::McpServer { location } => location.clone(),
            Self::Database { provider } => provider.as_str().to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum SurfaceRuntimeConfig {
    OpenApi(OpenApiRuntimeConfig),
    Mcp(McpRuntimeConfig),
    Database(DatabaseRuntimeConfig),
}

#[derive(Debug, Clone)]
pub struct OpenApiRuntimeConfig {
    pub base_url: ParsedTemplate,
    pub auth: AuthSpec,
    pub request_headers: Vec<HeaderSpec>,
    pub rate_limit: RateLimitSpec,
}

#[derive(Debug, Clone)]
pub struct McpRuntimeConfig {
    pub server: McpServerSpec,
}

#[derive(Debug, Clone)]
pub struct DatabaseRuntimeConfig {
    pub connection: DatabaseConnectionSpec,
}

impl V4Surface {
    pub fn openapi_runtime(&self) -> Option<&OpenApiRuntimeConfig> {
        match &self.runtime {
            SurfaceRuntimeConfig::OpenApi(runtime) => Some(runtime),
            SurfaceRuntimeConfig::Mcp(_) | SurfaceRuntimeConfig::Database(_) => None,
        }
    }

    pub fn mcp_runtime(&self) -> Option<&McpRuntimeConfig> {
        match &self.runtime {
            SurfaceRuntimeConfig::Mcp(runtime) => Some(runtime),
            SurfaceRuntimeConfig::OpenApi(_) | SurfaceRuntimeConfig::Database(_) => None,
        }
    }

    pub fn database_runtime(&self) -> Option<&DatabaseRuntimeConfig> {
        match &self.runtime {
            SurfaceRuntimeConfig::Database(runtime) => Some(runtime),
            SurfaceRuntimeConfig::OpenApi(_) | SurfaceRuntimeConfig::Mcp(_) => None,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawV4SourceManifest {
    dsl_version: u32,
    name: String,
    // Keep this placeholder so `deny_unknown_fields` accepts `inputs`; the
    // declarations are parsed from the preserved raw value to retain ordering.
    #[serde(default, rename = "inputs")]
    _inputs: Option<Value>,
    #[serde(default)]
    description: String,
    #[serde(default)]
    test_queries: Vec<String>,
    surface: RawV4Surface,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawV4Surface {
    #[serde(rename = "type")]
    surface_type: RawSurfaceType,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    file: Option<PathBuf>,
    #[serde(default)]
    base_url: Option<ParsedTemplate>,
    #[serde(default)]
    auth: AuthSpec,
    #[serde(default)]
    request_headers: Vec<HeaderSpec>,
    #[serde(default)]
    rate_limit: RateLimitSpec,
    #[serde(default)]
    server: Option<McpServerSpec>,
    #[serde(default)]
    provider: Option<DatabaseProvider>,
    #[serde(default)]
    connection: Option<Value>,
}

#[derive(Debug, Deserialize)]
enum RawSurfaceType {
    #[serde(rename = "openapi")]
    OpenApi,
    #[serde(rename = "mcp")]
    Mcp,
    #[serde(rename = "database")]
    Database,
}

impl V4SourceManifest {
    pub(crate) fn parse_manifest_value(value: Value) -> Result<Self> {
        let raw_value = value.clone();
        let raw: RawV4SourceManifest =
            serde_json::from_value(value).map_err(ManifestError::deserialize)?;
        let RawV4SourceManifest {
            dsl_version,
            name,
            description,
            test_queries,
            surface,
            ..
        } = raw;
        if dsl_version != 4 {
            return Err(ManifestError::validation(format!(
                "source '{name}' declares dsl_version {dsl_version}; expected 4"
            )));
        }
        validate_v4_source_name(&name)?;
        validate_test_queries(&name, &test_queries)?;
        let common = V4SourceCommon {
            dsl_version,
            name: name.clone(),
            description,
            test_queries,
        };
        let surface_value = raw_value
            .get("surface")
            .ok_or_else(|| ManifestError::validation("v4 manifest is missing surface"))?;
        let declared_inputs = collect_declared_inputs(&raw_value)?;
        validate_input_references(surface_value, &declared_inputs)?;
        validate_oauth_endpoint_templates_with_scope(&declared_inputs, "top-level inputs")?;
        let surface = parse_surface(&name, surface, surface_value, &declared_inputs)?;

        Ok(Self {
            common,
            surface,
            declared_inputs,
        })
    }
}

fn parse_surface(
    source_name: &str,
    raw_surface: RawV4Surface,
    surface_value: &Value,
    inputs: &[ManifestInputSpec],
) -> Result<V4Surface> {
    match raw_surface.surface_type {
        RawSurfaceType::OpenApi => parse_openapi_surface(source_name, raw_surface, inputs),
        RawSurfaceType::Mcp => parse_mcp_surface(source_name, raw_surface, surface_value, inputs),
        RawSurfaceType::Database => parse_database_surface(source_name, raw_surface, surface_value),
    }
}

fn parse_openapi_surface(
    source_name: &str,
    raw_surface: RawV4Surface,
    inputs: &[ManifestInputSpec],
) -> Result<V4Surface> {
    if raw_surface.server.is_some()
        || raw_surface.provider.is_some()
        || raw_surface.connection.is_some()
    {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' OpenAPI surface must not declare server, provider, or connection"
        )));
    }
    if let Some(base_url) = raw_surface.base_url.as_ref() {
        validate_openapi_base_url_template(source_name, inputs, base_url, "authored")?;
    }
    let descriptor = parse_openapi_descriptor(source_name, &raw_surface)?;
    Ok(V4Surface {
        surface_type: SurfaceType::OpenApi,
        descriptor,
        runtime: SurfaceRuntimeConfig::OpenApi(OpenApiRuntimeConfig {
            base_url: raw_surface
                .base_url
                .unwrap_or_else(|| ParsedTemplate::parse("").expect("empty template")),
            auth: raw_surface.auth,
            request_headers: raw_surface.request_headers,
            rate_limit: raw_surface.rate_limit,
        }),
    })
}

fn parse_mcp_surface(
    source_name: &str,
    raw_surface: RawV4Surface,
    surface_value: &Value,
    inputs: &[ManifestInputSpec],
) -> Result<V4Surface> {
    if raw_surface.url.is_some() || raw_surface.file.is_some() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' MCP surface must not declare url or file"
        )));
    }
    if raw_surface.provider.is_some() || raw_surface.connection.is_some() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' MCP surface must not declare provider or connection"
        )));
    }
    for field in ["base_url", "auth", "request_headers", "rate_limit"] {
        if surface_value.get(field).is_some() {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' MCP surface must not declare OpenAPI field '{field}'"
            )));
        }
    }
    let server = raw_surface.server.ok_or_else(|| {
        ManifestError::validation(format!(
            "source '{source_name}' MCP surface must declare server"
        ))
    })?;
    validate_mcp_server(source_name, &server, inputs)?;
    Ok(V4Surface {
        surface_type: SurfaceType::Mcp,
        descriptor: SurfaceDescriptor::McpServer {
            location: mcp_server_location(&server),
        },
        runtime: SurfaceRuntimeConfig::Mcp(McpRuntimeConfig { server }),
    })
}

fn parse_database_surface(
    source_name: &str,
    raw_surface: RawV4Surface,
    surface_value: &Value,
) -> Result<V4Surface> {
    if raw_surface.url.is_some() || raw_surface.file.is_some() || raw_surface.server.is_some() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' database surface must not declare url, file, or server"
        )));
    }
    for field in ["base_url", "auth", "request_headers", "rate_limit"] {
        if surface_value.get(field).is_some() {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' database surface must not declare OpenAPI field '{field}'"
            )));
        }
    }
    let provider = raw_surface.provider.ok_or_else(|| {
        ManifestError::validation(format!(
            "source '{source_name}' database surface must declare provider"
        ))
    })?;
    let connection = raw_surface.connection.ok_or_else(|| {
        ManifestError::validation(format!(
            "source '{source_name}' database surface must declare connection"
        ))
    })?;
    let connection = parse_database_connection(source_name, provider, connection)?;
    validate_database_connection_templates(source_name, &connection)?;
    Ok(V4Surface {
        surface_type: SurfaceType::Database,
        descriptor: SurfaceDescriptor::Database { provider },
        runtime: SurfaceRuntimeConfig::Database(DatabaseRuntimeConfig { connection }),
    })
}

fn validate_database_connection_templates(
    source_name: &str,
    connection: &DatabaseConnectionSpec,
) -> Result<()> {
    visit_database_connection_templates(connection, |field, template| {
        validate_database_connection_template(source_name, field, template)
    })
}

fn visit_database_connection_templates(
    connection: &DatabaseConnectionSpec,
    mut visit: impl FnMut(&str, &ParsedTemplate) -> Result<()>,
) -> Result<()> {
    match connection {
        DatabaseConnectionSpec::Postgres(connection) => {
            for (field, template) in [
                ("host", &connection.host),
                ("port", &connection.port),
                ("database", &connection.database),
                ("user", &connection.user),
                ("password", &connection.password),
            ] {
                visit(field, template)?;
            }
            if let Some(sslmode) = &connection.sslmode {
                visit("sslmode", sslmode)?;
            }
        }
        DatabaseConnectionSpec::MySql(connection) => {
            for (field, template) in [
                ("host", &connection.host),
                ("port", &connection.port),
                ("database", &connection.database),
                ("user", &connection.user),
                ("password", &connection.password),
            ] {
                visit(field, template)?;
            }
        }
        DatabaseConnectionSpec::Sqlite(connection) => visit("path", &connection.path)?,
    }
    Ok(())
}

fn validate_database_connection_template(
    source_name: &str,
    field: &str,
    template: &ParsedTemplate,
) -> Result<()> {
    for token in template.tokens() {
        match token.namespace() {
            TemplateNamespace::Input => {}
            _ => {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' database surface connection.{field} may only reference top-level inputs; unsupported template token '{{{{{}}}}}'",
                    token.raw()
                )));
            }
        }
    }
    Ok(())
}

fn parse_database_connection(
    source_name: &str,
    provider: DatabaseProvider,
    connection: Value,
) -> Result<DatabaseConnectionSpec> {
    match provider {
        DatabaseProvider::Postgres => serde_json::from_value::<PostgresConnectionSpec>(connection)
            .map(DatabaseConnectionSpec::Postgres),
        DatabaseProvider::MySql => serde_json::from_value::<MySqlConnectionSpec>(connection)
            .map(DatabaseConnectionSpec::MySql),
        DatabaseProvider::Sqlite => serde_json::from_value::<SqliteConnectionSpec>(connection)
            .map(DatabaseConnectionSpec::Sqlite),
    }
    .map_err(|error| {
        ManifestError::validation(format!(
            "source '{source_name}' database surface connection is invalid for provider '{}': {error}",
            provider.as_str()
        ))
    })
}

fn validate_v4_source_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    let valid = matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if !valid {
        return Err(ManifestError::validation(format!(
            "source name '{name}' must match [a-z][a-z0-9_]*"
        )));
    }
    validate_source_name(name)
}

fn mcp_server_location(server: &McpServerSpec) -> String {
    match server {
        McpServerSpec::Stdio { command, .. } => command.clone(),
        McpServerSpec::StreamableHttp { url, .. } => url.clone(),
    }
}

fn parse_openapi_descriptor(
    source_name: &str,
    surface: &RawV4Surface,
) -> Result<SurfaceDescriptor> {
    match (&surface.url, &surface.file) {
        (Some(url), None) => {
            if !url.starts_with("https://") {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' surface url descriptors must use https"
                )));
            }
            Ok(SurfaceDescriptor::Url { url: url.clone() })
        }
        (None, Some(file)) => Ok(SurfaceDescriptor::File { file: file.clone() }),
        (Some(_), Some(_)) | (None, None) => Err(ManifestError::validation(format!(
            "source '{source_name}' surface must declare exactly one of url or file"
        ))),
    }
}

pub fn validate_openapi_base_url_template(
    source_name: &str,
    inputs: &[ManifestInputSpec],
    base_url: &ParsedTemplate,
    provenance: &str,
) -> Result<()> {
    let provenance = if provenance.is_empty() {
        String::new()
    } else {
        format!("{provenance} ")
    };
    for token in base_url.tokens() {
        match token.namespace() {
            TemplateNamespace::Input => {
                if token.default_value().is_some() {
                    return Err(ManifestError::validation(format!(
                        "source '{source_name}' surface {provenance}base_url input token '{{{{{}}}}}' must declare defaults under top-level inputs",
                        token.raw()
                    )));
                }
                if !inputs.iter().any(|input| input.key == token.key()) {
                    return Err(ManifestError::validation(format!(
                        "source '{source_name}' surface {provenance}base_url references undeclared input '{}'",
                        token.key()
                    )));
                }
            }
            _ => {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' surface {provenance}base_url may only reference top-level inputs; unsupported template token '{{{{{}}}}}'",
                    token.raw()
                )));
            }
        }
    }
    Ok(())
}
