use std::collections::{BTreeMap, HashSet};
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
    HeaderSpec, ManifestError, ManifestInputKind, ManifestInputSpec, ParsedTemplate, Result,
    TemplateNamespace, validate_identifier, validate_source_name, validate_test_queries,
};

#[derive(Debug, Clone)]
pub struct V4SourceManifest {
    pub common: V4SourceCommon,
    /// Identity requirements that gate this source at runtime.
    pub identity_requirements: Option<IdentityRequirements>,
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
}

#[derive(Debug, Clone)]
pub enum SurfaceDescriptor {
    Url { url: String },
    File { file: PathBuf },
    McpServer { location: String },
}

impl SurfaceDescriptor {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Url { .. } => "url",
            Self::File { .. } => "file",
            Self::McpServer { .. } => "mcp_server",
        }
    }

    pub fn location(&self) -> String {
        match self {
            Self::Url { url, .. } => url.clone(),
            Self::File { file, .. } => file.display().to_string(),
            Self::McpServer { location } => location.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum SurfaceRuntimeConfig {
    OpenApi(OpenApiRuntimeConfig),
    Mcp(McpRuntimeConfig),
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

/// Identity authentication contract declared by a DSL v4 source.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct IdentityRequirements {
    /// Accepted identity alternatives. A source may be authenticated by any
    /// one entry in this list.
    pub accepts: Vec<AcceptedIdentityRequirement>,
}

/// One acceptable identity shape for a source identity requirement.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AcceptedIdentityRequirement {
    /// Stable author-chosen requirement identifier, scoped to the source.
    pub id: String,
    /// Identity spec ids that may satisfy this requirement.
    pub identity_specs: Vec<String>,
    /// Provider-specific audience constraints matched during identity
    /// binding and runtime resolution.
    #[serde(default)]
    pub audience: BTreeMap<String, Value>,
}

impl V4Surface {
    pub fn openapi_runtime(&self) -> Option<&OpenApiRuntimeConfig> {
        match &self.runtime {
            SurfaceRuntimeConfig::OpenApi(runtime) => Some(runtime),
            SurfaceRuntimeConfig::Mcp(_) => None,
        }
    }

    pub fn mcp_runtime(&self) -> Option<&McpRuntimeConfig> {
        match &self.runtime {
            SurfaceRuntimeConfig::Mcp(runtime) => Some(runtime),
            SurfaceRuntimeConfig::OpenApi(_) => None,
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
    #[serde(default)]
    identity_requirements: Option<IdentityRequirements>,
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
}

#[derive(Debug, Deserialize)]
enum RawSurfaceType {
    #[serde(rename = "openapi")]
    OpenApi,
    #[serde(rename = "mcp")]
    Mcp,
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
            mut identity_requirements,
            surface,
            ..
        } = raw;
        validate_manifest_header(&name, dsl_version, &test_queries)?;
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
        normalize_and_validate_source_identity_requirements(
            &name,
            identity_requirements.as_mut(),
            &surface,
            &declared_inputs,
        )?;

        Ok(Self {
            common,
            identity_requirements,
            surface,
            declared_inputs,
        })
    }
}

fn validate_manifest_header(
    source_name: &str,
    dsl_version: u32,
    test_queries: &[String],
) -> Result<()> {
    if dsl_version != 4 {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' declares dsl_version {dsl_version}; expected 4"
        )));
    }
    validate_v4_source_name(source_name)?;
    validate_test_queries(source_name, test_queries)
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
    }
}

fn parse_openapi_surface(
    source_name: &str,
    raw_surface: RawV4Surface,
    inputs: &[ManifestInputSpec],
) -> Result<V4Surface> {
    if raw_surface.server.is_some() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' OpenAPI surface must not declare server"
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

fn validate_identity_source_inputs(source_name: &str, inputs: &[ManifestInputSpec]) -> Result<()> {
    for input in inputs {
        if input.kind == ManifestInputKind::Secret {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' input '{}' must not use kind: secret in DSL v4; use identity_requirements and identity specs for credentials",
                input.key
            )));
        }
    }
    Ok(())
}

fn normalize_identity_requirements(requirements: &mut IdentityRequirements) {
    for accepted in &mut requirements.accepts {
        trim_in_place(&mut accepted.id);
        for identity_spec in &mut accepted.identity_specs {
            trim_in_place(identity_spec);
        }
    }
}

fn trim_in_place(value: &mut String) {
    if value.trim().len() != value.len() {
        *value = value.trim().to_string();
    }
}

fn normalize_and_validate_source_identity_requirements(
    source_name: &str,
    requirements: Option<&mut IdentityRequirements>,
    surface: &V4Surface,
    inputs: &[ManifestInputSpec],
) -> Result<()> {
    let Some(requirements) = requirements else {
        return Ok(());
    };
    if surface.surface_type != SurfaceType::OpenApi {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' identity_requirements are only supported for OpenAPI sources"
        )));
    }
    validate_identity_source_inputs(source_name, inputs)?;
    normalize_identity_requirements(requirements);
    validate_identity_requirements(source_name, requirements)
}

fn validate_identity_requirements(
    source_name: &str,
    requirements: &IdentityRequirements,
) -> Result<()> {
    if requirements.accepts.is_empty() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' identity_requirements.accepts must contain at least one accepted identity"
        )));
    }

    let mut seen_accept_ids = HashSet::new();
    for accepted in &requirements.accepts {
        if accepted.id.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' identity requirement id must be non-empty"
            )));
        }
        if !seen_accept_ids.insert(accepted.id.clone()) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' has duplicate identity requirement id '{}'",
                accepted.id
            )));
        }
        validate_accepted_identity_specs(source_name, accepted)?;
    }

    Ok(())
}

fn validate_accepted_identity_specs(
    source_name: &str,
    accepted: &AcceptedIdentityRequirement,
) -> Result<()> {
    if accepted.identity_specs.is_empty() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' identity requirement '{}' identity_specs must contain at least one identity spec id",
            accepted.id
        )));
    }

    let mut seen_identity_specs = HashSet::new();
    for identity_spec_id in &accepted.identity_specs {
        validate_identifier(
            identity_spec_id,
            &format!(
                "source '{source_name}' identity requirement '{}' identity spec id",
                accepted.id
            ),
        )?;
        if !seen_identity_specs.insert(identity_spec_id) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' identity requirement '{}' has duplicate identity spec id '{}'",
                accepted.id, identity_spec_id
            )));
        }
    }

    Ok(())
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
