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
use crate::validate::validate_host_template_inputs;
use crate::{
    HeaderSpec, ManifestError, ManifestInputSpec, ParsedTemplate, Result, TemplateNamespace,
    validate_reserved_source_schema_name, validate_test_queries,
};

#[derive(Debug, Clone)]
pub struct V4SourceManifest {
    pub common: V4SourceCommon,
    pub surfaces: Vec<V4Surface>,
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
    pub id: String,
    /// Effective relation namespace after applying any authored suffix.
    pub relation_namespace: String,
    pub surface_type: SurfaceType,
    pub descriptor: SurfaceDescriptor,
    pub inputs: Vec<ManifestInputSpec>,
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
    #[serde(default)]
    description: String,
    #[serde(default)]
    test_queries: Vec<String>,
    surfaces: Vec<RawV4Surface>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawV4Surface {
    id: String,
    #[serde(default)]
    namespace_suffix: Option<String>,
    #[serde(rename = "type")]
    surface_type: RawSurfaceType,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    file: Option<PathBuf>,
    #[serde(default, rename = "inputs")]
    _inputs: Option<Value>,
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
            surfaces,
        } = raw;
        if dsl_version != 4 {
            return Err(ManifestError::validation(format!(
                "source '{name}' declares dsl_version {dsl_version}; expected 4"
            )));
        }
        if surfaces.is_empty() {
            return Err(ManifestError::validation(format!(
                "source '{name}' must declare at least one surface"
            )));
        }
        validate_test_queries(&name, &test_queries)?;
        let common = V4SourceCommon {
            dsl_version,
            name: name.clone(),
            description,
            test_queries,
        };
        let surface_values = raw_value
            .get("surfaces")
            .and_then(Value::as_array)
            .ok_or_else(|| ManifestError::validation("v4 manifest surfaces must be a list"))?;
        let surface_count = surfaces.len();
        let mut seen_surface_ids = HashSet::new();
        let mut default_relation_namespace_surface = None;
        let mut relation_namespace_by_name = BTreeMap::new();
        let mut validated_surfaces = Vec::with_capacity(surfaces.len());
        let mut declared_inputs = Vec::new();
        let mut input_by_key: BTreeMap<String, (String, ManifestInputSpec)> = BTreeMap::new();

        for (index, raw_surface) in surfaces.into_iter().enumerate() {
            let surface_value = surface_values.get(index).ok_or_else(|| {
                ManifestError::validation(format!("source '{name}' surface[{index}] is missing"))
            })?;
            validate_surface_id(&name, &raw_surface.id)?;
            if !seen_surface_ids.insert(raw_surface.id.clone()) {
                return Err(ManifestError::validation(format!(
                    "source '{name}' has duplicate surface id '{}'",
                    raw_surface.id
                )));
            }
            if raw_surface.namespace_suffix.is_none()
                && surface_count > 1
                && let Some(existing_surface) =
                    default_relation_namespace_surface.replace(raw_surface.id.clone())
            {
                return Err(ManifestError::validation(format!(
                    "source '{name}' surfaces '{existing_surface}' and '{}' both omit namespace_suffix; at most one surface may use the default relation namespace '{name}'",
                    raw_surface.id
                )));
            }
            let relation_namespace = surface_relation_namespace(
                &name,
                &raw_surface.id,
                raw_surface.namespace_suffix.as_deref(),
            )?;
            validate_relation_namespace(&name, &raw_surface.id, &relation_namespace)?;
            if let Some(existing_surface) = relation_namespace_by_name
                .insert(relation_namespace.clone(), raw_surface.id.clone())
            {
                return Err(ManifestError::validation(format!(
                    "source '{name}' surfaces '{existing_surface}' and '{}' declare duplicate relation namespace '{relation_namespace}'",
                    raw_surface.id
                )));
            }
            let inputs = collect_declared_inputs(surface_value)?;
            validate_input_references(surface_value, &inputs)?;
            validate_oauth_endpoint_templates_with_scope(&inputs, "surface inputs")?;
            merge_surface_inputs(
                &name,
                &raw_surface.id,
                &inputs,
                &mut input_by_key,
                &mut declared_inputs,
            )?;
            validated_surfaces.push(parse_surface(
                &name,
                raw_surface,
                surface_value,
                inputs,
                relation_namespace,
            )?);
        }

        Ok(Self {
            common,
            surfaces: validated_surfaces,
            declared_inputs,
        })
    }

    pub fn surface(&self, surface_id: &str) -> Option<&V4Surface> {
        self.surfaces
            .iter()
            .find(|surface| surface.id == surface_id)
    }
}

fn parse_surface(
    source_name: &str,
    raw_surface: RawV4Surface,
    surface_value: &Value,
    inputs: Vec<ManifestInputSpec>,
    relation_namespace: String,
) -> Result<V4Surface> {
    match raw_surface.surface_type {
        RawSurfaceType::OpenApi => {
            parse_openapi_surface(source_name, raw_surface, inputs, relation_namespace)
        }
        RawSurfaceType::Mcp => parse_mcp_surface(
            source_name,
            raw_surface,
            surface_value,
            inputs,
            relation_namespace,
        ),
    }
}

fn parse_openapi_surface(
    source_name: &str,
    raw_surface: RawV4Surface,
    inputs: Vec<ManifestInputSpec>,
    relation_namespace: String,
) -> Result<V4Surface> {
    if raw_surface.server.is_some() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' OpenAPI surface '{}' must not declare server",
            raw_surface.id
        )));
    }
    if let Some(base_url) = raw_surface.base_url.as_ref() {
        validate_openapi_base_url_template(
            source_name,
            &raw_surface.id,
            &inputs,
            base_url,
            "authored",
        )?;
    }
    let descriptor = parse_openapi_descriptor(source_name, &raw_surface)?;
    Ok(V4Surface {
        id: raw_surface.id,
        relation_namespace,
        surface_type: SurfaceType::OpenApi,
        descriptor,
        inputs,
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
    inputs: Vec<ManifestInputSpec>,
    relation_namespace: String,
) -> Result<V4Surface> {
    if raw_surface.url.is_some() || raw_surface.file.is_some() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' MCP surface '{}' must not declare url or file",
            raw_surface.id
        )));
    }
    for field in ["base_url", "auth", "request_headers", "rate_limit"] {
        if surface_value.get(field).is_some() {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' MCP surface '{}' must not declare OpenAPI field '{field}'",
                raw_surface.id
            )));
        }
    }
    let server = raw_surface.server.ok_or_else(|| {
        ManifestError::validation(format!(
            "source '{source_name}' MCP surface '{}' must declare server",
            raw_surface.id
        ))
    })?;
    validate_mcp_server(source_name, &server, &inputs)?;
    Ok(V4Surface {
        id: raw_surface.id,
        relation_namespace,
        surface_type: SurfaceType::Mcp,
        descriptor: SurfaceDescriptor::McpServer {
            location: mcp_server_location(&server),
        },
        inputs,
        runtime: SurfaceRuntimeConfig::Mcp(McpRuntimeConfig { server }),
    })
}

fn mcp_server_location(server: &McpServerSpec) -> String {
    match server {
        McpServerSpec::Stdio { command, .. } => command.clone(),
        McpServerSpec::StreamableHttp { url, .. } => url.clone(),
    }
}

fn validate_surface_id(source_name: &str, id: &str) -> Result<()> {
    let mut chars = id.chars();
    let valid = matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if valid {
        Ok(())
    } else {
        Err(ManifestError::validation(format!(
            "source '{source_name}' surface id '{id}' must match [a-z][a-z0-9_]*"
        )))
    }
}

fn surface_relation_namespace(
    source_name: &str,
    surface_id: &str,
    namespace_suffix: Option<&str>,
) -> Result<String> {
    if let Some(namespace_suffix) = namespace_suffix {
        validate_surface_namespace_suffix(source_name, surface_id, namespace_suffix)?;
        return Ok(format!("{source_name}_{namespace_suffix}"));
    }
    Ok(source_name.to_string())
}

fn validate_surface_namespace_suffix(
    source_name: &str,
    surface_id: &str,
    namespace_suffix: &str,
) -> Result<()> {
    let mut chars = namespace_suffix.chars();
    let valid = matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if valid {
        Ok(())
    } else {
        Err(ManifestError::validation(format!(
            "source '{source_name}' surface '{surface_id}' namespace_suffix '{namespace_suffix}' must match [a-z][a-z0-9_]*"
        )))
    }
}

fn validate_relation_namespace(
    source_name: &str,
    surface_id: &str,
    relation_namespace: &str,
) -> Result<()> {
    let mut chars = relation_namespace.chars();
    let valid = matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if valid {
        validate_reserved_source_schema_name(
            relation_namespace,
            "source surface relation namespace",
        )
    } else {
        Err(ManifestError::validation(format!(
            "source '{source_name}' surface '{surface_id}' relation namespace '{relation_namespace}' must match [a-z][a-z0-9_]*"
        )))
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
                    "source '{source_name}' surface '{}' url descriptors must use https",
                    surface.id
                )));
            }
            Ok(SurfaceDescriptor::Url { url: url.clone() })
        }
        (None, Some(file)) => Ok(SurfaceDescriptor::File { file: file.clone() }),
        (Some(_), Some(_)) | (None, None) => Err(ManifestError::validation(format!(
            "source '{source_name}' surface '{}' must declare exactly one of url or file",
            surface.id
        ))),
    }
}

pub fn validate_openapi_base_url_template(
    source_name: &str,
    surface_id: &str,
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
                        "source '{source_name}' surface '{surface_id}' {provenance}base_url input token '{{{{{}}}}}' must declare defaults under source inputs",
                        token.raw()
                    )));
                }
                if !inputs.iter().any(|input| input.key == token.key()) {
                    return Err(ManifestError::validation(format!(
                        "source '{source_name}' surface '{surface_id}' {provenance}base_url references undeclared input '{}'",
                        token.key()
                    )));
                }
            }
            _ => {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' surface '{surface_id}' {provenance}base_url may only reference source inputs; unsupported template token '{{{{{}}}}}'",
                    token.raw()
                )));
            }
        }
    }
    validate_host_template_inputs(
        &format!("source '{source_name}' surface '{surface_id}' {provenance}base_url"),
        base_url,
        inputs,
    )?;
    Ok(())
}

fn merge_surface_inputs(
    source_name: &str,
    surface_id: &str,
    inputs: &[ManifestInputSpec],
    input_by_key: &mut BTreeMap<String, (String, ManifestInputSpec)>,
    declared_inputs: &mut Vec<ManifestInputSpec>,
) -> Result<()> {
    for input in inputs {
        if let Some((existing_surface, existing)) = input_by_key.get(&input.key) {
            if existing != input {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' surfaces '{existing_surface}' and '{surface_id}' declare incompatible input '{}'",
                    input.key
                )));
            }
            continue;
        }
        input_by_key.insert(input.key.clone(), (surface_id.to_string(), input.clone()));
        declared_inputs.push(input.clone());
    }
    Ok(())
}
