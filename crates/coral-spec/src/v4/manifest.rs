use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::backends::http::{AuthSpec, RateLimitSpec};
use crate::inputs::{collect_declared_inputs, validate_input_references};
use crate::{
    HeaderSpec, ManifestError, ManifestInputSpec, ParsedTemplate, Result, SourceManifestCommon,
    validate_test_queries,
};

#[derive(Debug, Clone)]
pub struct V4SourceManifest {
    pub common: SourceManifestCommon,
    pub surfaces: Vec<V4Surface>,
    pub declared_inputs: Vec<ManifestInputSpec>,
    pub projection_policy: ProjectionPolicy,
}

#[derive(Debug, Clone)]
pub struct V4Surface {
    pub id: String,
    pub surface_type: SurfaceType,
    pub descriptor: SurfaceDescriptor,
    pub inputs: Vec<ManifestInputSpec>,
    pub openapi_runtime: OpenApiRuntimeConfig,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SurfaceType {
    OpenApi,
}

#[derive(Debug, Clone)]
pub enum SurfaceDescriptor {
    Url { url: String, sha256: String },
    File { file: PathBuf, sha256: String },
}

impl SurfaceDescriptor {
    pub fn sha256(&self) -> &str {
        match self {
            Self::Url { sha256, .. } | Self::File { sha256, .. } => sha256,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Url { .. } => "url",
            Self::File { .. } => "file",
        }
    }

    pub fn location(&self) -> String {
        match self {
            Self::Url { url, .. } => url.clone(),
            Self::File { file, .. } => file.display().to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OpenApiRuntimeConfig {
    pub base_url: ParsedTemplate,
    pub auth: AuthSpec,
    pub request_headers: Vec<HeaderSpec>,
    pub rate_limit: RateLimitSpec,
}

#[derive(Debug, Clone, Copy)]
pub struct ProjectionPolicy {
    pub default: ProjectionPolicyDefault,
}

impl Default for ProjectionPolicy {
    fn default() -> Self {
        Self {
            default: ProjectionPolicyDefault::DeriveReadOperations,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionPolicyDefault {
    DeriveReadOperations,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawV4SourceManifest {
    dsl_version: u32,
    name: String,
    version: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    test_queries: Vec<String>,
    surfaces: Vec<RawV4Surface>,
    #[serde(default)]
    projection_policy: RawProjectionPolicy,
    #[serde(default)]
    onboarding: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawV4Surface {
    id: String,
    #[serde(rename = "type")]
    _surface_type: RawSurfaceType,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    file: Option<PathBuf>,
    sha256: String,
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
}

#[derive(Debug, Deserialize)]
enum RawSurfaceType {
    #[serde(rename = "openapi")]
    OpenApi,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProjectionPolicy {
    #[serde(default)]
    default: Option<String>,
}

impl V4SourceManifest {
    pub(crate) fn parse_manifest_value(value: Value) -> Result<Self> {
        let raw_value = value.clone();
        let raw: RawV4SourceManifest =
            serde_json::from_value(value).map_err(ManifestError::deserialize)?;
        let RawV4SourceManifest {
            dsl_version,
            name,
            version,
            description,
            test_queries,
            surfaces,
            projection_policy,
            onboarding: _onboarding,
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
        let policy = parse_projection_policy(&name, &projection_policy)?;
        let common = SourceManifestCommon::new(
            dsl_version,
            name.clone(),
            version,
            description,
            test_queries,
        );
        let surface_values = raw_value
            .get("surfaces")
            .and_then(Value::as_array)
            .ok_or_else(|| ManifestError::validation("v4 manifest surfaces must be a list"))?;
        let mut seen_surface_ids = HashSet::new();
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
            let inputs = collect_declared_inputs(surface_value)?;
            validate_input_references(surface_value, &inputs)?;
            merge_surface_inputs(
                &name,
                &raw_surface.id,
                &inputs,
                &mut input_by_key,
                &mut declared_inputs,
            )?;
            let descriptor = parse_descriptor(&name, &raw_surface)?;
            validated_surfaces.push(V4Surface {
                id: raw_surface.id,
                surface_type: SurfaceType::OpenApi,
                descriptor,
                inputs,
                openapi_runtime: OpenApiRuntimeConfig {
                    base_url: raw_surface
                        .base_url
                        .unwrap_or_else(|| ParsedTemplate::parse("").expect("empty template")),
                    auth: raw_surface.auth,
                    request_headers: raw_surface.request_headers,
                    rate_limit: raw_surface.rate_limit,
                },
            });
        }

        Ok(Self {
            common,
            surfaces: validated_surfaces,
            declared_inputs,
            projection_policy: policy,
        })
    }

    pub fn surface(&self, surface_id: &str) -> Option<&V4Surface> {
        self.surfaces
            .iter()
            .find(|surface| surface.id == surface_id)
    }
}

fn parse_projection_policy(
    source_name: &str,
    raw: &RawProjectionPolicy,
) -> Result<ProjectionPolicy> {
    match raw.default.as_deref().unwrap_or("derive_read_operations") {
        "derive_read_operations" => Ok(ProjectionPolicy::default()),
        other => Err(ManifestError::validation(format!(
            "source '{source_name}' projection_policy.default '{other}' is unsupported"
        ))),
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

fn parse_descriptor(source_name: &str, surface: &RawV4Surface) -> Result<SurfaceDescriptor> {
    match (&surface.url, &surface.file) {
        (Some(url), None) => {
            if !url.starts_with("https://") {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' surface '{}' url descriptors must use https",
                    surface.id
                )));
            }
            validate_sha256(source_name, &surface.id, &surface.sha256)?;
            Ok(SurfaceDescriptor::Url {
                url: url.clone(),
                sha256: surface.sha256.clone(),
            })
        }
        (None, Some(file)) => {
            validate_sha256(source_name, &surface.id, &surface.sha256)?;
            Ok(SurfaceDescriptor::File {
                file: file.clone(),
                sha256: surface.sha256.clone(),
            })
        }
        (Some(_), Some(_)) | (None, None) => Err(ManifestError::validation(format!(
            "source '{source_name}' surface '{}' must declare exactly one of url or file",
            surface.id
        ))),
    }
}

fn validate_sha256(source_name: &str, surface_id: &str, sha256: &str) -> Result<()> {
    let valid = sha256.len() == 64
        && sha256
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase());
    if valid {
        Ok(())
    } else {
        Err(ManifestError::validation(format!(
            "source '{source_name}' surface '{surface_id}' sha256 must be lowercase hex SHA-256"
        )))
    }
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
