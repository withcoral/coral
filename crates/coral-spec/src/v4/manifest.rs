use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::backends::http::{AuthSpec, RateLimitSpec};
use crate::inputs::{collect_declared_inputs, validate_input_references};
use crate::{
    HeaderSpec, ManifestError, ManifestInputKind, ManifestInputSpec, ParsedTemplate, Result,
    TemplateNamespace, validate_identifier, validate_test_queries,
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
    pub version: Option<String>,
    pub description: String,
    pub test_queries: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct V4Surface {
    pub id: String,
    pub surface_type: SurfaceType,
    pub descriptor: SurfaceDescriptor,
    pub inputs: Vec<ManifestInputSpec>,
    pub identity_requirements: Option<IdentityRequirements>,
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

    pub fn sha256(&self) -> &str {
        match self {
            Self::Url { sha256, .. } | Self::File { sha256, .. } => sha256,
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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct IdentityRequirements {
    pub accepts: Vec<AcceptedIdentityRequirement>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AcceptedIdentityRequirement {
    pub id: String,
    pub identity_specs: Vec<String>,
    /// Required audience claims for this accepted shape.
    ///
    /// At match time the declared audience must be a *subset* of the candidate
    /// identity's audience: every key/value here must be present in the
    /// candidate, but the candidate may carry extra entries. An empty map
    /// imposes no audience constraint. Values are matched by exact JSON
    /// equality including type (`443` does not match `443.0` or `"443"`).
    #[serde(default)]
    pub audience: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawV4SourceManifest {
    dsl_version: u32,
    name: String,
    #[serde(default)]
    version: Option<String>,
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
    identity_requirements: Option<IdentityRequirements>,
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
            version,
            description,
            test_queries,
        };
        let surface_values = raw_value
            .get("surfaces")
            .and_then(Value::as_array)
            .ok_or_else(|| ManifestError::validation("v4 manifest surfaces must be a list"))?;
        let mut seen_surface_ids = HashSet::new();
        let mut validated_surfaces = Vec::with_capacity(surfaces.len());
        let mut declared_inputs = Vec::new();
        let mut input_by_key: BTreeMap<String, (String, ManifestInputSpec)> = BTreeMap::new();

        for (index, mut raw_surface) in surfaces.into_iter().enumerate() {
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
            validate_v4_surface_inputs(&name, &raw_surface.id, &inputs)?;
            validate_input_references(surface_value, &inputs)?;
            if let Some(base_url) = raw_surface.base_url.as_ref() {
                validate_openapi_base_url_template(
                    &name,
                    &raw_surface.id,
                    &inputs,
                    base_url,
                    "authored",
                )?;
            }
            merge_surface_inputs(
                &name,
                &raw_surface.id,
                &inputs,
                &mut input_by_key,
                &mut declared_inputs,
            )?;
            let descriptor = parse_descriptor(&name, &raw_surface)?;
            if let Some(identity_requirements) = raw_surface.identity_requirements.as_mut() {
                normalize_identity_requirements(identity_requirements);
                validate_identity_requirements(&name, &raw_surface.id, identity_requirements)?;
            }
            validated_surfaces.push(V4Surface {
                id: raw_surface.id,
                surface_type: SurfaceType::OpenApi,
                descriptor,
                inputs,
                identity_requirements: raw_surface.identity_requirements,
                openapi_runtime: OpenApiRuntimeConfig {
                    base_url: raw_surface
                        .base_url
                        .unwrap_or_else(|| ParsedTemplate::parse("").expect("empty template")),
                    auth: AuthSpec::default(),
                    request_headers: raw_surface.request_headers,
                    rate_limit: raw_surface.rate_limit,
                },
            });
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

fn validate_v4_surface_inputs(
    source_name: &str,
    surface_id: &str,
    inputs: &[ManifestInputSpec],
) -> Result<()> {
    for input in inputs {
        if input.kind == ManifestInputKind::Secret {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' surface '{surface_id}' input '{}' must not use kind: secret in DSL v4; use identity_requirements and identity specs for credentials",
                input.key
            )));
        }
    }
    Ok(())
}

/// Trims surrounding whitespace from every identifier-like field so that
/// duplicate detection, diagnostics, and runtime matching all operate on the
/// canonical value. Audience keys/values are left untouched.
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

fn validate_identity_requirements(
    source_name: &str,
    surface_id: &str,
    requirements: &IdentityRequirements,
) -> Result<()> {
    if requirements.accepts.is_empty() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' surface '{surface_id}' identity_requirements.accepts must contain at least one accepted identity"
        )));
    }

    let mut seen_accept_ids = HashSet::new();
    for accepted in &requirements.accepts {
        if accepted.id.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' surface '{surface_id}' identity requirement id must be non-empty"
            )));
        }
        if !seen_accept_ids.insert(accepted.id.clone()) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' surface '{surface_id}' has duplicate identity requirement id '{}'",
                accepted.id
            )));
        }
        validate_accepted_identity_specs(source_name, surface_id, accepted)?;
    }

    Ok(())
}

fn validate_accepted_identity_specs(
    source_name: &str,
    surface_id: &str,
    accepted: &AcceptedIdentityRequirement,
) -> Result<()> {
    if accepted.identity_specs.is_empty() {
        return Err(ManifestError::validation(format!(
            "source '{source_name}' surface '{surface_id}' identity requirement '{}' identity_specs must contain at least one identity spec id",
            accepted.id
        )));
    }

    let mut seen_identity_specs = HashSet::new();
    for identity_spec_id in &accepted.identity_specs {
        validate_identifier(
            identity_spec_id,
            &format!(
                "source '{source_name}' surface '{surface_id}' identity requirement '{}' identity spec id",
                accepted.id
            ),
        )?;
        if !seen_identity_specs.insert(identity_spec_id) {
            return Err(ManifestError::validation(format!(
                "source '{source_name}' surface '{surface_id}' identity requirement '{}' has duplicate identity spec id '{}'",
                accepted.id, identity_spec_id
            )));
        }
    }

    Ok(())
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
    validate_descriptor_sha256(source_name, &surface.id, &surface.sha256)?;
    match (&surface.url, &surface.file) {
        (Some(url), None) => {
            if !url.starts_with("https://") {
                return Err(ManifestError::validation(format!(
                    "source '{source_name}' surface '{}' url descriptors must use https",
                    surface.id
                )));
            }
            Ok(SurfaceDescriptor::Url {
                url: url.clone(),
                sha256: surface.sha256.clone(),
            })
        }
        (None, Some(file)) => Ok(SurfaceDescriptor::File {
            file: file.clone(),
            sha256: surface.sha256.clone(),
        }),
        (Some(_), Some(_)) | (None, None) => Err(ManifestError::validation(format!(
            "source '{source_name}' surface '{}' must declare exactly one of url or file",
            surface.id
        ))),
    }
}

fn validate_descriptor_sha256(source_name: &str, surface_id: &str, sha256: &str) -> Result<()> {
    let valid = sha256.len() == 64
        && sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if valid {
        Ok(())
    } else {
        Err(ManifestError::validation(format!(
            "source '{source_name}' surface '{surface_id}' sha256 must be 64 lowercase hex characters"
        )))
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
