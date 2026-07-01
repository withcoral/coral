//! Bundled source catalog and installed-manifest resolution helpers.

use std::collections::{BTreeMap, BTreeSet};

use coral_spec::backends::file::{FileObjectStoreSpec, S3AuthSpec};
use coral_spec::{
    AuthSpec, BodySpec, HeaderSpec, ManifestInputKind, McpServerSpec, ParsedTemplate,
    TemplateNamespace, ValidatedSourceManifest, ValueSourceSpec, parse_source_manifest_yaml,
};
use serde_json::Value as JsonValue;

use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::sources::model::{CandidateSource, InstalledSource, SourceOrigin};
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

include!(concat!(env!("OUT_DIR"), "/bundled_sources.rs"));

#[derive(Debug, Clone)]
pub(crate) struct BundledSourceManifest {
    pub(crate) manifest_yaml: String,
}

#[derive(Debug, Clone)]
pub(crate) struct InstalledSourceManifest {
    pub(crate) source_spec: ValidatedSourceManifest,
    pub(crate) candidate: CandidateSource,
    pub(crate) manifest_yaml: String,
}

pub(crate) fn list_bundled_sources(
    installed_source_names: &BTreeSet<SourceName>,
) -> Result<Vec<CandidateSource>, AppError> {
    let mut candidates = BUNDLED_SOURCES
        .iter()
        .map(|(name, manifest_yaml)| {
            let bundled_name = SourceName::parse(name)?;
            let mut candidate = describe_manifest(
                manifest_yaml,
                SourceOrigin::Bundled,
                installed_source_names.contains(&bundled_name),
            )?;
            candidate.name = bundled_name;
            Ok(candidate)
        })
        .collect::<Result<Vec<_>, AppError>>()?;
    candidates.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(candidates)
}

pub(crate) fn load_bundled_source(name: &SourceName) -> Result<BundledSourceManifest, AppError> {
    let Some((_, manifest_yaml)) = BUNDLED_SOURCES
        .iter()
        .find(|(candidate, _)| *candidate == name.as_str())
    else {
        return Err(AppError::InvalidInput(format!(
            "unknown bundled source '{name}'"
        )));
    };
    Ok(BundledSourceManifest {
        manifest_yaml: (*manifest_yaml).to_string(),
    })
}

/// Resolve the effective installed manifest and verify it still matches the
/// installed source identity in app state.
pub(crate) fn resolve_installed_manifest(
    workspace_name: &WorkspaceName,
    source: &InstalledSource,
    layout: &AppStateLayout,
) -> Result<InstalledSourceManifest, AppError> {
    let manifest_yaml = match source.origin {
        SourceOrigin::Bundled => load_bundled_source(&source.name)?.manifest_yaml,
        SourceOrigin::Imported => {
            std::fs::read_to_string(layout.manifest_file(workspace_name, &source.name))?
        }
    };
    resolve_installed_manifest_from_yaml(source, &manifest_yaml)
}

pub(crate) fn resolve_installed_manifest_from_yaml(
    source: &InstalledSource,
    manifest_yaml: &str,
) -> Result<InstalledSourceManifest, AppError> {
    let source_spec = parse_installed_source_manifest(&source.name, manifest_yaml)?;
    let mut candidate = candidate_from_manifest(&source_spec, source.origin, false)?;
    if candidate.name != source.name {
        return Err(AppError::FailedPrecondition(format!(
            "installed source '{}' does not match manifest name '{}'",
            source.name, candidate.name
        )));
    }
    candidate.installed = true;
    candidate.credential_storage = Some(source.effective_credential_storage());
    Ok(InstalledSourceManifest {
        source_spec,
        candidate,
        manifest_yaml: manifest_yaml.to_string(),
    })
}

fn parse_installed_source_manifest(
    source_name: &SourceName,
    manifest_yaml: &str,
) -> Result<ValidatedSourceManifest, AppError> {
    parse_source_manifest_yaml(manifest_yaml).map_err(|error| {
        if is_legacy_plural_v4_manifest(manifest_yaml) {
            AppError::IncompatibleInstalledV4Manifest {
                source_name: source_name.to_string(),
                detail: "the manifest uses the removed plural 'surfaces' field".to_string(),
            }
        } else {
            AppError::InvalidInput(error.to_string())
        }
    })
}

fn is_legacy_plural_v4_manifest(manifest_yaml: &str) -> bool {
    let Ok(value) = serde_yaml::from_str::<serde_yaml::Value>(manifest_yaml) else {
        return false;
    };
    value.get("dsl_version").and_then(serde_yaml::Value::as_u64) == Some(4)
        && value.get("surfaces").is_some()
}

pub(crate) fn describe_manifest(
    manifest_yaml: &str,
    origin: SourceOrigin,
    installed: bool,
) -> Result<CandidateSource, AppError> {
    let manifest = parse_source_manifest_yaml(manifest_yaml)
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    candidate_from_manifest(&manifest, origin, installed)
}

pub(crate) fn validate_imported_manifest_database_persistence(
    manifest_yaml: &str,
) -> Result<(), AppError> {
    let manifest = parse_source_manifest_yaml(manifest_yaml)
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    let input_kinds = manifest
        .declared_inputs()
        .iter()
        .map(|input| (input.key.clone(), input.kind))
        .collect::<BTreeMap<_, _>>();

    if let Some(http) = manifest.as_http() {
        validate_http_auth_spec_for_database_persistence(&input_kinds, "auth", &http.auth)?;
        validate_headers_for_database_persistence(
            &input_kinds,
            "request_headers",
            &http.request_headers,
        )?;
        for table in &http.tables {
            validate_request_headers_for_database_persistence(
                &input_kinds,
                &format!("table '{}'", table.name()),
                &table.request,
            )?;
            for route in &table.requests {
                validate_request_headers_for_database_persistence(
                    &input_kinds,
                    &format!("table '{}' request route", table.name()),
                    &route.request,
                )?;
            }
        }
        for function in &http.functions {
            validate_request_headers_for_database_persistence(
                &input_kinds,
                &format!("function '{}'", function.name),
                &function.request,
            )?;
        }
    }

    if let Some(file) = manifest.as_file() {
        for table in &file.tables {
            validate_file_source_for_database_persistence(
                &input_kinds,
                &format!("table '{}'", table.name()),
                &table.source,
            )?;
        }
    }

    if let Some(mcp) = manifest.as_mcp() {
        validate_mcp_server_for_database_persistence(&input_kinds, "server", &mcp.server)?;
    }

    if let Some(v4) = manifest.as_v4() {
        match &v4.surface.runtime {
            coral_spec::v4::SurfaceRuntimeConfig::OpenApi(runtime) => {
                validate_http_auth_spec_for_database_persistence(
                    &input_kinds,
                    "surface auth",
                    &runtime.auth,
                )?;
                validate_headers_for_database_persistence(
                    &input_kinds,
                    "surface request_headers",
                    &runtime.request_headers,
                )?;
            }
            coral_spec::v4::SurfaceRuntimeConfig::Mcp(runtime) => {
                validate_mcp_server_for_database_persistence(
                    &input_kinds,
                    "surface server",
                    &runtime.server,
                )?;
            }
            coral_spec::v4::SurfaceRuntimeConfig::Database(_) => {
                // Database surfaces carry their credentials in the connection
                // spec, which the database connection layer validates on its
                // own. There is no surface auth/header/base_url transport to
                // guard for imported-manifest persistence here.
            }
        }
    }

    Ok(())
}

fn candidate_from_manifest(
    manifest: &ValidatedSourceManifest,
    origin: SourceOrigin,
    installed: bool,
) -> Result<CandidateSource, AppError> {
    Ok(CandidateSource {
        name: SourceName::parse(manifest.schema_name())?,
        description: manifest.description().to_string(),
        version: manifest.source_version().map(ToString::to_string),
        inputs: manifest.declared_inputs().to_vec(),
        installed,
        origin,
        credential_storage: None,
    })
}

fn validate_http_auth_spec_for_database_persistence(
    input_kinds: &BTreeMap<String, ManifestInputKind>,
    context: &str,
    auth: &AuthSpec,
) -> Result<(), AppError> {
    match auth {
        AuthSpec::BasicAuth(basic) => {
            if basic_auth_username_needs_secret(&basic.username) {
                validate_secret_template_for_database_persistence(
                    input_kinds,
                    &format!("{context}.username"),
                    &basic.username,
                )?;
            }
            validate_secret_template_for_database_persistence(
                input_kinds,
                &format!("{context}.password"),
                &basic.password,
            )?;
        }
        AuthSpec::HeaderAuth(header_auth) => {
            validate_headers_for_database_persistence(input_kinds, context, &header_auth.headers)?;
        }
        AuthSpec::CustomAuth(custom) => {
            for (key, value) in &custom.config {
                validate_custom_auth_value_for_database_persistence(
                    input_kinds,
                    &format!("{context}.{key}"),
                    sensitive_key_name(key),
                    value,
                )?;
            }
        }
    }
    Ok(())
}

fn validate_file_source_for_database_persistence(
    input_kinds: &BTreeMap<String, ManifestInputKind>,
    context: &str,
    source: &coral_spec::backends::file::FileSourceSpec,
) -> Result<(), AppError> {
    let Some(FileObjectStoreSpec::S3 {
        auth:
            S3AuthSpec::AccessKey {
                access_key_id,
                secret_access_key,
                session_token,
            },
        ..
    }) = source.object_store.as_ref()
    else {
        return Ok(());
    };

    validate_secret_template_for_database_persistence(
        input_kinds,
        &format!("{context} source.object_store.auth.access_key_id"),
        access_key_id,
    )?;
    validate_secret_template_for_database_persistence(
        input_kinds,
        &format!("{context} source.object_store.auth.secret_access_key"),
        secret_access_key,
    )?;
    if let Some(session_token) = session_token {
        validate_secret_template_for_database_persistence(
            input_kinds,
            &format!("{context} source.object_store.auth.session_token"),
            session_token,
        )?;
    }
    Ok(())
}

fn validate_mcp_server_for_database_persistence(
    input_kinds: &BTreeMap<String, ManifestInputKind>,
    context: &str,
    server: &McpServerSpec,
) -> Result<(), AppError> {
    let McpServerSpec::Stdio { env, .. } = server else {
        return Ok(());
    };
    for env in env {
        if sensitive_key_name(&env.name) {
            validate_sensitive_value_source_for_database_persistence(
                input_kinds,
                &format!("{context} env '{}'", env.name),
                &env.value,
            )?;
        }
    }
    Ok(())
}

fn validate_request_headers_for_database_persistence(
    input_kinds: &BTreeMap<String, ManifestInputKind>,
    context: &str,
    request: &coral_spec::RequestSpec,
) -> Result<(), AppError> {
    validate_headers_for_database_persistence(
        input_kinds,
        &format!("{context} request headers"),
        &request.headers,
    )?;
    for param in &request.query {
        if sensitive_query_param_name(&param.name) {
            validate_sensitive_value_source_for_database_persistence(
                input_kinds,
                &format!("{context} query param '{}'", param.name),
                &param.value,
            )?;
        }
    }
    match &request.body {
        BodySpec::Json { fields } => {
            for field in fields {
                if field.path.iter().any(|segment| sensitive_key_name(segment)) {
                    validate_sensitive_value_source_for_database_persistence(
                        input_kinds,
                        &format!("{context} body field '{}'", field.path.join(".")),
                        &field.value,
                    )?;
                }
            }
        }
        BodySpec::Text { content } => {
            validate_text_body_for_database_persistence(
                input_kinds,
                &format!("{context} request body text"),
                content,
            )?;
        }
    }
    Ok(())
}

fn validate_headers_for_database_persistence(
    input_kinds: &BTreeMap<String, ManifestInputKind>,
    context: &str,
    headers: &[HeaderSpec],
) -> Result<(), AppError> {
    for header in headers {
        if sensitive_http_header_name(&header.name) {
            validate_sensitive_value_source_for_database_persistence(
                input_kinds,
                &format!("{context} header '{}'", header.name),
                &header.value,
            )?;
        }
    }
    Ok(())
}

fn basic_auth_username_needs_secret(template: &ParsedTemplate) -> bool {
    template_contains_sensitive_marker(template)
}

fn template_contains_sensitive_marker(template: &ParsedTemplate) -> bool {
    text_body_contains_sensitive_marker(template.raw())
        || template.tokens().any(|token| {
            token.namespace() == &TemplateNamespace::Input && sensitive_key_name(token.key())
        })
}

fn validate_sensitive_value_source_for_database_persistence(
    input_kinds: &BTreeMap<String, ManifestInputKind>,
    context: &str,
    value: &ValueSourceSpec,
) -> Result<(), AppError> {
    match value {
        ValueSourceSpec::Template { template } => {
            validate_secret_template_for_database_persistence(input_kinds, context, template)
        }
        ValueSourceSpec::OneOf { values } => {
            for value in values {
                validate_sensitive_value_source_for_database_persistence(
                    input_kinds,
                    context,
                    value,
                )?;
            }
            Ok(())
        }
        ValueSourceSpec::Input { key } | ValueSourceSpec::Bearer { key } => {
            require_secret_input_for_database_persistence(input_kinds, context, key)
        }
        ValueSourceSpec::Literal { .. }
        | ValueSourceSpec::Filter { .. }
        | ValueSourceSpec::FilterInt { .. }
        | ValueSourceSpec::FilterBool { .. }
        | ValueSourceSpec::FilterStringArray { .. }
        | ValueSourceSpec::FilterSplit { .. }
        | ValueSourceSpec::FilterSplitInt { .. }
        | ValueSourceSpec::Arg { .. }
        | ValueSourceSpec::ArgInt { .. }
        | ValueSourceSpec::ArgBool { .. }
        | ValueSourceSpec::ArgStringArray { .. }
        | ValueSourceSpec::ArgSplit { .. }
        | ValueSourceSpec::ArgSplitInt { .. }
        | ValueSourceSpec::State { .. }
        | ValueSourceSpec::NowEpochMinusSeconds { .. } => Err(AppError::InvalidInput(format!(
            "{context} must reference a secret input before an imported manifest can be stored in the database"
        ))),
    }
}

fn validate_text_body_for_database_persistence(
    input_kinds: &BTreeMap<String, ManifestInputKind>,
    context: &str,
    value: &ValueSourceSpec,
) -> Result<(), AppError> {
    if text_body_value_source_needs_secret(value) {
        validate_sensitive_value_source_for_database_persistence(input_kinds, context, value)?;
    }
    Ok(())
}

fn text_body_value_source_needs_secret(value: &ValueSourceSpec) -> bool {
    match value {
        ValueSourceSpec::Template { template } => template_contains_sensitive_marker(template),
        ValueSourceSpec::OneOf { values } => values.iter().any(text_body_value_source_needs_secret),
        ValueSourceSpec::Literal { value } => json_value_contains_sensitive_marker(value),
        ValueSourceSpec::Input { key } => sensitive_key_name(key),
        ValueSourceSpec::Bearer { .. } => true,
        ValueSourceSpec::Filter { default, .. } | ValueSourceSpec::Arg { default, .. } => default
            .as_ref()
            .is_some_and(json_value_contains_sensitive_marker),
        ValueSourceSpec::FilterInt { .. }
        | ValueSourceSpec::FilterBool { .. }
        | ValueSourceSpec::FilterStringArray { .. }
        | ValueSourceSpec::FilterSplit { .. }
        | ValueSourceSpec::FilterSplitInt { .. }
        | ValueSourceSpec::ArgInt { .. }
        | ValueSourceSpec::ArgBool { .. }
        | ValueSourceSpec::ArgStringArray { .. }
        | ValueSourceSpec::ArgSplit { .. }
        | ValueSourceSpec::ArgSplitInt { .. }
        | ValueSourceSpec::State { .. }
        | ValueSourceSpec::NowEpochMinusSeconds { .. } => false,
    }
}

fn json_value_contains_sensitive_marker(value: &JsonValue) -> bool {
    match value {
        JsonValue::String(value) => text_body_contains_sensitive_marker(value),
        JsonValue::Array(values) => values.iter().any(json_value_contains_sensitive_marker),
        JsonValue::Object(map) => map.iter().any(|(key, value)| {
            sensitive_key_name(key) || json_value_contains_sensitive_marker(value)
        }),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) => false,
    }
}

fn text_body_contains_sensitive_marker(raw: &str) -> bool {
    if raw
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .any(|word| word.eq_ignore_ascii_case("bearer"))
        || raw.to_ascii_uppercase().contains("BEGIN PRIVATE KEY")
    {
        return true;
    }

    raw.split(['&', '\n', ',', '{', '}']).any(|part| {
        let Some((key, _)) = part.split_once('=').or_else(|| part.split_once(':')) else {
            return false;
        };
        let key = key.trim_matches(|ch: char| {
            ch.is_ascii_whitespace() || ch == '"' || ch == '\'' || ch == '-'
        });
        sensitive_key_name(key)
    })
}

fn validate_custom_auth_value_for_database_persistence(
    input_kinds: &BTreeMap<String, ManifestInputKind>,
    context: &str,
    sensitive_context: bool,
    value: &JsonValue,
) -> Result<(), AppError> {
    match value {
        JsonValue::Object(map) => {
            for (key, value) in map {
                validate_custom_auth_value_for_database_persistence(
                    input_kinds,
                    &format!("{context}.{key}"),
                    sensitive_context || sensitive_key_name(key),
                    value,
                )?;
            }
            Ok(())
        }
        JsonValue::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                validate_custom_auth_value_for_database_persistence(
                    input_kinds,
                    &format!("{context}[{index}]"),
                    sensitive_context,
                    value,
                )?;
            }
            Ok(())
        }
        JsonValue::String(raw) if sensitive_context => {
            let template = ParsedTemplate::parse(raw)
                .map_err(|error| AppError::InvalidInput(error.to_string()))?;
            validate_secret_template_for_database_persistence(input_kinds, context, &template)
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            Ok(())
        }
    }
}

fn validate_secret_template_for_database_persistence(
    input_kinds: &BTreeMap<String, ManifestInputKind>,
    context: &str,
    template: &ParsedTemplate,
) -> Result<(), AppError> {
    let mut saw_input = false;
    for token in template.tokens() {
        if token.namespace() != &TemplateNamespace::Input {
            continue;
        }
        saw_input = true;
        require_secret_input_for_database_persistence(input_kinds, context, token.key())?;
    }
    if !saw_input {
        return Err(AppError::InvalidInput(format!(
            "{context} must reference a secret input before an imported manifest can be stored in the database"
        )));
    }
    Ok(())
}

fn require_secret_input_for_database_persistence(
    input_kinds: &BTreeMap<String, ManifestInputKind>,
    context: &str,
    key: &str,
) -> Result<(), AppError> {
    match input_kinds.get(key) {
        Some(ManifestInputKind::Secret) => Ok(()),
        Some(ManifestInputKind::Variable) => Err(AppError::InvalidInput(format!(
            "{context} must reference a secret input before an imported manifest can be stored in the database; '{key}' is a variable"
        ))),
        None => Err(AppError::InvalidInput(format!(
            "{context} references undeclared input '{key}'"
        ))),
    }
}

fn sensitive_http_header_name(name: &str) -> bool {
    let compact = compact_key_name(name);
    matches!(compact.as_str(), "COOKIE" | "PROXYAUTHORIZATION") || sensitive_key_name(name)
}

fn sensitive_query_param_name(name: &str) -> bool {
    let compact = compact_key_name(name);
    if pagination_query_param_name(&compact) {
        return false;
    }
    sensitive_key_name(name)
}

fn pagination_query_param_name(compact: &str) -> bool {
    matches!(
        compact,
        "PAGETOKEN"
            | "NEXTPAGETOKEN"
            | "CURSORTOKEN"
            | "NEXTCURSORTOKEN"
            | "CONTINUATIONTOKEN"
            | "NEXTTOKEN"
    ) || (compact.contains("PAGE") && compact.ends_with("TOKEN"))
        || (compact.contains("CURSOR") && compact.ends_with("TOKEN"))
}

fn sensitive_key_name(name: &str) -> bool {
    const SENSITIVE_SUFFIXES: &[&str] = &[
        "AUTHORIZATION",
        "APIKEY",
        "APPLICATIONKEY",
        "ACCESSKEY",
        "ACCESSKEYID",
        "ACCESSTOKEN",
        "AUTH",
        "AUTHTOKEN",
        "BEARERTOKEN",
        "CLIENTSECRET",
        "CONSUMERKEY",
        "PASSWORD",
        "PRIVATEKEY",
        "SECRET",
        "TOKEN",
    ];
    let compact = compact_key_name(name);
    SENSITIVE_SUFFIXES
        .iter()
        .any(|suffix| compact.ends_with(suffix))
}

fn compact_key_name(name: &str) -> String {
    name.chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|ch| ch.to_ascii_uppercase())
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::indexing_slicing,
        reason = "manifest input order assertions intentionally fail loudly in tests"
    )]

    use std::collections::BTreeSet;

    use coral_spec::ManifestInputKind;

    use super::{
        describe_manifest, list_bundled_sources, load_bundled_source,
        parse_installed_source_manifest, validate_imported_manifest_database_persistence,
    };
    use crate::bootstrap::AppError;
    use crate::sources::SourceName;
    use crate::sources::model::SourceOrigin;

    fn minimal_http_manifest(extra_top_level: &str) -> String {
        format!(
            "name: demo\nversion: 1.0.0\ndsl_version: 3\nbackend: http\nbase_url: https://example.com\n{extra_top_level}tables:\n  - name: messages\n    description: Demo messages\n    request:\n      method: GET\n      path: /messages\n    response: {{}}\n    columns:\n      - name: id\n        type: Utf8\n"
        )
    }

    fn minimal_http_manifest_with_request(request_extra: &str) -> String {
        format!(
            "name: demo\nversion: 1.0.0\ndsl_version: 3\nbackend: http\nbase_url: https://example.com\ntables:\n  - name: messages\n    description: Demo messages\n    filters:\n      - name: page_token\n    request:\n      method: GET\n      path: /messages\n{request_extra}    response: {{}}\n    columns:\n      - name: id\n        type: Utf8\n"
        )
    }

    fn minimal_v4_openapi_manifest(surface_extra: &str) -> String {
        format!(
            "name: demo\ndsl_version: 4\nsurface:\n  type: openapi\n  file: /tmp/openapi.yaml\n{surface_extra}"
        )
    }

    fn minimal_http_manifest_with_route(route_extra: &str) -> String {
        format!(
            "name: demo\nversion: 1.0.0\ndsl_version: 3\nbackend: http\nbase_url: https://example.com\ntables:\n  - name: messages\n    description: Demo messages\n    filters:\n      - name: id\n    request:\n      method: GET\n      path: /messages\n    requests:\n      - when_filters:\n          - id\n        method: GET\n        path: /messages/{{{{filter.id}}}}\n{route_extra}    response: {{}}\n    columns:\n      - name: id\n        type: Utf8\n"
        )
    }

    fn minimal_http_function_manifest(request_extra: &str) -> String {
        format!(
            "name: demo\nversion: 1.0.0\ndsl_version: 3\nbackend: http\nbase_url: https://example.com\nfunctions:\n  - name: search_messages\n    args:\n      - name: q\n        bind:\n          arg: q\n    request:\n      method: GET\n      path: /search\n{request_extra}    columns:\n      - name: id\n        type: Utf8\n"
        )
    }

    fn minimal_file_manifest(inputs: &str, object_store: &str) -> String {
        format!(
            "name: demo\nversion: 1.0.0\ndsl_version: 3\nbackend: file\n{inputs}tables:\n  - name: objects\n    description: Demo objects\n    format: jsonl\n    source:\n      location: s3://demo-bucket/objects/\n{object_store}    columns:\n      - name: id\n        type: Utf8\n"
        )
    }

    fn minimal_mcp_manifest(server_extra: &str) -> String {
        format!(
            "name: demo\nversion: 1.0.0\ndsl_version: 3\nbackend: mcp\nserver:\n{server_extra}functions:\n  - name: search_messages\n    tool: search\n    columns:\n      - name: result\n        type: Utf8\n"
        )
    }

    fn minimal_v4_mcp_manifest(server_extra: &str) -> String {
        format!("name: demo\ndsl_version: 4\nsurface:\n  type: mcp\n  server:\n{server_extra}")
    }

    #[test]
    fn bundled_sources_load_through_catalog() {
        let sources = list_bundled_sources(&BTreeSet::new()).expect("bundled sources");
        assert!(!sources.is_empty());
        assert!(
            sources
                .iter()
                .any(|source| source.name == SourceName::parse("github").expect("source"))
        );
        assert!(
            sources
                .iter()
                .any(|source| source.name == SourceName::parse("stripe").expect("source"))
        );
        assert!(sources.iter().all(|source| source.version.is_some()));
    }

    #[test]
    fn community_sources_are_not_bundled() {
        let hn = SourceName::parse("hn").expect("source");
        let error = load_bundled_source(&hn).expect_err("community source should not be bundled");
        assert!(error.to_string().contains("unknown bundled source 'hn'"));
    }

    #[test]
    fn core_v4_preview_sources_are_not_bundled() {
        let github_v4 = SourceName::parse("github_v4").expect("source");

        let error = load_bundled_source(&github_v4).expect_err("v4 source should not be bundled");
        assert!(error.to_string().contains("unknown bundled source"));
    }

    #[test]
    fn installed_plural_v4_manifest_reports_re_add_guidance() {
        let manifest = r"
name: legacy_v4
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.yaml
";

        let error = parse_installed_source_manifest(
            &SourceName::parse("legacy_v4").expect("source"),
            manifest,
        )
        .expect_err("plural v4 manifest should be incompatible");
        let message = error.to_string();

        assert!(
            matches!(error, AppError::IncompatibleInstalledV4Manifest { .. }),
            "unexpected error: {error:#}"
        );
        assert!(message.contains("removed plural 'surfaces' field"));
        assert!(message.contains("Re-add the source with a current manifest"));
    }

    #[test]
    fn describe_manifest_extracts_declared_inputs() {
        let source = describe_manifest(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  API_BASE:
    kind: variable
    default: https://example.com
  API_TOKEN:
    kind: secret
base_url: "{{input.API_BASE}}"
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: template
      template: Bearer {{input.API_TOKEN}}
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
"#,
            SourceOrigin::Imported,
            false,
        )
        .expect("describe manifest");
        assert_eq!(source.inputs.len(), 2);
        assert_eq!(source.inputs[0].key, "API_BASE");
        assert_eq!(source.inputs[0].kind, ManifestInputKind::Variable);
        assert_eq!(source.inputs[1].key, "API_TOKEN");
        assert_eq!(source.inputs[1].kind, ManifestInputKind::Secret);
    }

    #[test]
    fn describe_manifest_rejects_legacy_schema_field() {
        let error = describe_manifest(
            r"
name: demo
schema: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
",
            SourceOrigin::Imported,
            false,
        )
        .expect_err("legacy schema field should fail");
        let message = error.to_string();
        assert!(message.starts_with("invalid input: source manifest failed schema validation:"));
        assert!(message.contains("'schema'"));
    }

    #[test]
    fn database_persistence_rejects_literal_sensitive_values() {
        let cases = [
            (
                minimal_http_manifest(
                    "auth:\n  type: HeaderAuth\n  headers:\n    - name: Authorization\n      from: literal\n      value: Bearer hardcoded-token\n",
                ),
                "auth header 'Authorization' must reference a secret input",
            ),
            (
                minimal_http_manifest(
                    "auth:\n  type: BasicAuth\n  username: user\n  password: hardcoded-password\n",
                ),
                "auth.password must reference a secret input",
            ),
            (
                minimal_http_manifest(
                    "auth:\n  type: CustomAuth\n  authenticator: demo_auth\n  nested:\n    password: hardcoded-password\n",
                ),
                "auth.nested.password must reference a secret input",
            ),
            (
                minimal_http_manifest(
                    "request_headers:\n  - name: X-ApiKey\n    from: literal\n    value: hardcoded-key\n",
                ),
                "request_headers header 'X-ApiKey' must reference a secret input",
            ),
            (
                minimal_http_manifest_with_request(
                    "      query:\n        - name: api_key\n          from: literal\n          value: hardcoded-key\n",
                ),
                "table 'messages' query param 'api_key' must reference a secret input",
            ),
            (
                minimal_http_manifest_with_request(
                    "      body:\n        - path: [credentials, password]\n          from: literal\n          value: hardcoded-password\n",
                ),
                "table 'messages' body field 'credentials.password' must reference a secret input",
            ),
            (
                minimal_http_manifest_with_request(
                    "      body:\n        format: text\n        content:\n          from: literal\n          value: \"api_key=hardcoded-key\"\n",
                ),
                "table 'messages' request body text must reference a secret input",
            ),
            (
                minimal_v4_openapi_manifest(
                    "  auth:\n    type: HeaderAuth\n    headers:\n      - name: Authorization\n        from: literal\n        value: Bearer hardcoded-token\n",
                ),
                "surface auth header 'Authorization' must reference a secret input",
            ),
            (
                minimal_v4_openapi_manifest(
                    "  request_headers:\n    - name: Cookie\n      from: literal\n      value: session=hardcoded-token\n",
                ),
                "surface request_headers header 'Cookie' must reference a secret input",
            ),
            (
                minimal_http_manifest(
                    "auth:\n  type: HeaderAuth\n  headers:\n    - name: Cookie\n      from: literal\n      value: session=hardcoded-token\n",
                ),
                "auth header 'Cookie' must reference a secret input",
            ),
            (
                minimal_http_manifest_with_route(
                    "        headers:\n          - name: Authorization\n            from: literal\n            value: Bearer hardcoded-token\n",
                ),
                "table 'messages' request route request headers header 'Authorization' must reference a secret input",
            ),
            (
                minimal_http_function_manifest(
                    "      headers:\n        - name: Authorization\n          from: literal\n          value: Bearer hardcoded-token\n",
                ),
                "function 'search_messages' request headers header 'Authorization' must reference a secret input",
            ),
        ];

        for (manifest, expected) in cases {
            let error = validate_imported_manifest_database_persistence(&manifest)
                .expect_err("literal sensitive value should be rejected");
            assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, got {error}"
            );
        }
    }

    #[test]
    fn database_persistence_rejects_basic_auth_username_when_it_is_credential_marked() {
        let error = validate_imported_manifest_database_persistence(&minimal_http_manifest(
            "inputs:\n  WOOCOMMERCE_CONSUMER_KEY:\n    kind: variable\n  API_PASSWORD:\n    kind: secret\nauth:\n  type: BasicAuth\n  username: \"{{input.WOOCOMMERCE_CONSUMER_KEY}}\"\n  password: \"{{input.API_PASSWORD}}\"\n",
        ))
        .expect_err("credential-marked BasicAuth username should be rejected");

        assert!(
            error
                .to_string()
                .contains("auth.username must reference a secret input"),
            "{error}"
        );
    }

    #[test]
    fn database_persistence_rejects_one_of_sensitive_fallbacks() {
        let error = validate_imported_manifest_database_persistence(&minimal_http_manifest(
            "inputs:\n  API_TOKEN:\n    kind: secret\nauth:\n  type: HeaderAuth\n  headers:\n    - name: Authorization\n      from: one_of\n      values:\n        - from: input\n          key: API_TOKEN\n        - from: literal\n          value: Bearer fallback-token\n",
        ))
        .expect_err("literal one_of fallback should be rejected");

        assert!(
            error
                .to_string()
                .contains("auth header 'Authorization' must reference a secret input"),
            "{error}"
        );
    }

    #[test]
    fn database_persistence_rejects_file_s3_credentials_not_backed_by_secret_inputs() {
        let cases = [
            (
                minimal_file_manifest(
                    "inputs:\n  AWS_SECRET_ACCESS_KEY:\n    kind: secret\n",
                    "      object_store:\n        type: s3\n        auth:\n          type: access_key\n          access_key_id: hardcoded-access-key\n          secret_access_key: \"{{input.AWS_SECRET_ACCESS_KEY}}\"\n",
                ),
                "source.object_store.auth.access_key_id must reference a secret input",
            ),
            (
                minimal_file_manifest(
                    "inputs:\n  AWS_ACCESS_KEY_ID:\n    kind: secret\n",
                    "      object_store:\n        type: s3\n        auth:\n          type: access_key\n          access_key_id: \"{{input.AWS_ACCESS_KEY_ID}}\"\n          secret_access_key: hardcoded-secret-key\n",
                ),
                "source.object_store.auth.secret_access_key must reference a secret input",
            ),
            (
                minimal_file_manifest(
                    "inputs:\n  AWS_ACCESS_KEY_ID:\n    kind: secret\n  AWS_SECRET_ACCESS_KEY:\n    kind: secret\n",
                    "      object_store:\n        type: s3\n        auth:\n          type: access_key\n          access_key_id: \"{{input.AWS_ACCESS_KEY_ID}}\"\n          secret_access_key: \"{{input.AWS_SECRET_ACCESS_KEY}}\"\n          session_token: hardcoded-session-token\n",
                ),
                "source.object_store.auth.session_token must reference a secret input",
            ),
        ];

        for (manifest, expected) in cases {
            let error = validate_imported_manifest_database_persistence(&manifest)
                .expect_err("S3 credential should be rejected");
            assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, got {error}"
            );
        }
    }

    #[test]
    fn database_persistence_rejects_sensitive_mcp_stdio_env_values() {
        let cases = [
            (
                minimal_mcp_manifest(
                    "  transport: stdio\n  command: demo-mcp\n  env:\n    - name: API_TOKEN\n      from: literal\n      value: hardcoded-token\n",
                ),
                "server env 'API_TOKEN' must reference a secret input",
            ),
            (
                minimal_v4_mcp_manifest(
                    "    transport: stdio\n    command: demo-mcp\n    env:\n      - name: API_TOKEN\n        from: literal\n        value: hardcoded-token\n",
                ),
                "surface server env 'API_TOKEN' must reference a secret input",
            ),
        ];

        for (manifest, expected) in cases {
            let error = validate_imported_manifest_database_persistence(&manifest)
                .expect_err("sensitive MCP env value should be rejected");
            assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, got {error}"
            );
        }
    }

    #[test]
    fn database_persistence_allows_literal_non_secret_headers() {
        validate_imported_manifest_database_persistence(&minimal_http_manifest("auth:\n  type: HeaderAuth\n  headers:\n    - name: Travis-API-Version\n      from: literal\n      value: \"3\"\nrequest_headers:\n  - name: Accept\n    from: literal\n    value: application/json\n"))
        .expect("non-secret literal headers should be allowed");

        validate_imported_manifest_database_persistence(&minimal_http_manifest_with_request("      body:\n        format: text\n        content:\n          from: literal\n          value: \"SELECT id, name FROM users FORMAT JSONEachRow\"\n"))
        .expect("non-secret literal text bodies should be allowed");

        validate_imported_manifest_database_persistence(&minimal_http_manifest_with_request("      query:\n        - name: page_token\n          from: filter\n          key: page_token\n"))
        .expect("pagination token filters should not be treated as credentials");
    }

    #[test]
    fn database_persistence_allows_sensitive_header_from_secret_input() {
        validate_imported_manifest_database_persistence(&minimal_http_manifest("inputs:\n  API_TOKEN:\n    kind: secret\nauth:\n  type: HeaderAuth\n  headers:\n    - name: Authorization\n      from: template\n      template: Bearer {{input.API_TOKEN}}\n"))
        .expect("secret-backed auth header should be allowed");

        validate_imported_manifest_database_persistence(&minimal_http_manifest("inputs:\n  API_PASSWORD:\n    kind: secret\nauth:\n  type: BasicAuth\n  username: public-user\n  password: \"{{input.API_PASSWORD}}\"\n"))
        .expect("public BasicAuth username should be allowed with a secret-backed password");
    }
}
