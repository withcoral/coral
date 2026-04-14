use std::io::{IsTerminal, stdin, stdout};
use std::path::Path;

use coral_api::v1::{
    AvailableSource, CreateBundledSourceRequest, DeleteSourceRequest, DiscoverSourcesRequest,
    ImportSourceRequest, ListSourcesRequest, Source, SourceInputKind, SourceInputSpec,
    SourceOrigin, SourceSecret, SourceVariable, ValidateSourceRequest, ValidateSourceResponse,
};
use coral_client::{AppClient, default_workspace};
use coral_spec::{ManifestInputKind, ManifestInputSpec, collect_source_inputs_yaml};
use dialoguer::{Input, Password, theme::ColorfulTheme};
use tonic::Request;

pub(crate) async fn discover_sources(
    app: &AppClient,
) -> Result<Vec<AvailableSource>, anyhow::Error> {
    Ok(app
        .source_client()
        .discover_sources(Request::new(DiscoverSourcesRequest {
            workspace: Some(default_workspace()),
        }))
        .await?
        .into_inner()
        .sources)
}

pub(crate) async fn list_sources(app: &AppClient) -> Result<Vec<Source>, anyhow::Error> {
    Ok(app
        .source_client()
        .list_sources(Request::new(ListSourcesRequest {
            workspace: Some(default_workspace()),
        }))
        .await?
        .into_inner()
        .sources)
}

pub(crate) async fn add_bundled_source(
    app: &AppClient,
    name: &str,
    variables: Vec<SourceVariable>,
    secrets: Vec<SourceSecret>,
) -> Result<Source, anyhow::Error> {
    Ok(app
        .source_client()
        .create_bundled_source(Request::new(CreateBundledSourceRequest {
            workspace: Some(default_workspace()),
            name: name.to_string(),
            variables,
            secrets,
        }))
        .await?
        .into_inner())
}

pub(crate) async fn import_source(
    app: &AppClient,
    manifest_yaml: String,
    variables: Vec<SourceVariable>,
    secrets: Vec<SourceSecret>,
) -> Result<Source, anyhow::Error> {
    Ok(app
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml,
            variables,
            secrets,
        }))
        .await?
        .into_inner())
}

pub(crate) async fn validate_source(
    app: &AppClient,
    name: &str,
) -> Result<ValidateSourceResponse, anyhow::Error> {
    Ok(app
        .source_client()
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name: source_name_arg(Some(name))?,
        }))
        .await?
        .into_inner())
}

pub(crate) async fn delete_source(app: &AppClient, name: &str) -> Result<(), anyhow::Error> {
    app.source_client()
        .delete_source(Request::new(DeleteSourceRequest {
            workspace: Some(default_workspace()),
            name: source_name_arg(Some(name))?,
        }))
        .await?;
    Ok(())
}

pub(crate) fn require_interactive() -> Result<(), anyhow::Error> {
    if !stdin().is_terminal() || !stdout().is_terminal() {
        return Err(anyhow::anyhow!("interactive source install requires a TTY"));
    }
    Ok(())
}

pub(crate) fn source_name_arg(name: Option<&str>) -> Result<String, anyhow::Error> {
    let Some(name) = name else {
        return Err(anyhow::anyhow!("missing source name"));
    };
    let name = name.trim();
    if name.is_empty() {
        return Err(anyhow::anyhow!("missing source name"));
    }
    if name.contains('/') || name.contains('\\') {
        return Err(anyhow::anyhow!(
            "source name must not contain '/' or '\\\\'"
        ));
    }
    Ok(name.to_string())
}

pub(crate) fn prompt_for_inputs(
    inputs: &[ManifestInputSpec],
) -> Result<(Vec<SourceVariable>, Vec<SourceSecret>), anyhow::Error> {
    let mut variables = Vec::new();
    let mut secrets = Vec::new();

    for input in inputs {
        match input.kind {
            ManifestInputKind::Variable => {
                if let Some(variable) = prompt_variable(input)? {
                    variables.push(variable);
                }
            }
            ManifestInputKind::Secret => {
                if let Some(secret) = prompt_secret(input)? {
                    secrets.push(secret);
                }
            }
        }
    }

    Ok((variables, secrets))
}

pub(crate) fn manifest_input_from_proto(
    input: &SourceInputSpec,
) -> Result<ManifestInputSpec, anyhow::Error> {
    let kind = match SourceInputKind::try_from(input.kind) {
        Ok(SourceInputKind::Variable) => ManifestInputKind::Variable,
        Ok(SourceInputKind::Secret) => ManifestInputKind::Secret,
        Ok(SourceInputKind::Unspecified) | Err(_) => {
            return Err(anyhow::anyhow!("unknown input kind for '{}'", input.key));
        }
    };
    Ok(ManifestInputSpec {
        key: input.key.clone(),
        kind,
        required: input.required,
        default_value: input.default_value.clone(),
    })
}

pub(crate) fn load_manifest_inputs(
    path: &Path,
) -> Result<(String, Vec<ManifestInputSpec>), anyhow::Error> {
    let manifest_yaml = std::fs::read_to_string(path)?;
    let inputs = collect_source_inputs_yaml(&manifest_yaml)?;
    Ok((manifest_yaml, inputs))
}

pub(crate) fn source_origin_label(origin: i32) -> &'static str {
    match SourceOrigin::try_from(origin) {
        Ok(SourceOrigin::Bundled) => "bundled",
        Ok(SourceOrigin::Imported) => "imported",
        Ok(SourceOrigin::Unspecified) | Err(_) => "unknown",
    }
}

pub(crate) fn print_validation_success(
    response: &ValidateSourceResponse,
) -> Result<(), anyhow::Error> {
    let source = response
        .source
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("validate response missing source metadata"))?;
    println!("Source {} is queryable", source.name);
    for table in &response.tables {
        println!("{}.{}", table.schema_name, table.name);
    }
    Ok(())
}

fn prompt_variable(input: &ManifestInputSpec) -> Result<Option<SourceVariable>, anyhow::Error> {
    let theme = ColorfulTheme::default();
    let prompt = if input.default_value.is_empty() {
        input.key.clone()
    } else {
        format!("{} [{}]", input.key, input.default_value)
    };
    let value = Input::<String>::with_theme(&theme)
        .with_prompt(prompt)
        .allow_empty(true)
        .interact_text()?;
    let Some(value) = finalize_input_value(input, value, "source variable")? else {
        return Ok(None);
    };
    Ok(Some(SourceVariable {
        key: input.key.clone(),
        value,
    }))
}

fn prompt_secret(input: &ManifestInputSpec) -> Result<Option<SourceSecret>, anyhow::Error> {
    let theme = ColorfulTheme::default();
    let prompt = if input.default_value.is_empty() {
        input.key.clone()
    } else {
        format!("{} [default hidden]", input.key)
    };
    let value = Password::with_theme(&theme)
        .with_prompt(prompt)
        .allow_empty_password(true)
        .interact()?;
    let Some(value) = finalize_input_value(input, value, "source secret")? else {
        return Ok(None);
    };
    Ok(Some(SourceSecret {
        key: input.key.clone(),
        value,
    }))
}

pub(crate) fn finalize_input_value(
    input: &ManifestInputSpec,
    value: String,
    kind_label: &str,
) -> Result<Option<String>, anyhow::Error> {
    if !value.is_empty() {
        return Ok(Some(value));
    }
    if !input.default_value.is_empty() {
        return Ok(Some(input.default_value.clone()));
    }
    if input.required {
        return Err(anyhow::anyhow!(
            "missing required {kind_label} '{}'",
            input.key
        ));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use coral_spec::{ManifestInputKind, ManifestInputSpec};

    use super::finalize_input_value;

    #[test]
    fn empty_input_uses_default_value() {
        let input = ManifestInputSpec {
            key: "API_BASE".to_string(),
            kind: ManifestInputKind::Variable,
            required: false,
            default_value: "https://example.com".to_string(),
        };
        assert_eq!(
            finalize_input_value(&input, String::new(), "source variable")
                .expect("default should apply"),
            Some("https://example.com".to_string())
        );
    }

    #[test]
    fn empty_required_input_without_default_is_rejected() {
        let input = ManifestInputSpec {
            key: "API_TOKEN".to_string(),
            kind: ManifestInputKind::Secret,
            required: true,
            default_value: String::new(),
        };
        let error = finalize_input_value(&input, String::new(), "source secret")
            .expect_err("required empty input should fail");
        assert!(error.to_string().contains("missing required source secret"));
    }
}
