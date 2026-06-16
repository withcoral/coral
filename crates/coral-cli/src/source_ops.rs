use std::collections::{BTreeMap, BTreeSet};
use std::io::{IsTerminal, Read as _, Write, stdin, stdout};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{Context as _, bail};
use coral_api::CORAL_ERROR_REASON_SOURCE_NOT_FOUND;
use coral_api::v1::{
    AddIdentitySpecRequest, CreateBundledSourceRequest, CreateBundledSourceWithOAuthRequest,
    CreateIdentitySpecRequest, CreateUserOwnedIdentityWithFixedTokenRequest,
    CreateUserOwnedIdentityWithOAuthRequest, DeleteIdentitySpecRequest, DeleteSourceRequest,
    DeleteUserOwnedIdentityRequest, DiscoverSourcesRequest, GetIdentitySpecRequest,
    GetSourceInfoRequest, GetUserOwnedIdentityRequest, Identity, IdentityOwner, IdentitySpec,
    IdentitySpecImportInputs, IdentitySpecInput, ImportSourceRequest, ListIdentitySpecsRequest,
    ListSourcesRequest, ListUserOwnedIdentitiesRequest, OAuthCredentialInput,
    OAuthCredentialRetrieval, QueryTestFailure, QueryTestSuccess, Source, SourceCredentialStorage,
    SourceIdentityBinding, SourceIdentityOwner, SourceInfo, SourceOrigin, SourceSecret,
    SourceVariable, UserSourceIdentityBinding, ValidateSourceRequest, ValidateSourceResponse,
    create_bundled_source_with_o_auth_response, create_user_owned_identity_with_o_auth_response,
    import_source_response, query_test_result, source_input_spec::Input as ProtoSourceInput,
};
use coral_client::{AppClient, DecodedStatusError, decode_status_error, default_workspace};
use coral_spec::v4::{AcceptedIdentityRequirement, SurfaceDescriptor};
use coral_spec::{
    IdentityManifest, IdentityManifestDocument, IdentitySpecConfig, IdentitySpecType,
    ManifestCredentialMethod, ManifestCredentialMethodKind, ManifestCredentialSpec,
    ManifestInputKind, ManifestInputSpec, ManifestOAuthCredentialSpec, ValidatedSourceManifest,
    parse_identity_manifest_yaml, parse_manifest_bundle_yaml, parse_source_manifest_yaml,
};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use dialoguer::console::style;
use dialoguer::{Input, Password, Select, theme::ColorfulTheme};
use serde_yaml::Value as YamlValue;
use tonic::Request;
use url::{Host, Url};

const MAX_TABLES_PER_SCHEMA: usize = 9;
const IDENTITY_OAUTH_PROGRESS_INPUT_KEY: &str = "ACCESS_TOKEN";

/// How many tables to show per schema when pretty-printing validation results.
#[derive(Debug, Clone, Copy)]
pub(crate) enum TableDisplayLimit {
    /// Show every table the source exposes.
    All,
    /// Show at most this many tables per schema, with a summary for the rest.
    Max(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValidationSeverityMode {
    Strict,
    WarnOnly,
}

#[derive(Debug, PartialEq, Eq)]
enum ValidationFollowUp {
    None,
    Warn(String),
    Fail(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QueryTestCounts {
    declared: usize,
    passed: usize,
    failed: usize,
}

impl TableDisplayLimit {
    /// The default truncation used after `source add` and during onboarding.
    pub(crate) const DEFAULT: Self = Self::Max(MAX_TABLES_PER_SCHEMA);
}

pub(crate) async fn discover_sources(app: &AppClient) -> Result<Vec<SourceInfo>, anyhow::Error> {
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

/// Generates a thin gRPC call wrapper: builds the request, sends it through
/// the named client, and maps the response with `$map` (`response` in scope).
macro_rules! rpc_fn {
    (
        $(#[$meta:meta])*
        fn $name:ident($($arg:ident: $arg_ty:ty),*) -> $ok:ty;
        $client:ident.$rpc:ident($req:expr);
        |$response:ident| $map:expr
    ) => {
        $(#[$meta])*
        pub(crate) async fn $name(
            app: &AppClient,
            $($arg: $arg_ty),*
        ) -> Result<$ok, anyhow::Error> {
            let $response = app.$client().$rpc(Request::new($req)).await?.into_inner();
            Ok($map)
        }
    };
}

rpc_fn! {
    fn list_identity_specs() -> Vec<IdentitySpec>;
    identity_spec_client.list_identity_specs(ListIdentitySpecsRequest {});
    |response| response.identity_specs
}

rpc_fn! {
    fn get_identity_spec(name: &str) -> IdentitySpec;
    identity_spec_client.get_identity_spec(GetIdentitySpecRequest {
        name: name.to_string(),
    });
    |response| response
        .identity_spec
        .ok_or_else(|| anyhow::anyhow!("get identity spec response missing identity_spec"))?
}

rpc_fn! {
    fn add_identity_spec(
        manifest_yaml: String,
        inputs: Vec<IdentitySpecInput>
    ) -> (IdentitySpec, bool);
    identity_spec_client.add_identity_spec(AddIdentitySpecRequest {
        manifest_yaml,
        inputs,
    });
    |response| (
        response
            .identity_spec
            .ok_or_else(|| anyhow::anyhow!("add identity spec response missing identity_spec"))?,
        response.replaced,
    )
}

rpc_fn! {
    fn create_identity_spec(
        manifest_yaml: String,
        inputs: Vec<IdentitySpecInput>
    ) -> IdentitySpec;
    identity_spec_client.create_identity_spec(CreateIdentitySpecRequest {
        manifest_yaml,
        inputs,
    });
    |response| response
        .identity_spec
        .ok_or_else(|| anyhow::anyhow!("create identity spec response missing identity_spec"))?
}

rpc_fn! {
    fn remove_identity_spec(name: &str, force: bool) -> u32;
    identity_spec_client.delete_identity_spec(DeleteIdentitySpecRequest {
        name: name.to_string(),
        force,
    });
    |response| response.orphaned_identities
}

rpc_fn! {
    fn list_user_owned_identities() -> Vec<Identity>;
    identity_client.list_user_owned_identities(ListUserOwnedIdentitiesRequest {});
    |response| response.identities
}

rpc_fn! {
    fn get_user_owned_identity(name: &str) -> Identity;
    identity_client.get_user_owned_identity(GetUserOwnedIdentityRequest {
        name: name.to_string(),
    });
    |response| response
        .identity
        .ok_or_else(|| anyhow::anyhow!("get user-owned identity response missing identity"))?
}

rpc_fn! {
    fn delete_user_owned_identity(name: &str) -> ();
    identity_client.delete_user_owned_identity(DeleteUserOwnedIdentityRequest {
        name: name.to_string(),
    });
    |response| {
        let _ = response;
    }
}

pub(crate) async fn create_user_owned_identity_with_oauth(
    app: &AppClient,
    name: &str,
    identity_spec: &str,
    credential_inputs: Vec<OAuthCredentialInput>,
    label: &str,
    retry_command: &'static str,
) -> Result<Identity, anyhow::Error> {
    let response = app
        .identity_client()
        .create_user_owned_identity_with_o_auth(Request::new(
            CreateUserOwnedIdentityWithOAuthRequest {
                name: name.to_string(),
                identity_spec: identity_spec.to_string(),
                credential_inputs,
            },
        ))
        .await?;
    let oauth_labels = BTreeMap::from([(
        IDENTITY_OAUTH_PROGRESS_INPUT_KEY.to_string(),
        label.to_string(),
    )]);
    completion_from_oauth_stream(
        response.into_inner(),
        &oauth_labels,
        OAuthStreamContext {
            ended_message: "identity OAuth stream ended before identity creation completed",
            error_action: "identity creation",
            retry_command,
        },
        |response| response.event.map(CredentialStreamEvent::from),
    )
    .await
}

pub(crate) async fn create_user_owned_identity_with_fixed_token(
    app: &AppClient,
    name: &str,
    identity_spec: &str,
    token: String,
) -> Result<Identity, anyhow::Error> {
    let response = app
        .identity_client()
        .create_user_owned_identity_with_fixed_token(Request::new(
            CreateUserOwnedIdentityWithFixedTokenRequest {
                name: name.to_string(),
                identity_spec: identity_spec.to_string(),
                token,
            },
        ))
        .await?
        .into_inner();
    response
        .identity
        .ok_or_else(|| anyhow::anyhow!("create fixed-token identity response missing identity"))
}

pub(crate) async fn add_bundled_source(
    app: &AppClient,
    name: &str,
    variables: Vec<SourceVariable>,
    secrets: Vec<SourceSecret>,
) -> Result<Source, anyhow::Error> {
    let response = app
        .source_client()
        .create_bundled_source(Request::new(CreateBundledSourceRequest {
            workspace: Some(default_workspace()),
            name: name.to_string(),
            variables,
            secrets,
        }))
        .await?
        .into_inner();
    response
        .source
        .ok_or_else(|| anyhow::anyhow!("create bundled source response missing source"))
}

pub(crate) async fn import_source(
    app: &AppClient,
    manifest_yaml: String,
    identity_specs: ImportSourceIdentitySpecs,
    identity_bindings: Vec<SourceIdentityBinding>,
    user_identity_bindings: Vec<UserSourceIdentityBinding>,
    variables: Vec<SourceVariable>,
    secrets: Vec<SourceSecret>,
) -> Result<Source, anyhow::Error> {
    let mut responses = app
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml,
            variables,
            secrets,
            oauth_credential_retrievals: Vec::new(),
            identity_spec_manifest_yamls: identity_specs.manifest_yamls,
            identity_spec_inputs: identity_specs.inputs,
            identity_bindings,
            user_identity_bindings,
            replace_identity_bindings: true,
        }))
        .await?
        .into_inner();
    while let Some(response) = responses.message().await? {
        if let Some(import_source_response::Event::Source(source)) = response.event {
            return Ok(source);
        }
    }
    Err(anyhow::anyhow!("import source stream ended without source"))
}

#[derive(Default)]
pub(crate) struct ImportSourceIdentitySpecs {
    pub(crate) manifest_yamls: Vec<String>,
    pub(crate) inputs: Vec<IdentitySpecImportInputs>,
}

pub(crate) struct CollectedSourceInputs {
    pub(crate) variables: Vec<SourceVariable>,
    pub(crate) secrets: Vec<SourceSecret>,
    oauth_credential_retrievals: Vec<OAuthCredentialRetrieval>,
    oauth_labels: BTreeMap<String, String>,
}

impl CollectedSourceInputs {
    fn new() -> Self {
        Self {
            variables: Vec::new(),
            secrets: Vec::new(),
            oauth_credential_retrievals: Vec::new(),
            oauth_labels: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CredentialPromptMode {
    EnvFirst,
    CredentialMethodFirst,
}

impl CredentialPromptMode {
    fn reads_env_before_prompt(self, input: &ManifestInputSpec) -> bool {
        match self {
            Self::EnvFirst => true,
            Self::CredentialMethodFirst => {
                input.kind == ManifestInputKind::Variable || input.credential.is_none()
            }
        }
    }
}

pub(crate) async fn add_bundled_source_with_credentials(
    app: &AppClient,
    name: &str,
    inputs: CollectedSourceInputs,
) -> Result<Source, anyhow::Error> {
    if inputs.oauth_credential_retrievals.is_empty() {
        return add_bundled_source(app, name, inputs.variables, inputs.secrets).await;
    }
    let response = app
        .source_client()
        .create_bundled_source_with_o_auth(Request::new(CreateBundledSourceWithOAuthRequest {
            workspace: Some(default_workspace()),
            name: name.to_string(),
            variables: inputs.variables,
            secrets: inputs.secrets,
            oauth_credential_retrievals: inputs.oauth_credential_retrievals,
        }))
        .await?;
    completion_from_oauth_stream(
        response.into_inner(),
        &inputs.oauth_labels,
        OAuthStreamContext {
            ended_message:
                "source credential retrieval stream ended before source installation completed",
            error_action: "retrieve",
            retry_command: "coral source add",
        },
        |response| response.event.map(CredentialStreamEvent::from),
    )
    .await
}

pub(crate) async fn import_source_with_credentials(
    app: &AppClient,
    manifest_yaml: String,
    identity_specs: ImportSourceIdentitySpecs,
    identity_bindings: Vec<SourceIdentityBinding>,
    user_identity_bindings: Vec<UserSourceIdentityBinding>,
    inputs: CollectedSourceInputs,
) -> Result<Source, anyhow::Error> {
    if inputs.oauth_credential_retrievals.is_empty() {
        return import_source(
            app,
            manifest_yaml,
            identity_specs,
            identity_bindings,
            user_identity_bindings,
            inputs.variables,
            inputs.secrets,
        )
        .await;
    }
    let response = app
        .source_client()
        .import_source(Request::new(ImportSourceRequest {
            workspace: Some(default_workspace()),
            manifest_yaml,
            variables: inputs.variables,
            secrets: inputs.secrets,
            oauth_credential_retrievals: inputs.oauth_credential_retrievals,
            identity_spec_manifest_yamls: identity_specs.manifest_yamls,
            identity_spec_inputs: identity_specs.inputs,
            identity_bindings,
            user_identity_bindings,
            replace_identity_bindings: true,
        }))
        .await?;
    completion_from_oauth_stream(
        response.into_inner(),
        &inputs.oauth_labels,
        OAuthStreamContext {
            ended_message:
                "source credential retrieval stream ended before source import completed",
            error_action: "retrieve",
            retry_command: "coral source add",
        },
        |response| response.event.map(CredentialStreamEvent::from),
    )
    .await
}

pub(crate) fn source_has_identity_requirements(manifest: &ValidatedSourceManifest) -> bool {
    source_identity_requirements(manifest).next().is_some()
}

pub(crate) async fn prepare_identity_specs_for_source_import(
    app: &AppClient,
    identity_manifests: &[IdentityManifestDocument],
    interactive: bool,
    interactive_command: &str,
) -> Result<ImportSourceIdentitySpecs, anyhow::Error> {
    let mut identity_specs = ImportSourceIdentitySpecs::default();
    for document in identity_manifests {
        if matching_identity_spec_exists(app, document).await? {
            continue;
        }
        let inputs = identity_spec_inputs_for_add(
            &document.manifest,
            interactive,
            interactive_command.to_string(),
        )?;
        identity_specs
            .manifest_yamls
            .push(document.manifest_yaml.clone());
        if !inputs.is_empty() {
            identity_specs.inputs.push(IdentitySpecImportInputs {
                identity_spec_name: document.manifest.name.clone(),
                inputs,
            });
        }
    }
    Ok(identity_specs)
}

pub(crate) async fn install_identity_specs_for_source_add(
    app: &AppClient,
    identity_manifests: &[IdentityManifestDocument],
    interactive: bool,
    interactive_command: &str,
) -> Result<Vec<String>, anyhow::Error> {
    let mut prepared = Vec::new();
    for document in identity_manifests {
        if matching_identity_spec_exists(app, document).await? {
            continue;
        }
        let inputs = identity_spec_inputs_for_add(
            &document.manifest,
            interactive,
            interactive_command.to_string(),
        )?;
        prepared.push((document, inputs));
    }

    let mut newly_installed_names = Vec::new();
    for (document, inputs) in prepared {
        let exists = match matching_identity_spec_exists(app, document).await {
            Ok(exists) => exists,
            Err(error) => {
                rollback_identity_specs_for_source_add(newly_installed_names);
                return Err(error);
            }
        };
        if exists {
            if !document.manifest.inputs.is_empty() {
                rollback_identity_specs_for_source_add(newly_installed_names);
                bail!(
                    "identity spec '{}' was installed concurrently while source add was preparing setup inputs; retry source add",
                    document.manifest.name
                );
            }
            continue;
        }
        let has_setup_inputs = !document.manifest.inputs.is_empty();
        match create_identity_spec(app, document.manifest_yaml.clone(), inputs).await {
            Ok(_spec) => {
                newly_installed_names.push(document.manifest.name.clone());
            }
            Err(error) => {
                if tonic_status_code(&error) == Some(tonic::Code::FailedPrecondition) {
                    if has_setup_inputs {
                        rollback_identity_specs_for_source_add(newly_installed_names);
                        bail!(
                            "identity spec '{}' was installed concurrently while source add was preparing setup inputs; retry source add",
                            document.manifest.name
                        );
                    }
                    match matching_identity_spec_exists(app, document).await {
                        Ok(true) => continue,
                        Ok(false) => {}
                        Err(recheck_error) => {
                            rollback_identity_specs_for_source_add(newly_installed_names);
                            return Err(recheck_error.context(format!(
                                "failed to re-check identity spec '{}' after create-only install conflict",
                                document.manifest.name
                            )));
                        }
                    }
                }
                rollback_identity_specs_for_source_add(newly_installed_names);
                return Err(error);
            }
        }
    }
    Ok(newly_installed_names)
}

pub(crate) async fn parseable_installed_identity_spec_manifest(
    app: &AppClient,
    name: &str,
) -> Result<Option<IdentityManifest>, anyhow::Error> {
    match get_identity_spec(app, name).await {
        Ok(existing) => Ok(parse_identity_manifest_yaml(&existing.manifest_yaml).ok()),
        Err(error) if tonic_status_code(&error) == Some(tonic::Code::NotFound) => Ok(None),
        Err(error) => Err(error),
    }
}

async fn matching_identity_spec_exists(
    app: &AppClient,
    document: &IdentityManifestDocument,
) -> Result<bool, anyhow::Error> {
    match get_identity_spec(app, &document.manifest.name).await {
        Ok(existing) => {
            let existing_manifest = parse_identity_manifest_yaml(&existing.manifest_yaml)
                .with_context(|| {
                    format!(
                        "installed identity spec '{}' could not be parsed",
                        document.manifest.name
                    )
                })?;
            if existing_manifest != document.manifest {
                bail!(
                    "identity spec '{}' is already installed with a different manifest; remove or update it before using this source bundle",
                    document.manifest.name
                );
            }
            Ok(true)
        }
        Err(error) => {
            if tonic_status_code(&error) == Some(tonic::Code::NotFound) {
                return Ok(false);
            }
            Err(error)
        }
    }
}

fn tonic_status_code(error: &anyhow::Error) -> Option<tonic::Code> {
    error
        .downcast_ref::<tonic::Status>()
        .map(tonic::Status::code)
}

pub(crate) fn rollback_identity_specs_for_source_add(identity_spec_names: Vec<String>) {
    for name in identity_spec_names {
        eprintln!("{}", identity_spec_left_installed_warning(&name));
    }
}

pub(crate) fn rollback_user_owned_identities_for_source_add(identity_names: Vec<String>) {
    for name in identity_names {
        eprintln!("{}", user_owned_identity_left_installed_warning(&name));
    }
}

fn identity_spec_left_installed_warning(name: &str) -> String {
    format!(
        "Warning: source add installed identity spec '{name}' before a later step failed. It was left installed because deleting by name could remove concurrently changed state. Retry source add to reuse it, or remove it through the identity-spec management API after confirming no concurrent source add is using it."
    )
}

fn user_owned_identity_left_installed_warning(name: &str) -> String {
    format!(
        "Warning: source add created user-owned identity '{name}' before a later step failed. It was left installed because deleting by name could remove concurrently changed credential state. Retry source add to reuse or select it, or remove it through the identity management API after confirming it still belongs to this failed setup."
    )
}

pub(crate) async fn prompt_for_source_identity_bindings(
    app: &AppClient,
    manifest: &ValidatedSourceManifest,
    identity_manifests: &[IdentityManifestDocument],
) -> Result<PromptedSourceIdentityBindings, anyhow::Error> {
    let requirements = source_identity_requirements(manifest).collect::<Vec<_>>();
    if requirements.is_empty() {
        return Ok(PromptedSourceIdentityBindings::default());
    }
    let mut available_identities = list_user_owned_identities(app).await?;
    let mut reserved_identity_names = available_identities
        .iter()
        .map(|identity| identity.name.clone())
        .collect::<BTreeSet<_>>();
    let mut identity_specs = identity_manifests
        .iter()
        .map(|document| (document.manifest.name.clone(), document.manifest.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut identity_bindings = Vec::new();
    let mut user_identity_bindings = Vec::new();
    let mut created_user_identities = Vec::new();
    for requirement in requirements {
        let selected = match select_or_create_identity_for_surface(
            app,
            manifest.schema_name(),
            &requirement,
            &available_identities,
            &mut reserved_identity_names,
            &mut identity_specs,
        )
        .await
        {
            Ok(selected) => selected,
            Err(error) => {
                rollback_user_owned_identities_for_source_add(created_user_identities);
                return Err(error);
            }
        };
        if !available_identities
            .iter()
            .any(|identity| identity.name == selected.identity.name)
        {
            available_identities.push(selected.identity.clone());
        }
        if selected.created {
            created_user_identities.push(selected.identity.name.clone());
        }
        identity_bindings.push(SourceIdentityBinding {
            surface_id: requirement.surface_id.clone(),
            identity: String::new(),
            owner: SourceIdentityOwner::User as i32,
            accepted_identity: String::new(),
        });
        user_identity_bindings.push(UserSourceIdentityBinding {
            surface_id: requirement.surface_id.clone(),
            identity: selected.identity.name,
            accepted_identity: selected.accepted_identity,
        });
    }
    Ok(PromptedSourceIdentityBindings {
        identity_bindings,
        user_identity_bindings,
        created_user_identities,
    })
}

#[derive(Default)]
pub(crate) struct PromptedSourceIdentityBindings {
    pub(crate) identity_bindings: Vec<SourceIdentityBinding>,
    pub(crate) user_identity_bindings: Vec<UserSourceIdentityBinding>,
    pub(crate) created_user_identities: Vec<String>,
}

#[derive(Clone)]
struct SurfaceIdentityRequirement {
    surface_id: String,
    accepts: Vec<AcceptedIdentityRequirement>,
}

#[derive(Clone)]
struct SelectedSurfaceIdentity {
    identity: Identity,
    accepted_identity: String,
    created: bool,
}

#[derive(Clone)]
struct IdentityCreationOption {
    accepted_identity: String,
    identity_spec: IdentityManifest,
    method: IdentityCreationMethod,
}

#[derive(Clone)]
enum IdentityCreationMethod {
    OAuth { label: String },
    FixedToken,
}

impl IdentityCreationMethod {
    fn label(&self) -> &str {
        match self {
            Self::OAuth { label } => label,
            Self::FixedToken => "Fixed token",
        }
    }
}

fn source_identity_requirements(
    manifest: &ValidatedSourceManifest,
) -> impl Iterator<Item = SurfaceIdentityRequirement> + '_ {
    manifest.as_v4().into_iter().flat_map(|v4| {
        v4.surfaces.iter().filter_map(|surface| {
            surface
                .identity_requirements
                .as_ref()
                .map(|requirements| SurfaceIdentityRequirement {
                    surface_id: surface.id.clone(),
                    accepts: requirements.accepts.clone(),
                })
        })
    })
}

async fn select_or_create_identity_for_surface(
    app: &AppClient,
    source_name: &str,
    requirement: &SurfaceIdentityRequirement,
    existing_identities: &[Identity],
    reserved_identity_names: &mut BTreeSet<String>,
    identity_specs: &mut BTreeMap<String, IdentityManifest>,
) -> Result<SelectedSurfaceIdentity, anyhow::Error> {
    let compatible =
        compatible_existing_identities(app, requirement, existing_identities, identity_specs)
            .await?;
    if let Some(selected) =
        prompt_existing_identity_or_create(&requirement.surface_id, &compatible)?
    {
        return Ok(selected);
    }
    create_identity_for_surface(
        app,
        source_name,
        requirement,
        reserved_identity_names,
        identity_specs,
    )
    .await
}

async fn compatible_existing_identities(
    app: &AppClient,
    requirement: &SurfaceIdentityRequirement,
    existing_identities: &[Identity],
    identity_specs: &mut BTreeMap<String, IdentityManifest>,
) -> Result<Vec<SelectedSurfaceIdentity>, anyhow::Error> {
    let mut compatible = Vec::new();
    for identity in existing_identities {
        let Some(identity_spec) =
            identity_spec_manifest_cached(app, identity_specs, &identity.identity_spec).await?
        else {
            continue;
        };
        if let Some(accepted) = requirement.accepts.iter().find(|accepted| {
            accepted
                .identity_specs
                .iter()
                .any(|name| name == &identity.identity_spec)
                && audience_matches(&accepted.audience, &identity_spec.audience)
        }) {
            compatible.push(SelectedSurfaceIdentity {
                identity: identity.clone(),
                accepted_identity: accepted.id.clone(),
                created: false,
            });
        }
    }
    Ok(compatible)
}

fn prompt_existing_identity_or_create(
    surface_id: &str,
    compatible: &[SelectedSurfaceIdentity],
) -> Result<Option<SelectedSurfaceIdentity>, anyhow::Error> {
    if compatible.is_empty() {
        return Ok(None);
    }
    let theme = ColorfulTheme::default();
    let mut items = compatible
        .iter()
        .map(|candidate| {
            format!(
                "Use {} ({})",
                candidate.identity.name, candidate.identity.identity_spec
            )
        })
        .collect::<Vec<_>>();
    items.push("Create new identity".to_string());
    let selected = Select::with_theme(&theme)
        .with_prompt(format!("{surface_id} identity"))
        .items(&items)
        .default(0)
        .interact()?;
    if selected == compatible.len() {
        Ok(None)
    } else {
        Ok(compatible.get(selected).cloned())
    }
}

async fn create_identity_for_surface(
    app: &AppClient,
    source_name: &str,
    requirement: &SurfaceIdentityRequirement,
    reserved_identity_names: &mut BTreeSet<String>,
    identity_specs: &mut BTreeMap<String, IdentityManifest>,
) -> Result<SelectedSurfaceIdentity, anyhow::Error> {
    let options = identity_creation_options(app, requirement, identity_specs).await?;
    if options.is_empty() {
        return Err(anyhow::anyhow!(
            "source surface '{}' has no installed identity spec that can be created",
            requirement.surface_id
        ));
    }
    let selected = select_identity_creation_option(&requirement.surface_id, options)?;
    let identity_name = generated_source_identity_name(
        source_name,
        &requirement.surface_id,
        &selected.identity_spec.name,
        reserved_identity_names,
    );
    let identity = match selected.method {
        IdentityCreationMethod::OAuth { label } => {
            let method = identity_oauth_method(&selected.identity_spec)?;
            print_oauth_hint(method.hint);
            let credential_inputs =
                prompt_identity_oauth_inputs(&selected.identity_spec, method.oauth)?;
            create_user_owned_identity_with_oauth(
                app,
                &identity_name,
                &selected.identity_spec.name,
                credential_inputs,
                &label,
                "coral source add --interactive --file <manifest.yaml>",
            )
            .await?
        }
        IdentityCreationMethod::FixedToken => {
            let token = prompt_fixed_token_identity_token(&selected.identity_spec.name)?;
            create_user_owned_identity_with_fixed_token(
                app,
                &identity_name,
                &selected.identity_spec.name,
                token,
            )
            .await?
        }
    };
    Ok(SelectedSurfaceIdentity {
        identity,
        accepted_identity: selected.accepted_identity,
        created: true,
    })
}

async fn identity_creation_options(
    app: &AppClient,
    requirement: &SurfaceIdentityRequirement,
    identity_specs: &mut BTreeMap<String, IdentityManifest>,
) -> Result<Vec<IdentityCreationOption>, anyhow::Error> {
    let mut options = Vec::new();
    for accepted in &requirement.accepts {
        for identity_spec_name in &accepted.identity_specs {
            let Some(identity_spec) =
                identity_spec_manifest_cached(app, identity_specs, identity_spec_name).await?
            else {
                continue;
            };
            if !audience_matches(&accepted.audience, &identity_spec.audience) {
                continue;
            }
            let method = match identity_spec.identity_type {
                IdentitySpecType::OAuth => {
                    let method = identity_oauth_method(&identity_spec)?;
                    IdentityCreationMethod::OAuth {
                        label: method.label.clone(),
                    }
                }
                IdentitySpecType::FixedToken => IdentityCreationMethod::FixedToken,
            };
            options.push(IdentityCreationOption {
                accepted_identity: accepted.id.clone(),
                identity_spec,
                method,
            });
        }
    }
    Ok(options)
}

fn select_identity_creation_option(
    surface_id: &str,
    options: Vec<IdentityCreationOption>,
) -> Result<IdentityCreationOption, anyhow::Error> {
    if options.len() == 1 {
        return options
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing identity creation option"));
    }
    let theme = ColorfulTheme::default();
    let items = options
        .iter()
        .map(|option| {
            format!(
                "{} ({}, {})",
                option.identity_spec.name,
                option.method.label(),
                option.identity_spec.issuer
            )
        })
        .collect::<Vec<_>>();
    let selected = Select::with_theme(&theme)
        .with_prompt(format!("Create identity for {surface_id}"))
        .items(&items)
        .default(0)
        .interact()?;
    options
        .into_iter()
        .nth(selected)
        .ok_or_else(|| anyhow::anyhow!("identity selection {selected} is out of range"))
}

pub(crate) fn prompt_fixed_token_identity_token(
    identity_spec_name: &str,
) -> Result<String, anyhow::Error> {
    let token = Password::with_theme(&ColorfulTheme::default())
        .with_prompt(format!("Token for {identity_spec_name}"))
        .allow_empty_password(false)
        .interact()?;
    Ok(token)
}

pub(crate) fn read_fixed_token_identity_token_from_stdin() -> Result<String, anyhow::Error> {
    let mut token = String::new();
    stdin().read_to_string(&mut token)?;
    let token = token.trim().to_string();
    if token.is_empty() {
        return Err(anyhow::anyhow!(
            "fixed token identity token must not be empty"
        ));
    }
    Ok(token)
}

async fn identity_spec_manifest_cached(
    app: &AppClient,
    identity_specs: &mut BTreeMap<String, IdentityManifest>,
    name: &str,
) -> Result<Option<IdentityManifest>, anyhow::Error> {
    if let Some(manifest) = identity_specs.get(name) {
        return Ok(Some(manifest.clone()));
    }
    let record = match get_identity_spec(app, name).await {
        Ok(record) => record,
        Err(error)
            if error
                .downcast_ref::<tonic::Status>()
                .is_some_and(|status| status.code() == tonic::Code::NotFound) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let manifest = parse_identity_manifest_yaml(&record.manifest_yaml)?;
    identity_specs.insert(name.to_string(), manifest.clone());
    Ok(Some(manifest))
}

fn audience_matches(
    required: &BTreeMap<String, serde_json::Value>,
    candidate: &BTreeMap<String, serde_json::Value>,
) -> bool {
    required
        .iter()
        .all(|(key, value)| candidate.get(key) == Some(value))
}

fn generated_source_identity_name(
    source_name: &str,
    surface_id: &str,
    identity_spec_name: &str,
    reserved_names: &mut BTreeSet<String>,
) -> String {
    let base = format!("{source_name}_{surface_id}_{identity_spec_name}");
    loop {
        let candidate = format!("{base}_{}", uuid::Uuid::new_v4().simple());
        if reserved_names.insert(candidate.clone()) {
            return candidate;
        }
    }
}

struct OAuthStreamContext {
    ended_message: &'static str,
    error_action: &'static str,
    retry_command: &'static str,
}

/// Drives one OAuth-progress response stream to completion, rendering
/// authorization prompts along the way, and returns the completion payload.
async fn completion_from_oauth_stream<M, T>(
    mut stream: tonic::Streaming<M>,
    oauth_labels: &BTreeMap<String, String>,
    context: OAuthStreamContext,
    event_from: impl Fn(M) -> Option<CredentialStreamEvent<T>>,
) -> Result<T, anyhow::Error> {
    let mut redirect_prompt = OAuthRedirectPastePrompt::default();
    loop {
        let response = match stream.message().await {
            Ok(Some(response)) => response,
            Ok(None) => {
                redirect_prompt.cancel_and_join();
                return Err(anyhow::anyhow!(context.ended_message));
            }
            Err(error) => {
                redirect_prompt.cancel_and_join();
                return Err(oauth_error(
                    context.error_action,
                    &error,
                    context.retry_command,
                ));
            }
        };
        if let Some(completed) =
            handle_credential_stream_event(event_from(response), oauth_labels, &mut redirect_prompt)
        {
            redirect_prompt.cancel_and_join();
            return Ok(completed);
        }
    }
}

enum CredentialStreamEvent<T> {
    Completed(T),
    OAuthAuthorization {
        input_key: String,
        authorization_url: String,
        user_code: String,
    },
    OAuthCompleted,
}

impl From<create_bundled_source_with_o_auth_response::Event> for CredentialStreamEvent<Source> {
    fn from(event: create_bundled_source_with_o_auth_response::Event) -> Self {
        match event {
            create_bundled_source_with_o_auth_response::Event::Source(source) => {
                Self::Completed(source)
            }
            create_bundled_source_with_o_auth_response::Event::OauthAuthorization(
                authorization,
            ) => Self::OAuthAuthorization {
                input_key: authorization.input_key,
                authorization_url: authorization.authorization_url,
                user_code: authorization.user_code,
            },
            create_bundled_source_with_o_auth_response::Event::OauthCompleted(_) => {
                Self::OAuthCompleted
            }
        }
    }
}

impl From<import_source_response::Event> for CredentialStreamEvent<Source> {
    fn from(event: import_source_response::Event) -> Self {
        match event {
            import_source_response::Event::Source(source) => Self::Completed(source),
            import_source_response::Event::OauthAuthorization(authorization) => {
                Self::OAuthAuthorization {
                    input_key: authorization.input_key,
                    authorization_url: authorization.authorization_url,
                    user_code: authorization.user_code,
                }
            }
            import_source_response::Event::OauthCompleted(_) => Self::OAuthCompleted,
        }
    }
}

impl From<create_user_owned_identity_with_o_auth_response::Event>
    for CredentialStreamEvent<Identity>
{
    fn from(event: create_user_owned_identity_with_o_auth_response::Event) -> Self {
        match event {
            create_user_owned_identity_with_o_auth_response::Event::Identity(identity) => {
                Self::Completed(identity)
            }
            create_user_owned_identity_with_o_auth_response::Event::OauthAuthorization(
                authorization,
            ) => Self::OAuthAuthorization {
                input_key: authorization.input_key,
                authorization_url: authorization.authorization_url,
                user_code: authorization.user_code,
            },
            create_user_owned_identity_with_o_auth_response::Event::OauthCompleted(_) => {
                Self::OAuthCompleted
            }
        }
    }
}

fn handle_credential_stream_event<T>(
    event: Option<CredentialStreamEvent<T>>,
    oauth_labels: &BTreeMap<String, String>,
    redirect_prompt: &mut OAuthRedirectPastePrompt,
) -> Option<T> {
    match event {
        Some(CredentialStreamEvent::OAuthAuthorization {
            input_key,
            authorization_url,
            user_code,
        }) => {
            handle_oauth_authorization_event(
                &input_key,
                &authorization_url,
                &user_code,
                oauth_labels,
                redirect_prompt,
            );
            None
        }
        Some(CredentialStreamEvent::Completed(completed)) => {
            redirect_prompt.cancel_and_join();
            Some(completed)
        }
        Some(CredentialStreamEvent::OAuthCompleted) => {
            redirect_prompt.cancel_and_join();
            None
        }
        None => None,
    }
}

fn handle_oauth_authorization_event(
    input_key: &str,
    authorization_url: &str,
    user_code: &str,
    oauth_labels: &BTreeMap<String, String>,
    redirect_prompt: &mut OAuthRedirectPastePrompt,
) {
    let label = oauth_label_for_input_key(input_key, oauth_labels);
    println!("Open this URL to connect {label}:");
    println!("{authorization_url}");
    redirect_prompt.cancel_and_join();
    if user_code.is_empty() {
        redirect_prompt.replace(spawn_oauth_redirect_paste_prompt(authorization_url, label));
    } else {
        println!("Enter this code when prompted: {user_code}");
    }
    if let Err(err) = crate::browser::open_url(authorization_url) {
        println!("{}", style(format!("Could not open browser: {err}")).dim());
    }
}

fn oauth_label_for_input_key<'a>(
    input_key: &'a str,
    oauth_labels: &'a BTreeMap<String, String>,
) -> &'a str {
    oauth_labels
        .get(input_key)
        .map_or(input_key, String::as_str)
}

pub(crate) async fn validate_source(
    app: &AppClient,
    name: &str,
) -> Result<ValidateSourceResponse, anyhow::Error> {
    Ok(validate_source_request(app, source_name_arg(Some(name))?).await?)
}

async fn validate_source_request(
    app: &AppClient,
    name: String,
) -> Result<ValidateSourceResponse, tonic::Status> {
    Ok(app
        .source_client()
        .validate_source(Request::new(ValidateSourceRequest {
            workspace: Some(default_workspace()),
            name,
        }))
        .await?
        .into_inner())
}

pub(crate) fn load_validated_manifest_file(
    file: &Path,
) -> Result<(String, ValidatedSourceManifest), anyhow::Error> {
    let bundle = load_validated_manifest_bundle_file(file)?;
    Ok((bundle.source_manifest_yaml, bundle.source_manifest))
}

pub(crate) fn load_validated_identity_spec_file(
    file: &Path,
) -> Result<(String, IdentityManifest), anyhow::Error> {
    let raw = std::fs::read_to_string(file)?;
    let manifest = parse_identity_manifest_yaml(&raw)?;
    Ok((raw, manifest))
}

pub(crate) struct ValidatedManifestBundleFile {
    pub(crate) source_manifest_yaml: String,
    pub(crate) source_manifest: ValidatedSourceManifest,
    pub(crate) identity_manifests: Vec<IdentityManifestDocument>,
}

pub(crate) fn load_validated_manifest_bundle_file(
    file: &Path,
) -> Result<ValidatedManifestBundleFile, anyhow::Error> {
    let raw = std::fs::read_to_string(file)?;
    load_validated_manifest_bundle_file_yaml(file, raw.as_str())
}

pub(crate) fn load_validated_manifest_bundle_file_yaml(
    file: &Path,
    manifest_yaml: &str,
) -> Result<ValidatedManifestBundleFile, anyhow::Error> {
    let bundle = parse_manifest_bundle_yaml(manifest_yaml)?;
    let manifest_dir = manifest_file_parent_dir(file)?;
    let source_manifest_yaml = durable_manifest_file_yaml(
        &bundle.source_manifest_yaml,
        &bundle.source_manifest,
        manifest_dir.as_path(),
    )?;
    let source_manifest = parse_source_manifest_yaml(source_manifest_yaml.as_str())?;
    Ok(ValidatedManifestBundleFile {
        source_manifest_yaml,
        source_manifest,
        identity_manifests: bundle.identity_manifests,
    })
}

fn manifest_file_parent_dir(file: &Path) -> Result<PathBuf, anyhow::Error> {
    let parent = file
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    parent.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize manifest directory '{}'",
            parent.display()
        )
    })
}

fn durable_manifest_file_yaml(
    manifest_yaml: &str,
    manifest: &ValidatedSourceManifest,
    manifest_dir: &Path,
) -> Result<String, anyhow::Error> {
    let Some(v4) = manifest.as_v4() else {
        return Ok(manifest_yaml.to_string());
    };
    let mut replacement_files = BTreeMap::new();
    for surface in &v4.surfaces {
        let SurfaceDescriptor::File { file, .. } = &surface.descriptor else {
            continue;
        };
        let canonical = canonicalize_manifest_descriptor(file, manifest_dir)?;
        if canonical != *file {
            replacement_files.insert(surface.id.as_str(), canonical);
        }
    }
    if replacement_files.is_empty() {
        return Ok(manifest_yaml.to_string());
    }

    let mut value: YamlValue = serde_yaml::from_str(manifest_yaml)?;
    let surfaces_key = YamlValue::String("surfaces".to_string());
    let id_key = YamlValue::String("id".to_string());
    let file_key = YamlValue::String("file".to_string());
    let surfaces = value
        .as_mapping_mut()
        .and_then(|mapping| mapping.get_mut(&surfaces_key))
        .and_then(YamlValue::as_sequence_mut)
        .ok_or_else(|| anyhow::anyhow!("DSL v4 manifest is missing surfaces"))?;
    for surface in surfaces {
        let Some(mapping) = surface.as_mapping_mut() else {
            continue;
        };
        let Some(surface_id) = mapping.get(&id_key).and_then(YamlValue::as_str) else {
            continue;
        };
        let Some(file) = replacement_files.get(surface_id) else {
            continue;
        };
        mapping.insert(
            file_key.clone(),
            YamlValue::String(file.display().to_string()),
        );
    }
    serde_yaml::to_string(&value).map_err(Into::into)
}

fn canonicalize_manifest_descriptor(
    file: &Path,
    manifest_dir: &Path,
) -> Result<PathBuf, anyhow::Error> {
    let (candidate, relative_base) = if file.is_absolute() {
        (file.to_path_buf(), None)
    } else {
        (manifest_dir.join(file), Some(manifest_dir))
    };
    let metadata = std::fs::symlink_metadata(&candidate).with_context(|| {
        format!(
            "failed to inspect OpenAPI descriptor '{}' resolved from manifest directory '{}'",
            file.display(),
            manifest_dir.display()
        )
    })?;
    if metadata.file_type().is_symlink() {
        bail!(
            "OpenAPI descriptor '{}' must not be a symlink",
            file.display()
        );
    }
    if !metadata.file_type().is_file() {
        bail!(
            "OpenAPI descriptor '{}' must be a regular file",
            file.display()
        );
    }
    let canonical = candidate.canonicalize().with_context(|| {
        format!(
            "failed to canonicalize OpenAPI descriptor '{}' resolved from manifest directory '{}'",
            file.display(),
            manifest_dir.display()
        )
    })?;
    if let Some(base) = relative_base
        && !canonical.starts_with(base)
    {
        bail!(
            "relative OpenAPI descriptor '{}' resolves outside manifest directory '{}'",
            file.display(),
            base.display()
        );
    }
    Ok(canonical)
}

pub(crate) async fn print_source_info(
    app: &AppClient,
    name: &str,
    verbose: bool,
) -> Result<(), anyhow::Error> {
    let response = app
        .source_client()
        .get_source_info(Request::new(GetSourceInfoRequest {
            workspace: Some(default_workspace()),
            name: source_name_arg(Some(name))?,
        }))
        .await?
        .into_inner();
    let source = response
        .source_info
        .ok_or_else(|| anyhow::anyhow!("get source info response missing source_info"))?;
    print_source_info_response(&source, verbose);
    Ok(())
}

fn print_source_info_response(source: &SourceInfo, verbose: bool) {
    let status = if source.installed {
        style("installed").green().to_string()
    } else {
        style("not installed").dim().to_string()
    };

    println!("{}", style(&source.name).bold());
    println!("  Status:      {status}");
    println!("  Origin:      {}", source_origin_label(source.origin));
    if source.installed {
        println!(
            "  Secrets:     {}",
            source_credential_storage_label(source.credential_storage)
        );
    }
    if !source.version.is_empty() {
        println!("  Version:     {}", source.version);
    }
    if !source.description.is_empty() {
        println!("  Description: {}", source.description);
    }

    if source.inputs.is_empty() {
        return;
    }

    println!();
    println!("  {}", style("Inputs").bold());
    for input in &source.inputs {
        let (kind_label, default_value) = match input.input.as_ref() {
            Some(ProtoSourceInput::Variable(variable)) => {
                ("variable", variable.default_value.as_str())
            }
            Some(ProtoSourceInput::Secret(_)) => ("secret", ""),
            None => ("unknown", ""),
        };
        let requirement = if input.required {
            "required"
        } else {
            "optional"
        };
        println!(
            "    {} {}",
            style(&input.key).bold(),
            style(format!("({kind_label}, {requirement})")).dim()
        );
        if !default_value.is_empty() {
            println!("      default: {default_value}");
        }
        if verbose && !input.hint.is_empty() {
            println!("      {}", style(&input.hint).dim());
        }
    }
}

pub(crate) fn display_version(version: &str) -> String {
    if version.is_empty() {
        "-".to_string()
    } else {
        version.to_string()
    }
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
    require_interactive_for("interactive source install")
}

pub(crate) fn require_interactive_for(action: &str) -> Result<(), anyhow::Error> {
    if !stdin().is_terminal() || !stdout().is_terminal() {
        return Err(anyhow::anyhow!("{action} requires a TTY"));
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
    if name == "." || name == ".." {
        return Err(anyhow::anyhow!("source name must not be '.' or '..'"));
    }
    Ok(name.to_string())
}

pub(crate) fn prompt_for_inputs_with_credential_methods(
    inputs: &[ManifestInputSpec],
) -> Result<CollectedSourceInputs, anyhow::Error> {
    prompt_for_inputs_with_credential_methods_in_mode(inputs, CredentialPromptMode::EnvFirst)
}

pub(crate) fn prompt_for_inputs_with_credential_methods_in_mode(
    inputs: &[ManifestInputSpec],
    mode: CredentialPromptMode,
) -> Result<CollectedSourceInputs, anyhow::Error> {
    let mut collected = CollectedSourceInputs::new();

    for input in inputs {
        if mode.reads_env_before_prompt(input) {
            let env_value = read_source_input_env(&input.key).unwrap_or_default();
            if !env_value.is_empty() {
                push_collected_input(&mut collected, input, env_value);
                continue;
            }
        }

        match input.kind {
            ManifestInputKind::Variable => {
                if let Some(variable) = prompt_variable(input)? {
                    collected.variables.push(variable);
                }
            }
            ManifestInputKind::Secret => match prompt_secret_with_methods(
                input,
                !collected.secrets.is_empty() || !collected.oauth_credential_retrievals.is_empty(),
            )? {
                SecretInputOutcome::SourceConfig(secret) => {
                    if let Some(secret) = secret {
                        collected.secrets.push(secret);
                    }
                }
                SecretInputOutcome::OAuth { credential, label } => {
                    collected.oauth_labels.insert(input.key.clone(), label);
                    collected.oauth_credential_retrievals.push(credential);
                }
            },
        }
    }

    Ok(collected)
}

fn push_collected_input(
    collected: &mut CollectedSourceInputs,
    input: &ManifestInputSpec,
    value: String,
) {
    match input.kind {
        ManifestInputKind::Variable => collected.variables.push(SourceVariable {
            key: input.key.clone(),
            value,
        }),
        ManifestInputKind::Secret => collected.secrets.push(SourceSecret {
            key: input.key.clone(),
            value,
        }),
    }
}

pub(crate) fn collect_inputs_from_env(
    inputs: &[ManifestInputSpec],
    interactive_command: String,
) -> Result<(Vec<SourceVariable>, Vec<SourceSecret>), anyhow::Error> {
    collect_inputs_with_hint(
        inputs,
        |key| read_source_input_env(key).unwrap_or_default(),
        Some(interactive_command),
    )
}

pub(crate) fn identity_spec_inputs_for_add(
    manifest: &IdentityManifest,
    interactive: bool,
    interactive_command: String,
) -> Result<Vec<IdentitySpecInput>, anyhow::Error> {
    if manifest.inputs.is_empty() {
        return Ok(Vec::new());
    }
    if interactive {
        return prompt_identity_spec_inputs(manifest);
    }
    collect_identity_spec_inputs_from_env(manifest, interactive_command)
}

pub(crate) fn collect_identity_spec_inputs_from_env(
    manifest: &IdentityManifest,
    interactive_command: String,
) -> Result<Vec<IdentitySpecInput>, anyhow::Error> {
    let mut values = Vec::new();
    let mut missing = Vec::new();

    for input in &manifest.inputs {
        let value = read_identity_spec_input_env(&input.key).filter(|value| !value.is_empty());
        if let Some(value) = value {
            values.push(IdentitySpecInput {
                key: input.key.clone(),
                value,
            });
            continue;
        }
        let default_resolves_in_manifest =
            input.kind == ManifestInputKind::Variable && !input.default_value.is_empty();
        if input.required && !default_resolves_in_manifest {
            missing.push(input.key.clone());
        }
    }

    if !missing.is_empty() {
        return Err(missing_environment_variables_error(
            &missing,
            Some(interactive_command),
        ));
    }

    Ok(values)
}

pub(crate) fn prompt_identity_spec_inputs(
    manifest: &IdentityManifest,
) -> Result<Vec<IdentitySpecInput>, anyhow::Error> {
    let mut values = Vec::new();
    for input in &manifest.inputs {
        match input.kind {
            ManifestInputKind::Variable => {
                if let Some(variable) = prompt_variable(input)? {
                    values.push(IdentitySpecInput {
                        key: variable.key,
                        value: variable.value,
                    });
                }
            }
            ManifestInputKind::Secret => {
                if let Some(secret) = prompt_source_config_secret(input, None)? {
                    values.push(IdentitySpecInput {
                        key: secret.key,
                        value: secret.value,
                    });
                }
            }
        }
    }
    Ok(values)
}

pub(crate) fn shell_quote_arg(value: &str) -> String {
    if value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-' | ':' | '='))
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[expect(
    clippy::disallowed_methods,
    reason = "`coral source add` reads install-time source inputs from matching environment variables."
)]
fn read_source_input_env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

#[expect(
    clippy::disallowed_methods,
    reason = "Identity spec setup reads install-time inputs from matching environment variables."
)]
fn read_identity_spec_input_env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn collect_inputs_with_hint(
    inputs: &[ManifestInputSpec],
    mut lookup: impl FnMut(&str) -> String,
    interactive_command: Option<String>,
) -> Result<(Vec<SourceVariable>, Vec<SourceSecret>), anyhow::Error> {
    let mut variables = Vec::new();
    let mut secrets = Vec::new();
    let mut missing = Vec::new();

    for input in inputs {
        let raw = lookup(&input.key);
        let value = if raw.is_empty() {
            input.default_value.clone()
        } else {
            raw
        };
        if value.is_empty() {
            if input.required {
                missing.push(input.key.clone());
            }
            continue;
        }
        match input.kind {
            ManifestInputKind::Variable => variables.push(SourceVariable {
                key: input.key.clone(),
                value,
            }),
            ManifestInputKind::Secret => secrets.push(SourceSecret {
                key: input.key.clone(),
                value,
            }),
        }
    }

    if !missing.is_empty() {
        return Err(missing_environment_variables_error(
            &missing,
            interactive_command,
        ));
    }

    Ok((variables, secrets))
}

fn missing_environment_variables_error(
    missing: &[String],
    interactive_command: Option<String>,
) -> anyhow::Error {
    let interactive_hint = interactive_command.map_or_else(
        || "--interactive".to_string(),
        |command| format!("`{command}`"),
    );
    anyhow::anyhow!(
        "missing required environment variable{}: {}. Set the variable{} or run {interactive_hint}.",
        if missing.len() == 1 { "" } else { "s" },
        missing.join(", "),
        if missing.len() == 1 { "" } else { "s" },
    )
}

pub(crate) fn source_origin_label(origin: i32) -> &'static str {
    match SourceOrigin::try_from(origin) {
        Ok(SourceOrigin::Bundled) => "bundled",
        Ok(SourceOrigin::Imported) => "imported",
        Ok(SourceOrigin::Unspecified) | Err(_) => "unknown",
    }
}

pub(crate) fn source_credential_storage_label(storage: i32) -> &'static str {
    match SourceCredentialStorage::try_from(storage) {
        Ok(SourceCredentialStorage::Unspecified) => "none",
        Ok(SourceCredentialStorage::File) => "file (plaintext)",
        Ok(SourceCredentialStorage::Keychain) => "keychain",
        Err(_) => "unknown",
    }
}

pub(crate) fn identity_owner_label(owner: i32) -> &'static str {
    match IdentityOwner::try_from(owner) {
        Ok(IdentityOwner::User) => "user",
        Ok(IdentityOwner::Unspecified) | Err(_) => "unknown",
    }
}

/// Secret-storage label for a `coral source add` summary, or `None` when the
/// source stores no secrets.
///
/// DSL v4 sources never store secrets — they authenticate through identities —
/// so this returns `None` for them, as well as for any source whose inputs
/// declared no secrets. Callers omit the secret summary entirely in that case.
pub(crate) fn added_source_secret_storage_label(storage: i32) -> Option<&'static str> {
    match SourceCredentialStorage::try_from(storage) {
        Ok(SourceCredentialStorage::File | SourceCredentialStorage::Keychain) => {
            Some(source_credential_storage_label(storage))
        }
        _ => None,
    }
}

pub(crate) struct SelectedIdentityOAuthMethod<'a> {
    pub(crate) label: String,
    pub(crate) hint: Option<&'a str>,
    pub(crate) oauth: &'a ManifestOAuthCredentialSpec,
}

pub(crate) fn identity_oauth_method(
    manifest: &IdentityManifest,
) -> Result<SelectedIdentityOAuthMethod<'_>, anyhow::Error> {
    let IdentitySpecConfig::OAuth(oauth) = &manifest.config else {
        return Err(anyhow::anyhow!(
            "identity spec '{}' has type '{}'; expected oauth",
            manifest.name,
            manifest.identity_type.label()
        ));
    };
    let method = &oauth.method;
    Ok(SelectedIdentityOAuthMethod {
        label: identity_oauth_method_label(method),
        hint: method.hint.as_deref(),
        oauth: &method.oauth,
    })
}

pub(crate) fn print_oauth_hint(hint: Option<&str>) {
    print_prompt_hint(hint);
}

pub(crate) async fn validate_and_print(
    app: &AppClient,
    source_name: &str,
    limit: TableDisplayLimit,
) -> Result<(), anyhow::Error> {
    let response = validate_source(app, source_name).await?;
    print_validation_pretty(&response, limit)?;
    match validation_follow_up(&response, ValidationSeverityMode::WarnOnly) {
        ValidationFollowUp::None => Ok(()),
        ValidationFollowUp::Warn(message) => {
            eprintln!("Warning: {message}");
            Ok(())
        }
        ValidationFollowUp::Fail(message) => Err(anyhow::anyhow!(message)),
    }
}

pub(crate) async fn validate_and_warn(
    app: &AppClient,
    source_name: &str,
    limit: TableDisplayLimit,
) -> Result<(), anyhow::Error> {
    if let Err(err) = validate_and_print(app, source_name, limit).await {
        eprintln!("Warning: validation failed: {err}");
    }
    Ok(())
}

pub(crate) async fn test_and_print(
    app: &AppClient,
    source_name: &str,
    limit: TableDisplayLimit,
    severity_mode: ValidationSeverityMode,
) -> Result<(), crate::CliError> {
    let normalized = source_name_arg(Some(source_name))?;
    let response = match validate_source_request(app, normalized.clone()).await {
        Ok(response) => response,
        Err(status) if is_source_missing_status(&status) => {
            return source_test_not_found_error(app, &normalized, status).await;
        }
        Err(status) => return Err(anyhow::Error::from(status).into()),
    };

    print_validation_pretty(&response, limit)?;
    match validation_follow_up(&response, severity_mode) {
        ValidationFollowUp::None => Ok(()),
        ValidationFollowUp::Warn(message) => {
            eprintln!("Warning: {message}");
            Ok(())
        }
        ValidationFollowUp::Fail(message) => Err(anyhow::anyhow!(message).into()),
    }
}

async fn source_test_not_found_error(
    app: &AppClient,
    source_name: &str,
    original_status: tonic::Status,
) -> Result<(), crate::CliError> {
    // Discovery failure must not mask the original validation error.
    let Ok(available) = discover_sources(app).await else {
        return Err(anyhow::Error::from(original_status).into());
    };
    if available
        .iter()
        .any(|source| source.name == source_name && !source.installed)
    {
        return Err(crate::CliError::SourceNotInstalled {
            source_name: source_name.to_string(),
        });
    }

    Err(crate::CliError::SourceNotFound {
        source_name: source_name.to_string(),
    })
}

pub(crate) async fn remove_and_print(
    app: &AppClient,
    source_name: &str,
) -> Result<(), crate::CliError> {
    let normalized = source_name_arg(Some(source_name))?;
    match delete_source(app, &normalized).await {
        Ok(()) => {
            println!("Removed source {normalized}");
            Ok(())
        }
        Err(err) => {
            if err
                .downcast_ref::<tonic::Status>()
                .is_some_and(is_source_missing_status)
            {
                Err(crate::CliError::SourceRemoveNotFound {
                    source_name: normalized,
                })
            } else {
                Err(err.into())
            }
        }
    }
}

/// Returns `true` only when the gRPC status carries the server's
/// `SOURCE_NOT_FOUND` AIP-193 reason. Other `Code::NotFound` causes
/// (e.g. a missing manifest file mapped from `io::ErrorKind::NotFound`)
/// have no Coral `ErrorInfo` attached, so they remain diagnosable instead
/// of being rewritten into the friendly "source not found" message.
fn is_source_missing_status(status: &tonic::Status) -> bool {
    match decode_status_error(status) {
        DecodedStatusError::Structured(error) => {
            error.reason == CORAL_ERROR_REASON_SOURCE_NOT_FOUND
        }
        DecodedStatusError::Plain(_) => false,
    }
}

pub(crate) fn print_validation_pretty(
    response: &ValidateSourceResponse,
    limit: TableDisplayLimit,
) -> Result<(), anyhow::Error> {
    let source = response
        .source
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("validate response missing source metadata"))?;

    println!();
    println!(
        "  {} {}",
        style("✓").green(),
        style(format!("{} connected successfully", source.name)).bold()
    );
    println!(
        "  Secrets: {}",
        source_credential_storage_label(source.credential_storage)
    );

    // Group tables by schema, sorted.
    let mut by_schema: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for table in &response.tables {
        by_schema
            .entry(&table.schema_name)
            .or_default()
            .push(&table.name);
    }
    for tables in by_schema.values_mut() {
        tables.sort_unstable();
    }

    for (schema, tables) in &by_schema {
        let count = tables.len();
        println!();
        println!(
            "    {}",
            style(format!(
                "{schema} ({count} {})",
                if count == 1 { "table" } else { "tables" }
            ))
            .bold()
        );

        let show_count = match limit {
            TableDisplayLimit::All => tables.len(),
            TableDisplayLimit::Max(max) => tables.len().min(max),
        };
        let remaining = tables.len() - show_count;

        for (i, table) in tables.iter().take(show_count).enumerate() {
            let is_last = i == show_count - 1 && remaining == 0;
            let branch = if is_last { "└─" } else { "├─" };
            println!("    {} {}", style(branch).dim(), table);
        }

        if remaining > 0 {
            println!(
                "    {} {}",
                style("└─").dim(),
                style(format!("... and {remaining} more")).dim()
            );
        }
    }

    let query_test_counts = query_test_counts(response);
    if query_test_counts.declared > 0 {
        println!("    {}", style("Query tests").bold());
        println!(
            "    {}",
            style(format!(
                "{} declared · {} passed · {} failed",
                query_test_counts.declared, query_test_counts.passed, query_test_counts.failed
            ))
            .dim()
        );
        for test in &response.query_tests {
            println!();
            let status = if matches!(test.outcome, Some(query_test_result::Outcome::Success(_))) {
                style("✓").green()
            } else {
                style("✗").red()
            };
            println!("    {} {}", status, style(test.sql.trim()).bold());
            match &test.outcome {
                Some(query_test_result::Outcome::Success(QueryTestSuccess { row_count })) => {
                    println!(
                        "      {}",
                        style(format!(
                            "{row_count} row{}",
                            if *row_count == 1 { "" } else { "s" }
                        ))
                        .dim()
                    );
                }
                Some(query_test_result::Outcome::Failure(QueryTestFailure { error_message }))
                    if !error_message.is_empty() =>
                {
                    println!("      {}", style(error_message.as_str()).yellow());
                }
                Some(query_test_result::Outcome::Failure(QueryTestFailure { .. })) | None => {}
            }
        }
    }
    println!();

    Ok(())
}

fn validation_follow_up(
    response: &ValidateSourceResponse,
    severity_mode: ValidationSeverityMode,
) -> ValidationFollowUp {
    let query_test_counts = query_test_counts(response);
    if query_test_counts.declared == 0 || query_test_counts.failed == 0 {
        return ValidationFollowUp::None;
    }

    let failure_count = query_test_counts.failed.max(1);
    let message = format!(
        "{} of {} validation quer{} failed",
        failure_count,
        query_test_counts.declared.max(failure_count),
        if query_test_counts.declared == 1 {
            "y"
        } else {
            "ies"
        }
    );
    match severity_mode {
        ValidationSeverityMode::Strict => ValidationFollowUp::Fail(message),
        ValidationSeverityMode::WarnOnly => ValidationFollowUp::Warn(message),
    }
}

fn query_test_counts(response: &ValidateSourceResponse) -> QueryTestCounts {
    let declared = response.query_tests.len();
    let passed = response
        .query_tests
        .iter()
        .filter(|test| matches!(test.outcome, Some(query_test_result::Outcome::Success(_))))
        .count();
    QueryTestCounts {
        declared,
        passed,
        failed: declared.saturating_sub(passed),
    }
}

fn prompt_variable(input: &ManifestInputSpec) -> Result<Option<SourceVariable>, anyhow::Error> {
    let theme = ColorfulTheme::default();
    print_prompt_hint(resolve_prompt_hint(input, None));
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

fn prompt_secret(
    input: &ManifestInputSpec,
    method: Option<&ManifestCredentialMethod>,
) -> Result<Option<SourceSecret>, anyhow::Error> {
    let theme = ColorfulTheme::default();
    print_prompt_hint(resolve_prompt_hint(input, method));
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

fn prompt_source_config_secret(
    input: &ManifestInputSpec,
    method: Option<&ManifestCredentialMethod>,
) -> Result<Option<SourceSecret>, anyhow::Error> {
    let env_value = read_source_input_env(&input.key).unwrap_or_default();
    if !env_value.is_empty() {
        return Ok(Some(SourceSecret {
            key: input.key.clone(),
            value: env_value,
        }));
    }
    prompt_secret(input, method)
}

enum SecretInputOutcome {
    SourceConfig(Option<SourceSecret>),
    OAuth {
        credential: OAuthCredentialRetrieval,
        label: String,
    },
}

fn prompt_secret_with_methods(
    input: &ManifestInputSpec,
    prefer_skip: bool,
) -> Result<SecretInputOutcome, anyhow::Error> {
    let Some(credential) = input.credential.as_ref() else {
        return Ok(SecretInputOutcome::SourceConfig(
            prompt_source_config_secret(input, None)?,
        ));
    };
    let Some(selected) = select_credential_method(input, credential, prefer_skip)? else {
        return Ok(SecretInputOutcome::SourceConfig(None));
    };
    let method = credential
        .methods
        .get(selected)
        .ok_or_else(|| anyhow::anyhow!("credential method index {selected} is out of range"))?;
    // Inside a credential-method flow the selected method's hint is the
    // guidance shown; the input-level hint is reserved for inspection
    // surfaces and is not reprinted here.
    match method.kind {
        ManifestCredentialMethodKind::SourceConfig => Ok(SecretInputOutcome::SourceConfig(
            prompt_source_config_secret(input, Some(method))?,
        )),
        ManifestCredentialMethodKind::OAuth => {
            print_prompt_hint(resolve_prompt_hint(input, Some(method)));
            Ok(SecretInputOutcome::OAuth {
                credential: collect_oauth_credential_method(input, selected, method)?,
                label: credential_method_label(method),
            })
        }
    }
}

fn select_credential_method(
    input: &ManifestInputSpec,
    credential: &ManifestCredentialSpec,
    prefer_skip: bool,
) -> Result<Option<usize>, anyhow::Error> {
    if credential.methods.len() == 1 && input.required {
        return Ok(Some(0));
    }
    let theme = ColorfulTheme::default();
    let mut items = credential
        .methods
        .iter()
        .map(credential_method_label)
        .collect::<Vec<_>>();
    if !input.required {
        items.push("Skip".to_string());
    }
    let skip_index = items.len().saturating_sub(1);
    let selected = Select::with_theme(&theme)
        .with_prompt(format!("{} credential", input.key))
        .items(&items)
        .default(if !input.required && prefer_skip {
            skip_index
        } else {
            0
        })
        .interact()?;
    if !input.required && selected == skip_index {
        return Ok(None);
    }
    Ok(Some(selected))
}

fn credential_method_label(method: &ManifestCredentialMethod) -> String {
    method.label.clone().unwrap_or_else(|| match method.kind {
        ManifestCredentialMethodKind::SourceConfig => "Paste token".to_string(),
        ManifestCredentialMethodKind::OAuth => "Connect with OAuth".to_string(),
    })
}

fn identity_oauth_method_label(method: &coral_spec::IdentityOAuthMethodSpec) -> String {
    method
        .label
        .clone()
        .unwrap_or_else(|| "Connect with OAuth".to_string())
}

fn collect_oauth_credential_method(
    input: &ManifestInputSpec,
    method_index: usize,
    method: &ManifestCredentialMethod,
) -> Result<OAuthCredentialRetrieval, anyhow::Error> {
    let oauth = method
        .oauth
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("oauth credential method is missing OAuth config"))?;
    Ok(OAuthCredentialRetrieval {
        input_key: input.key.clone(),
        method_index: Some(u32::try_from(method_index)?),
        credential_inputs: prompt_oauth_credential_inputs(oauth)?,
    })
}

fn oauth_error(action: &str, error: &tonic::Status, retry_command: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "OAuth credential retrieval failed during {action}: {error}. Rerun `{retry_command}` to try again."
    )
}

#[derive(Default)]
struct OAuthRedirectPastePrompt {
    cancel: Option<Arc<AtomicBool>>,
    handle: Option<JoinHandle<()>>,
}

impl OAuthRedirectPastePrompt {
    fn new(cancel: Arc<AtomicBool>, handle: JoinHandle<()>) -> Self {
        Self {
            cancel: Some(cancel),
            handle: Some(handle),
        }
    }

    fn replace(&mut self, next: Option<Self>) {
        self.cancel_and_join();
        if let Some(next) = next {
            *self = next;
        }
    }

    fn cancel_and_join(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel.store(true, Ordering::Relaxed);
        }
        if let Some(handle) = self.handle.take()
            && handle.join().is_err()
        {
            eprintln!("OAuth redirect paste prompt stopped unexpectedly");
        }
    }
}

impl Drop for OAuthRedirectPastePrompt {
    fn drop(&mut self) {
        self.cancel_and_join();
    }
}

fn spawn_oauth_redirect_paste_prompt(
    authorization_url: &str,
    label: &str,
) -> Option<OAuthRedirectPastePrompt> {
    if !stdin().is_terminal() || !stdout().is_terminal() {
        return None;
    }
    let (expected_redirect_uri, expected_state) = match expected_oauth_redirect(authorization_url) {
        Ok(expected) => expected,
        Err(error) => {
            println!(
                "{}",
                style(format!("Could not enable redirect paste fallback: {error}")).dim()
            );
            return None;
        }
    };
    let label = label.to_string();
    println!(
        "{}",
        style(
            "If the browser cannot reach the localhost callback, paste the final redirect URL below."
        )
        .dim()
    );

    let cancel = Arc::new(AtomicBool::new(false));
    let worker_cancel = Arc::clone(&cancel);
    let handle = thread::spawn(move || {
        while !worker_cancel.load(Ordering::Relaxed) {
            print!("Redirect URL: ");
            if let Err(error) = stdout().flush() {
                eprintln!("Could not render OAuth redirect prompt: {error}");
                return;
            }
            match read_oauth_redirect_prompt(&worker_cancel) {
                Ok(Some(value)) if value.trim().is_empty() => {}
                Ok(Some(value)) => {
                    match submit_oauth_redirect_url(
                        value.trim(),
                        &expected_redirect_uri,
                        expected_state.as_deref(),
                    ) {
                        Ok(()) => {
                            println!("Submitted OAuth redirect for {label}.");
                            return;
                        }
                        Err(error) => eprintln!("Could not submit OAuth redirect URL: {error}"),
                    }
                }
                Ok(None) => return,
                Err(error) => {
                    eprintln!("Could not read OAuth redirect URL: {error}");
                    return;
                }
            }
        }
    });
    Some(OAuthRedirectPastePrompt::new(cancel, handle))
}

fn expected_oauth_redirect(
    authorization_url: &str,
) -> Result<(Url, Option<String>), anyhow::Error> {
    let authorization_url = Url::parse(authorization_url)?;
    let redirect_uri = authorization_url
        .query_pairs()
        .find_map(|(key, value)| (key == "redirect_uri").then(|| value.into_owned()))
        .ok_or_else(|| anyhow::anyhow!("authorization URL is missing redirect_uri"))?;
    let redirect_uri = Url::parse(&redirect_uri)?;
    validate_loopback_http_redirect(&redirect_uri)?;
    let state = authorization_url
        .query_pairs()
        .find_map(|(key, value)| (key == "state").then(|| value.into_owned()));
    Ok((redirect_uri, state))
}

fn submit_oauth_redirect_url(
    value: &str,
    expected_redirect_uri: &Url,
    expected_state: Option<&str>,
) -> Result<(), anyhow::Error> {
    let callback_url = Url::parse(value)?;
    validate_oauth_redirect_url(&callback_url, expected_redirect_uri, expected_state)?;
    let response = send_loopback_get(&callback_url)?;
    let status = response.lines().next().unwrap_or_default();
    if !http_status_is_success(status) {
        return Err(anyhow::anyhow!(
            "callback listener returned unexpected response: {status}"
        ));
    }
    Ok(())
}

fn validate_oauth_redirect_url(
    callback_url: &Url,
    expected_redirect_uri: &Url,
    expected_state: Option<&str>,
) -> Result<(), anyhow::Error> {
    validate_loopback_http_redirect(callback_url)?;
    if callback_url.host() != expected_redirect_uri.host()
        || callback_url.port_or_known_default() != expected_redirect_uri.port_or_known_default()
        || callback_url.path() != expected_redirect_uri.path()
    {
        return Err(anyhow::anyhow!(
            "redirect URL must match the OAuth redirect URI host, port, and path"
        ));
    }
    if callback_url.query().is_none() {
        return Err(anyhow::anyhow!("redirect URL is missing query parameters"));
    }
    if let Some(expected_state) = expected_state {
        let callback_state = callback_url
            .query_pairs()
            .find_map(|(key, value)| (key == "state").then(|| value.into_owned()));
        if callback_state.as_deref() != Some(expected_state) {
            return Err(anyhow::anyhow!(
                "redirect URL state does not match the active OAuth authorization"
            ));
        }
    }
    Ok(())
}

fn read_oauth_redirect_prompt(cancel: &AtomicBool) -> Result<Option<String>, anyhow::Error> {
    let _raw_mode = RawModeGuard::enable()?;
    let mut output = stdout();
    let mut value = String::new();
    while !cancel.load(Ordering::Relaxed) {
        if !event::poll(Duration::from_millis(100))? {
            continue;
        }
        if let Event::Key(key) = event::read()? {
            let previous_len = value.len();
            match apply_redirect_prompt_key(key, &mut value) {
                RedirectPromptAction::Continue => {}
                RedirectPromptAction::Submit => {
                    finish_redirect_prompt_line(&mut output)?;
                    return Ok(Some(value));
                }
                RedirectPromptAction::Cancel => {
                    finish_redirect_prompt_line(&mut output)?;
                    return Ok(None);
                }
            }
            render_redirect_prompt_key_echo(&mut output, key, previous_len, value.len())?;
        }
    }
    finish_redirect_prompt_line(&mut output)?;
    Ok(None)
}

struct RawModeGuard;

impl RawModeGuard {
    fn enable() -> Result<Self, anyhow::Error> {
        enable_raw_mode()?;
        Ok(Self)
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        if let Err(error) = disable_raw_mode() {
            eprintln!("Could not restore terminal mode: {error}");
        }
    }
}

fn finish_redirect_prompt_line(output: &mut impl Write) -> Result<(), anyhow::Error> {
    output.write_all(b"\r\n")?;
    output.flush()?;
    Ok(())
}

fn render_redirect_prompt_key_echo(
    output: &mut impl Write,
    key: KeyEvent,
    previous_len: usize,
    current_len: usize,
) -> Result<(), anyhow::Error> {
    if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
        return Ok(());
    }
    match key.code {
        KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
            let mut buf = [0; 4];
            output.write_all(ch.encode_utf8(&mut buf).as_bytes())?;
            output.flush()?;
        }
        KeyCode::Backspace if current_len < previous_len => {
            output.write_all(b"\x08 \x08")?;
            output.flush()?;
        }
        _ => {}
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RedirectPromptAction {
    Continue,
    Submit,
    Cancel,
}

fn apply_redirect_prompt_key(key: KeyEvent, value: &mut String) -> RedirectPromptAction {
    if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
        return RedirectPromptAction::Continue;
    }
    match key.code {
        KeyCode::Enter => RedirectPromptAction::Submit,
        KeyCode::Esc => RedirectPromptAction::Cancel,
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            RedirectPromptAction::Cancel
        }
        KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
            value.push(ch);
            RedirectPromptAction::Continue
        }
        KeyCode::Backspace => {
            value.pop();
            RedirectPromptAction::Continue
        }
        _ => RedirectPromptAction::Continue,
    }
}

fn validate_loopback_http_redirect(url: &Url) -> Result<(), anyhow::Error> {
    if url.scheme() != "http" {
        return Err(anyhow::anyhow!("redirect URL must use http"));
    }
    let Some(host) = url.host() else {
        return Err(anyhow::anyhow!("redirect URL is missing host"));
    };
    let is_loopback = match host {
        Host::Domain(domain) => domain.eq_ignore_ascii_case("localhost"),
        Host::Ipv4(addr) => addr.is_loopback(),
        Host::Ipv6(addr) => addr.is_loopback(),
    };
    if !is_loopback {
        return Err(anyhow::anyhow!("redirect URL host must be loopback"));
    }
    if url.port_or_known_default().is_none() {
        return Err(anyhow::anyhow!("redirect URL is missing port"));
    }
    Ok(())
}

fn send_loopback_get(url: &Url) -> Result<String, anyhow::Error> {
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("redirect URL is missing host"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| anyhow::anyhow!("redirect URL is missing port"))?;
    let mut stream = TcpStream::connect((host, port))?;
    let timeout = Some(Duration::from_secs(5));
    stream.set_read_timeout(timeout)?;
    stream.set_write_timeout(timeout)?;
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        request_target(url),
        host_header(url)?
    );
    stream.write_all(request.as_bytes())?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

fn request_target(url: &Url) -> String {
    match url.query() {
        Some(query) => format!("{}?{query}", url.path()),
        None => url.path().to_string(),
    }
}

fn host_header(url: &Url) -> Result<String, anyhow::Error> {
    let host = url
        .host()
        .ok_or_else(|| anyhow::anyhow!("redirect URL is missing host"))?;
    let mut value = match host {
        Host::Domain(domain) => domain.to_string(),
        Host::Ipv4(addr) => addr.to_string(),
        Host::Ipv6(addr) => format!("[{addr}]"),
    };
    if let Some(port) = url.port() {
        value.push(':');
        value.push_str(&port.to_string());
    }
    Ok(value)
}

fn http_status_is_success(status: &str) -> bool {
    status
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .is_some_and(|code| (200..300).contains(&code))
}

pub(crate) fn prompt_oauth_credential_inputs(
    oauth: &ManifestOAuthCredentialSpec,
) -> Result<Vec<OAuthCredentialInput>, anyhow::Error> {
    prompt_oauth_credential_inputs_excluding(oauth, &BTreeSet::new())
}

pub(crate) fn prompt_identity_oauth_inputs(
    manifest: &IdentityManifest,
    oauth: &ManifestOAuthCredentialSpec,
) -> Result<Vec<OAuthCredentialInput>, anyhow::Error> {
    let declared = manifest
        .inputs
        .iter()
        .map(|input| input.key.as_str())
        .collect::<BTreeSet<_>>();
    prompt_oauth_credential_inputs_excluding(oauth, &declared)
}

fn prompt_oauth_credential_inputs_excluding(
    oauth: &ManifestOAuthCredentialSpec,
    excluded_inputs: &BTreeSet<&str>,
) -> Result<Vec<OAuthCredentialInput>, anyhow::Error> {
    let mut values = Vec::new();
    if let Some(input_key) = oauth.client.id.input.as_deref()
        && !excluded_inputs.contains(input_key)
        && let Some(value) = prompt_oauth_client_id(input_key, oauth.client.id.default.as_deref())?
    {
        values.push(OAuthCredentialInput {
            key: input_key.to_string(),
            value,
        });
    }
    if let Some(secret) = oauth.client.secret.as_ref()
        && !excluded_inputs.contains(secret.input.as_str())
    {
        let value = prompt_oauth_client_secret(&secret.input)?;
        values.push(OAuthCredentialInput {
            key: secret.input.clone(),
            value,
        });
    }
    Ok(values)
}

fn prompt_oauth_client_id(
    input_key: &str,
    default: Option<&str>,
) -> Result<Option<String>, anyhow::Error> {
    let theme = ColorfulTheme::default();
    let prompt = if default.is_some_and(|value| !value.is_empty()) {
        format!("{input_key} [source default]")
    } else {
        input_key.to_string()
    };
    let value = Input::<String>::with_theme(&theme)
        .with_prompt(prompt)
        .allow_empty(true)
        .interact_text()?;
    if !value.is_empty() {
        return Ok(Some(value));
    }
    if default.is_some_and(|value| !value.is_empty()) {
        return Ok(None);
    }
    Err(anyhow::anyhow!(
        "missing required OAuth client ID '{input_key}'"
    ))
}

fn prompt_oauth_client_secret(input_key: &str) -> Result<String, anyhow::Error> {
    let theme = ColorfulTheme::default();
    let value = Password::with_theme(&theme)
        .with_prompt(input_key)
        .allow_empty_password(false)
        .interact()?;
    if value.is_empty() {
        return Err(anyhow::anyhow!(
            "missing required OAuth client secret '{input_key}'"
        ));
    }
    Ok(value)
}

/// Resolve the single hint to show while interactively collecting `input`.
///
/// Inside a credential-method flow (`method` is `Some`) the selected method's
/// hint takes precedence, so the input-level hint — kept concise for
/// inspection surfaces (`coral source info --verbose`, `coral.inputs`) and the
/// generated docs — is not reprinted alongside it. When the selected method
/// has no hint we fall back to the input-level hint rather than show nothing:
/// a dormant safety net for multi-method secrets that have not authored
/// per-method hints. For variables and plain secrets (`method` is `None`) the
/// input-level hint is used directly. Returning a single value makes it
/// impossible to print both the input-level and method-level hints together.
fn resolve_prompt_hint<'a>(
    input: &'a ManifestInputSpec,
    method: Option<&'a ManifestCredentialMethod>,
) -> Option<&'a str> {
    let trimmed = |hint: Option<&'a str>| hint.map(str::trim).filter(|hint| !hint.is_empty());
    trimmed(method.and_then(|method| method.hint.as_deref()))
        .or_else(|| trimmed(input.hint.as_deref()))
}

fn print_prompt_hint(hint: Option<&str>) {
    if let Some(hint) = hint {
        println!("  {}", style(hint).dim());
    }
}

pub(crate) fn finalize_input_value(
    input: &ManifestInputSpec,
    value: String,
    kind_label: &str,
) -> Result<Option<String>, anyhow::Error> {
    if !value.is_empty() {
        return Ok(Some(value));
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
    #![expect(
        clippy::indexing_slicing,
        reason = "collected input order assertions intentionally fail loudly in tests"
    )]

    use coral_api::v1::ValidateSourceResponse;
    use coral_spec::{
        ManifestCredentialMethod, ManifestCredentialMethodKind, ManifestCredentialSpec,
        ManifestInputKind, ManifestInputSpec, parse_identity_manifest_yaml,
    };
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;
    use std::thread;
    use tempfile::TempDir;
    use url::Url;

    use super::{
        CredentialPromptMode, IDENTITY_OAUTH_PROGRESS_INPUT_KEY, RedirectPromptAction,
        ValidationFollowUp, ValidationSeverityMode, apply_redirect_prompt_key, audience_matches,
        collect_inputs_with_hint, expected_oauth_redirect, finalize_input_value,
        generated_source_identity_name, identity_spec_inputs_for_add,
        identity_spec_left_installed_warning, load_validated_manifest_file, oauth_error,
        oauth_label_for_input_key, render_redirect_prompt_key_echo, resolve_prompt_hint,
        shell_quote_arg, source_name_arg, submit_oauth_redirect_url,
        user_owned_identity_left_installed_warning, validate_oauth_redirect_url,
        validation_follow_up,
    };

    #[test]
    fn added_source_secret_storage_label_omits_summary_for_secretless_sources() {
        use coral_api::v1::SourceCredentialStorage;

        assert_eq!(
            super::added_source_secret_storage_label(SourceCredentialStorage::Unspecified as i32),
            None,
            "DSL v4 and other secretless sources should not show a secret summary"
        );
        assert_eq!(
            super::added_source_secret_storage_label(SourceCredentialStorage::File as i32),
            Some("file (plaintext)")
        );
        assert_eq!(
            super::added_source_secret_storage_label(SourceCredentialStorage::Keychain as i32),
            Some("keychain")
        );
    }

    #[test]
    fn load_validated_manifest_file_resolves_v4_file_descriptors_relative_to_manifest() {
        let temp = TempDir::new().expect("temp dir");
        let source_dir = temp.path().join("statusgator_v4");
        std::fs::create_dir_all(&source_dir).expect("source dir");
        std::fs::write(source_dir.join("openapi.yaml"), "openapi: 3.0.0\n").expect("write openapi");
        let manifest_file = source_dir.join("manifest.yaml");
        std::fs::write(
            &manifest_file,
            r"
name: statusgator_v4
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: openapi.yaml
    sha256: 0000000000000000000000000000000000000000000000000000000000000000
",
        )
        .expect("write manifest");

        let (manifest_yaml, manifest) =
            load_validated_manifest_file(&manifest_file).expect("load manifest");

        assert_eq!(manifest.schema_name(), "statusgator_v4");
        let value: serde_yaml::Value =
            serde_yaml::from_str(&manifest_yaml).expect("rewritten yaml parses");
        let file = value["surfaces"][0]["file"].as_str().expect("surface file");
        assert_eq!(
            file,
            source_dir
                .join("openapi.yaml")
                .canonicalize()
                .expect("canonical descriptor")
                .to_string_lossy()
        );
    }

    #[test]
    fn collect_inputs_reads_variables_and_secrets_from_lookup() {
        let inputs = vec![
            ManifestInputSpec {
                key: "LINEAR_API_BASE".to_string(),
                kind: ManifestInputKind::Variable,
                required: false,
                default_value: "https://api.linear.app".to_string(),
                hint: None,
                credential: None,
            },
            ManifestInputSpec {
                key: "LINEAR_API_KEY".to_string(),
                kind: ManifestInputKind::Secret,
                required: true,
                default_value: String::new(),
                hint: None,
                credential: None,
            },
        ];
        let env: HashMap<&str, &str> = [("LINEAR_API_KEY", "lin_token")].into_iter().collect();
        let (variables, secrets) = collect_inputs_with_hint(
            &inputs,
            |key| env.get(key).map(|v| (*v).to_string()).unwrap_or_default(),
            None,
        )
        .expect("should succeed");
        assert_eq!(variables.len(), 1);
        assert_eq!(variables[0].key, "LINEAR_API_BASE");
        assert_eq!(variables[0].value, "https://api.linear.app");
        assert_eq!(secrets.len(), 1);
        assert_eq!(secrets[0].key, "LINEAR_API_KEY");
        assert_eq!(secrets[0].value, "lin_token");
    }

    #[test]
    fn credential_method_first_defers_env_for_secrets_with_credential_methods() {
        let input = ManifestInputSpec {
            key: "LINEAR_OAUTH_ACCESS_TOKEN".to_string(),
            kind: ManifestInputKind::Secret,
            required: false,
            default_value: String::new(),
            hint: None,
            credential: Some(ManifestCredentialSpec {
                methods: vec![ManifestCredentialMethod {
                    kind: ManifestCredentialMethodKind::SourceConfig,
                    label: Some("Paste token".to_string()),
                    description: None,
                    hint: None,
                    oauth: None,
                }],
            }),
        };

        assert!(CredentialPromptMode::EnvFirst.reads_env_before_prompt(&input));
        assert!(!CredentialPromptMode::CredentialMethodFirst.reads_env_before_prompt(&input));
    }

    fn secret_with_method(
        input_hint: Option<&str>,
        method_hint: Option<&str>,
    ) -> (ManifestInputSpec, ManifestCredentialMethod) {
        let method = ManifestCredentialMethod {
            kind: ManifestCredentialMethodKind::SourceConfig,
            label: Some("Paste token".to_string()),
            description: None,
            hint: method_hint.map(ToString::to_string),
            oauth: None,
        };
        let input = ManifestInputSpec {
            key: "GITHUB_TOKEN".to_string(),
            kind: ManifestInputKind::Secret,
            required: true,
            default_value: String::new(),
            hint: input_hint.map(ToString::to_string),
            credential: Some(ManifestCredentialSpec {
                methods: vec![method.clone()],
            }),
        };
        (input, method)
    }

    #[test]
    fn prompt_hint_uses_input_hint_outside_a_credential_method_flow() {
        let (input, _) = secret_with_method(Some("Input-level summary."), Some("Method guidance."));
        assert_eq!(
            resolve_prompt_hint(&input, None),
            Some("Input-level summary.")
        );
    }

    #[test]
    fn prompt_hint_uses_only_the_method_hint_inside_a_credential_method_flow() {
        // Once a method is selected, the method hint is the guidance and the
        // input-level hint is never reprinted (the source_config/"Paste token"
        // path must not show both).
        let (input, method) =
            secret_with_method(Some("Input-level summary."), Some("Method guidance."));
        assert_eq!(
            resolve_prompt_hint(&input, Some(&method)),
            Some("Method guidance.")
        );
    }

    #[test]
    fn prompt_hint_falls_back_to_input_hint_when_method_has_no_hint() {
        // Dormant safety net: a multi-method secret whose selected method has
        // no hint still shows the input-level hint rather than nothing.
        let (input, method) = secret_with_method(Some("Input-level summary."), None);
        assert_eq!(
            resolve_prompt_hint(&input, Some(&method)),
            Some("Input-level summary.")
        );
    }

    #[test]
    fn prompt_hint_shows_nothing_when_neither_method_nor_input_has_a_hint() {
        let (input, method) = secret_with_method(None, None);
        assert_eq!(resolve_prompt_hint(&input, Some(&method)), None);
    }

    #[test]
    fn prompt_hint_trims_and_drops_blank_hints() {
        let (input, method) = secret_with_method(Some("   "), Some("  Method guidance.  "));
        assert_eq!(resolve_prompt_hint(&input, None), None);
        assert_eq!(
            resolve_prompt_hint(&input, Some(&method)),
            Some("Method guidance.")
        );
    }

    #[test]
    fn credential_method_first_keeps_env_for_plain_inputs() {
        let variable = ManifestInputSpec {
            key: "LINEAR_API_BASE".to_string(),
            kind: ManifestInputKind::Variable,
            required: false,
            default_value: String::new(),
            hint: None,
            credential: None,
        };
        let plain_secret = ManifestInputSpec {
            key: "LINEAR_API_KEY".to_string(),
            kind: ManifestInputKind::Secret,
            required: false,
            default_value: String::new(),
            hint: None,
            credential: None,
        };

        assert!(CredentialPromptMode::CredentialMethodFirst.reads_env_before_prompt(&variable));
        assert!(CredentialPromptMode::CredentialMethodFirst.reads_env_before_prompt(&plain_secret));
    }

    #[test]
    fn collect_inputs_env_value_overrides_default() {
        let inputs = vec![ManifestInputSpec {
            key: "API_BASE".to_string(),
            kind: ManifestInputKind::Variable,
            required: false,
            default_value: "https://example.com".to_string(),
            hint: None,
            credential: None,
        }];
        let (variables, _) =
            collect_inputs_with_hint(&inputs, |_| "https://override.test".to_string(), None)
                .expect("env should override default");
        assert_eq!(variables.len(), 1);
        assert_eq!(variables[0].value, "https://override.test");
    }

    #[test]
    fn collect_inputs_uses_default_when_env_empty() {
        let inputs = vec![ManifestInputSpec {
            key: "API_BASE".to_string(),
            kind: ManifestInputKind::Variable,
            required: true,
            default_value: "https://example.com".to_string(),
            hint: None,
            credential: None,
        }];
        let (variables, secrets) = collect_inputs_with_hint(&inputs, |_| String::new(), None)
            .expect("default should satisfy required");
        assert_eq!(secrets.len(), 0);
        assert_eq!(variables.len(), 1);
        assert_eq!(variables[0].value, "https://example.com");
    }

    #[test]
    fn collect_inputs_errors_on_missing_required() {
        let inputs = vec![
            ManifestInputSpec {
                key: "LINEAR_API_KEY".to_string(),
                kind: ManifestInputKind::Secret,
                required: true,
                default_value: String::new(),
                hint: None,
                credential: None,
            },
            ManifestInputSpec {
                key: "OTHER_KEY".to_string(),
                kind: ManifestInputKind::Variable,
                required: true,
                default_value: String::new(),
                hint: None,
                credential: None,
            },
        ];
        let error = collect_inputs_with_hint(&inputs, |_| String::new(), None)
            .expect_err("missing required inputs should fail");
        let message = error.to_string();
        assert!(message.contains("LINEAR_API_KEY"));
        assert!(message.contains("OTHER_KEY"));
        assert!(message.contains("--interactive"));
    }

    #[test]
    fn identity_spec_inputs_for_add_noninteractive_uses_environment_only() {
        let input_key = format!(
            "CORAL_CLI_TEST_REQUIRED_IDENTITY_INPUT_{}",
            std::process::id()
        );
        let manifest = parse_identity_manifest_yaml(&format!(
            r"
kind: identity
spec_version: 1
name: demo_oauth
version: 0.1.0
issuer: demo
type: oauth
audience:
  host: api.example.test
inputs:
  {input_key}:
    kind: secret
    required: true
oauth:
  method:
    label: Demo OAuth
    flow:
      type: authorization_code
      pkce: required
    redirect_uri: http://127.0.0.1:53682/callback
    endpoints:
      authorization_url: https://auth.example.test/authorize
      token_url: https://auth.example.test/token
    client:
      id:
        default: demo-client
      secret:
        input: {input_key}
        transport: request_body
"
        ))
        .expect("identity manifest");

        let error = identity_spec_inputs_for_add(
            &manifest,
            false,
            "coral source add --interactive --file bundle.yaml".to_string(),
        )
        .expect_err("noninteractive identity spec inputs should require env");
        let message = error.to_string();
        assert!(message.contains(&input_key), "unexpected error: {message}");
        assert!(
            message.contains("coral source add --interactive --file bundle.yaml"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn rollback_warnings_explain_non_destructive_cleanup() {
        let identity_spec = identity_spec_left_installed_warning("github_oauth");
        assert!(identity_spec.contains("github_oauth"));
        assert!(identity_spec.contains("left installed"));
        assert!(identity_spec.contains("identity-spec management API"));
        assert!(identity_spec.contains("Retry source add"));

        let identity = user_owned_identity_left_installed_warning("github_users_github_oauth");
        assert!(identity.contains("github_users_github_oauth"));
        assert!(identity.contains("left installed"));
        assert!(identity.contains("identity management API"));
        assert!(identity.contains("Retry source add"));
    }

    #[test]
    fn identity_oauth_error_points_back_to_source_add() {
        let error = oauth_error(
            "identity creation",
            &tonic::Status::unavailable("provider unavailable"),
            "coral source add --interactive --file <manifest.yaml>",
        );
        let message = error.to_string();

        assert!(
            message.contains("coral source add --interactive --file <manifest.yaml>"),
            "unexpected error: {message}"
        );
        assert!(
            !message.contains("coral identity add"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn identity_oauth_progress_label_uses_access_token_event_key() {
        let labels = BTreeMap::from([(
            IDENTITY_OAUTH_PROGRESS_INPUT_KEY.to_string(),
            "GitHub OAuth".to_string(),
        )]);

        assert_eq!(
            oauth_label_for_input_key(IDENTITY_OAUTH_PROGRESS_INPUT_KEY, &labels),
            "GitHub OAuth"
        );
        assert_eq!(
            oauth_label_for_input_key("OTHER_TOKEN", &labels),
            "OTHER_TOKEN"
        );
    }

    #[test]
    fn source_name_arg_rejects_dot_segments() {
        let error = source_name_arg(Some("..")).expect_err("dot segment should fail");
        assert!(error.to_string().contains("must not be '.' or '..'"));

        let error = source_name_arg(Some(" . ")).expect_err("dot segment should fail");
        assert!(error.to_string().contains("must not be '.' or '..'"));
    }

    #[test]
    fn collect_inputs_skips_optional_empty_inputs() {
        let inputs = vec![ManifestInputSpec {
            key: "OPTIONAL".to_string(),
            kind: ManifestInputKind::Variable,
            required: false,
            default_value: String::new(),
            hint: None,
            credential: None,
        }];
        let (variables, secrets) = collect_inputs_with_hint(&inputs, |_| String::new(), None)
            .expect("optional should be omitted");
        assert!(variables.is_empty());
        assert!(secrets.is_empty());
    }

    #[test]
    fn empty_optional_input_is_omitted_for_server_side_defaults() {
        let input = ManifestInputSpec {
            key: "API_BASE".to_string(),
            kind: ManifestInputKind::Variable,
            required: false,
            default_value: "https://example.com".to_string(),
            hint: None,
            credential: None,
        };
        assert_eq!(
            finalize_input_value(&input, String::new(), "source variable")
                .expect("empty optional input should be omitted"),
            None
        );
    }

    #[test]
    fn empty_required_input_without_default_is_rejected() {
        let input = ManifestInputSpec {
            key: "API_TOKEN".to_string(),
            kind: ManifestInputKind::Secret,
            required: true,
            default_value: String::new(),
            hint: None,
            credential: None,
        };
        let error = finalize_input_value(&input, String::new(), "source secret")
            .expect_err("required empty input should fail");
        assert!(error.to_string().contains("missing required source secret"));
    }

    #[test]
    fn shell_quote_arg_quotes_copyable_commands() {
        assert_eq!(shell_quote_arg("sources/demo.yaml"), "sources/demo.yaml");
        assert_eq!(
            shell_quote_arg("fixtures/my source.yaml"),
            "'fixtures/my source.yaml'"
        );
        assert_eq!(shell_quote_arg("it'demo.yaml"), "'it'\\''demo.yaml'");
    }

    #[test]
    fn audience_matches_requires_required_subset() {
        let required = BTreeMap::from([("host".to_string(), serde_json::json!("github.com"))]);
        let candidate = BTreeMap::from([
            ("host".to_string(), serde_json::json!("github.com")),
            ("scheme".to_string(), serde_json::json!("https")),
        ]);
        assert!(audience_matches(&required, &candidate));

        let mismatched =
            BTreeMap::from([("host".to_string(), serde_json::json!("api.github.com"))]);
        assert!(!audience_matches(&required, &mismatched));

        let missing = BTreeMap::new();
        assert!(!audience_matches(&required, &missing));
    }

    #[test]
    fn generated_source_identity_name_uses_unique_suffix() {
        let mut reserved_names = BTreeSet::from([
            "github_v4_rest_github_oauth".to_string(),
            "github_v4_rest_github_oauth_2".to_string(),
        ]);

        let generated = generated_source_identity_name(
            "github_v4",
            "rest",
            "github_oauth",
            &mut reserved_names,
        );

        let prefix = "github_v4_rest_github_oauth_";
        assert!(generated.starts_with(prefix));
        let suffix = generated
            .strip_prefix(prefix)
            .expect("generated name should use expected prefix");
        assert_eq!(suffix.len(), 32);
        assert!(suffix.chars().all(|ch| ch.is_ascii_hexdigit()));
        assert!(reserved_names.contains(&generated));
        assert_ne!(generated, "github_v4_rest_github_oauth");
        assert_ne!(generated, "github_v4_rest_github_oauth_2");
    }

    #[test]
    fn expected_oauth_redirect_reads_authorization_query() {
        let authorization_url = "https://provider.example.com/oauth/authorize?client_id=abc&redirect_uri=http%3A%2F%2Flocalhost%3A53682%2Foauth%2Fcallback&state=xyz";

        let (redirect_uri, state) =
            expected_oauth_redirect(authorization_url).expect("redirect_uri should parse");

        assert_eq!(
            redirect_uri.as_str(),
            "http://localhost:53682/oauth/callback"
        );
        assert_eq!(state.as_deref(), Some("xyz"));
    }

    #[test]
    fn oauth_redirect_url_must_match_expected_loopback_callback() {
        let expected = Url::parse("http://localhost:53682/oauth/callback").expect("expected url");
        let mismatched =
            Url::parse("http://localhost:53682/other?state=xyz&code=abc").expect("callback url");

        let error = validate_oauth_redirect_url(&mismatched, &expected, None)
            .expect_err("mismatched callback should fail");

        assert!(
            error
                .to_string()
                .contains("must match the OAuth redirect URI"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn oauth_redirect_url_rejects_non_loopback_hosts() {
        let expected = Url::parse("http://localhost:53682/oauth/callback").expect("expected url");
        let callback = Url::parse("http://example.com:53682/oauth/callback?state=xyz&code=abc")
            .expect("callback url");

        let error = validate_oauth_redirect_url(&callback, &expected, None)
            .expect_err("non-loopback callback should fail");

        assert!(
            error.to_string().contains("host must be loopback"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn submit_oauth_redirect_url_sends_get_to_loopback_listener() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind callback listener");
        let port = listener.local_addr().expect("listener addr").port();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept callback");
            let mut buffer = [0_u8; 1024];
            let read = stream.read(&mut buffer).expect("read callback request");
            let request = String::from_utf8_lossy(&buffer[..read]);
            assert!(
                request.starts_with("GET /oauth/callback?state=xyz&code=test-code HTTP/1.1\r\n"),
                "unexpected request: {request}"
            );
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok")
                .expect("write callback response");
        });
        let expected =
            Url::parse(&format!("http://127.0.0.1:{port}/oauth/callback")).expect("expected url");
        let callback_url =
            format!("http://127.0.0.1:{port}/oauth/callback?state=xyz&code=test-code");

        submit_oauth_redirect_url(&callback_url, &expected, Some("xyz"))
            .expect("submit redirect url");
        server.join().expect("callback server");
    }

    #[test]
    fn oauth_redirect_url_must_match_expected_state_when_present() {
        let expected = Url::parse("http://localhost:53682/oauth/callback").expect("expected url");
        let stale = Url::parse("http://localhost:53682/oauth/callback?state=old&code=abc")
            .expect("callback url");

        let error = validate_oauth_redirect_url(&stale, &expected, Some("xyz"))
            .expect_err("state mismatch should fail before callback submission");

        assert!(
            error.to_string().contains("state"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn redirect_prompt_key_events_collect_submit_and_edit_url() {
        let mut value = String::new();

        for ch in "http://localhost/callback".chars() {
            assert_eq!(
                apply_redirect_prompt_key(
                    KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE),
                    &mut value
                ),
                RedirectPromptAction::Continue
            );
        }
        assert_eq!(
            apply_redirect_prompt_key(
                KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE),
                &mut value
            ),
            RedirectPromptAction::Continue
        );
        assert_eq!(
            apply_redirect_prompt_key(
                KeyEvent::new(KeyCode::Char('k'), KeyModifiers::NONE),
                &mut value
            ),
            RedirectPromptAction::Continue
        );
        assert_eq!(
            apply_redirect_prompt_key(
                KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
                &mut value
            ),
            RedirectPromptAction::Submit
        );

        assert_eq!(value, "http://localhost/callback");
    }

    #[test]
    fn redirect_prompt_key_events_cancel_without_appending_control_input() {
        let mut value = String::from("http://localhost/callback");

        assert_eq!(
            apply_redirect_prompt_key(
                KeyEvent::new(KeyCode::Char('x'), KeyModifiers::CONTROL),
                &mut value
            ),
            RedirectPromptAction::Continue
        );
        assert_eq!(
            apply_redirect_prompt_key(
                KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
                &mut value
            ),
            RedirectPromptAction::Cancel
        );
        assert_eq!(value, "http://localhost/callback");
    }

    #[test]
    fn redirect_prompt_key_echoes_visible_edits() {
        let mut output = Vec::new();

        render_redirect_prompt_key_echo(
            &mut output,
            KeyEvent::new(KeyCode::Char('h'), KeyModifiers::NONE),
            0,
            1,
        )
        .expect("echo char");
        render_redirect_prompt_key_echo(
            &mut output,
            KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE),
            1,
            0,
        )
        .expect("echo backspace");
        render_redirect_prompt_key_echo(
            &mut output,
            KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
            0,
            0,
        )
        .expect("skip control char");

        assert_eq!(output, b"h\x08 \x08");
    }

    #[test]
    fn validation_follow_up_is_none_when_all_query_tests_pass() {
        let response = ValidateSourceResponse {
            source: None,
            tables: Vec::new(),
            table_functions: Vec::new(),
            query_tests: vec![coral_api::v1::QueryTestResult {
                sql: "SELECT 1".to_string(),
                outcome: Some(coral_api::v1::query_test_result::Outcome::Success(
                    coral_api::v1::QueryTestSuccess { row_count: 1 },
                )),
            }],
        };

        assert_eq!(
            validation_follow_up(&response, ValidationSeverityMode::Strict),
            ValidationFollowUp::None
        );
    }

    #[test]
    fn validation_follow_up_is_error_in_strict_mode() {
        let response = ValidateSourceResponse {
            source: None,
            tables: Vec::new(),
            table_functions: Vec::new(),
            query_tests: vec![
                coral_api::v1::QueryTestResult {
                    sql: "SELECT 1".to_string(),
                    outcome: Some(coral_api::v1::query_test_result::Outcome::Success(
                        coral_api::v1::QueryTestSuccess { row_count: 1 },
                    )),
                },
                coral_api::v1::QueryTestResult {
                    sql: "SELECT missing".to_string(),
                    outcome: Some(coral_api::v1::query_test_result::Outcome::Failure(
                        coral_api::v1::QueryTestFailure {
                            error_message: "missing".to_string(),
                        },
                    )),
                },
            ],
        };

        assert_eq!(
            validation_follow_up(&response, ValidationSeverityMode::Strict),
            ValidationFollowUp::Fail("1 of 2 validation queries failed".to_string())
        );
    }

    #[test]
    fn validation_follow_up_is_warning_in_warn_only_mode() {
        let response = ValidateSourceResponse {
            source: None,
            tables: Vec::new(),
            table_functions: Vec::new(),
            query_tests: vec![coral_api::v1::QueryTestResult {
                sql: "SELECT missing".to_string(),
                outcome: Some(coral_api::v1::query_test_result::Outcome::Failure(
                    coral_api::v1::QueryTestFailure {
                        error_message: "missing".to_string(),
                    },
                )),
            }],
        };

        assert_eq!(
            validation_follow_up(&response, ValidationSeverityMode::WarnOnly),
            ValidationFollowUp::Warn("1 of 1 validation query failed".to_string())
        );
    }
}
