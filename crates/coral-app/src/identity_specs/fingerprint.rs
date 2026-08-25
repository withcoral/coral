//! Stable fingerprints for normalized identity manifests.

use std::collections::BTreeMap;

use coral_spec::{
    IdentityManifest, IdentityOAuthMethodSpec, IdentityOAuthSpec, IdentitySpecConfig,
    IdentitySpecType, ManifestInputKind, ManifestInputSpec, ManifestOAuthClientIdSpec,
    ManifestOAuthClientSecretSpec, ManifestOAuthClientSecretTransport, ManifestOAuthClientSpec,
    ManifestOAuthCredentialSpec, ManifestOAuthDynamicClientRegistrationAuthMethod,
    ManifestOAuthDynamicClientRegistrationSpec, ManifestOAuthFlowKind, ManifestOAuthFlowSpec,
    ManifestOAuthPkceMode, ManifestOAuthRedirectUriPortMode, ManifestOAuthScopeDelimiter,
    ManifestOAuthScopeSpec, ManifestOAuthScopesSpec,
};
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};

use crate::bootstrap::AppError;

const FINGERPRINT_PREFIX: &str = "identity-manifest-v1:sha256:";

/// Fingerprint one normalized manifest using the durable v1 semantic projection.
pub(crate) fn identity_spec_fingerprint(manifest: &IdentityManifest) -> Result<String, AppError> {
    let canonical = canonicalize_json(project_manifest(manifest)?);
    let digest = Sha256::digest(serde_json::to_vec(&canonical)?);
    Ok(format!("{FINGERPRINT_PREFIX}{digest:x}"))
}

fn project_manifest(manifest: &IdentityManifest) -> Result<Value, AppError> {
    let IdentityManifest {
        spec_version,
        name,
        version,
        description,
        issuer,
        identity_type,
        audience,
        inputs,
        config,
    } = manifest;
    let inputs = inputs
        .iter()
        .map(project_input)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({
        "spec_version": spec_version,
        "name": name,
        "version": version,
        "description": description,
        "issuer": issuer,
        "identity_type": identity_type_label(*identity_type),
        "audience": project_audience(audience)?,
        "inputs": inputs,
        "config": project_config(config),
    }))
}

fn project_audience(audience: &BTreeMap<String, Value>) -> Result<Value, AppError> {
    if audience.keys().any(|key| key != "host" && key != "port") {
        return Err(invalid_audience());
    }
    let host = audience
        .get("host")
        .and_then(Value::as_str)
        .ok_or_else(invalid_audience)?;
    let normalized_host = url::Host::parse(host).map_err(|_error| invalid_audience())?;
    if normalized_host.to_string() != host {
        return Err(invalid_audience());
    }
    let port = audience
        .get("port")
        .map(|value| {
            value
                .as_u64()
                .and_then(|port| u16::try_from(port).ok())
                .filter(|port| *port > 0)
                .ok_or_else(invalid_audience)
        })
        .transpose()?;
    Ok(json!({ "host": host, "port": port }))
}

fn invalid_audience() -> AppError {
    AppError::InvalidInput(
        "identity manifest audience is not normalized host/port data".to_string(),
    )
}

fn project_input(input: &ManifestInputSpec) -> Result<Value, AppError> {
    let ManifestInputSpec {
        key,
        kind,
        required,
        default_value,
        hint,
        credential,
    } = input;
    if credential.is_some() {
        return Err(AppError::InvalidInput(format!(
            "identity manifest input '{key}' has unsupported credential methods"
        )));
    }
    Ok(json!({
        "key": key,
        "kind": input_kind_label(*kind),
        "required": required,
        "default_value": default_value,
        "hint": hint,
        "credential": Value::Null,
    }))
}

fn project_config(config: &IdentitySpecConfig) -> Value {
    match config {
        IdentitySpecConfig::FixedToken => json!({ "kind": "fixed_token" }),
        IdentitySpecConfig::OAuth(oauth) => {
            let IdentityOAuthSpec { method } = oauth.as_ref();
            json!({ "kind": "oauth", "method": project_oauth_method(method) })
        }
    }
}

fn project_oauth_method(method: &IdentityOAuthMethodSpec) -> Value {
    let IdentityOAuthMethodSpec {
        label,
        description,
        hint,
        oauth,
    } = method;
    json!({
        "label": label,
        "description": description,
        "hint": hint,
        "oauth": project_oauth(oauth),
    })
}

fn project_oauth(oauth: &ManifestOAuthCredentialSpec) -> Value {
    let ManifestOAuthCredentialSpec {
        flow,
        resource,
        redirect_uri,
        redirect_uri_port_mode,
        authorization_url,
        device_authorization_url,
        token_url,
        client,
        scopes,
    } = oauth;
    json!({
        "flow": project_flow(flow),
        "resource": resource,
        "redirect_uri": redirect_uri,
        "redirect_uri_port_mode": redirect_uri_port_mode_label(*redirect_uri_port_mode),
        "authorization_url": authorization_url,
        "device_authorization_url": device_authorization_url,
        "token_url": token_url,
        "client": project_client(client),
        "scopes": scopes.as_ref().map(project_scopes),
    })
}

fn project_flow(flow: &ManifestOAuthFlowSpec) -> Value {
    let ManifestOAuthFlowSpec { kind, pkce } = flow;
    json!({
        "kind": flow_kind_label(*kind),
        "pkce": pkce_label(*pkce),
    })
}

fn project_client(client: &ManifestOAuthClientSpec) -> Value {
    let ManifestOAuthClientSpec {
        id,
        secret,
        dynamic_registration,
    } = client;
    json!({
        "id": project_client_id(id),
        "secret": secret.as_ref().map(project_client_secret),
        "dynamic_registration": dynamic_registration.as_ref().map(project_dynamic_registration),
    })
}

fn project_client_id(id: &ManifestOAuthClientIdSpec) -> Value {
    let ManifestOAuthClientIdSpec { default, input } = id;
    json!({ "default": default, "input": input })
}

fn project_client_secret(secret: &ManifestOAuthClientSecretSpec) -> Value {
    let ManifestOAuthClientSecretSpec { input, transport } = secret;
    json!({ "input": input, "transport": client_secret_transport_label(*transport) })
}

fn project_dynamic_registration(
    registration: &ManifestOAuthDynamicClientRegistrationSpec,
) -> Value {
    let ManifestOAuthDynamicClientRegistrationSpec {
        registration_url,
        client_name,
        token_endpoint_auth_method,
        request_refresh_token_grant,
    } = registration;
    json!({
        "registration_url": registration_url,
        "client_name": client_name,
        "token_endpoint_auth_method": dynamic_auth_method_label(*token_endpoint_auth_method),
        "request_refresh_token_grant": request_refresh_token_grant,
    })
}

fn project_scopes(scopes: &ManifestOAuthScopesSpec) -> Value {
    let ManifestOAuthScopesSpec { scope } = scopes;
    let ManifestOAuthScopeSpec { delimiter, values } = scope;
    json!({
        "scope": {
            "delimiter": scope_delimiter_label(*delimiter),
            "values": values,
        }
    })
}

fn canonicalize_json(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize_json).collect()),
        Value::Number(number) if number.is_f64() && number.as_f64() == Some(0.0) => json!(0.0),
        Value::Object(object) => {
            let mut entries = object.into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonicalize_json(value)))
                    .collect::<Map<_, _>>(),
            )
        }
        scalar => scalar,
    }
}

fn input_kind_label(kind: ManifestInputKind) -> &'static str {
    match kind {
        ManifestInputKind::Variable => "variable",
        ManifestInputKind::Secret => "secret",
    }
}

fn identity_type_label(identity_type: IdentitySpecType) -> &'static str {
    match identity_type {
        IdentitySpecType::OAuth => "oauth",
        IdentitySpecType::FixedToken => "fixed_token",
    }
}

fn client_secret_transport_label(transport: ManifestOAuthClientSecretTransport) -> &'static str {
    match transport {
        ManifestOAuthClientSecretTransport::BasicAuth => "basic_auth",
        ManifestOAuthClientSecretTransport::RequestBody => "request_body",
    }
}

fn flow_kind_label(kind: ManifestOAuthFlowKind) -> &'static str {
    match kind {
        ManifestOAuthFlowKind::AuthorizationCode => "authorization_code",
        ManifestOAuthFlowKind::DeviceCode => "device_code",
    }
}

fn pkce_label(pkce: ManifestOAuthPkceMode) -> &'static str {
    match pkce {
        ManifestOAuthPkceMode::Required => "required",
        ManifestOAuthPkceMode::Disabled => "disabled",
    }
}

fn redirect_uri_port_mode_label(mode: ManifestOAuthRedirectUriPortMode) -> &'static str {
    match mode {
        ManifestOAuthRedirectUriPortMode::Fixed => "fixed",
        ManifestOAuthRedirectUriPortMode::Random => "random",
    }
}

fn dynamic_auth_method_label(
    method: ManifestOAuthDynamicClientRegistrationAuthMethod,
) -> &'static str {
    match method {
        ManifestOAuthDynamicClientRegistrationAuthMethod::None => "none",
        ManifestOAuthDynamicClientRegistrationAuthMethod::ClientSecretBasic => {
            "client_secret_basic"
        }
        ManifestOAuthDynamicClientRegistrationAuthMethod::ClientSecretPost => "client_secret_post",
    }
}

fn scope_delimiter_label(delimiter: ManifestOAuthScopeDelimiter) -> &'static str {
    match delimiter {
        ManifestOAuthScopeDelimiter::Space => "space",
        ManifestOAuthScopeDelimiter::Comma => "comma",
    }
}

#[cfg(test)]
mod tests {
    use coral_spec::{
        IdentityOAuthMethodSpec, IdentitySpecConfig, IdentitySpecType as Type,
        ManifestCredentialSpec, ManifestInputKind as InputKind,
        ManifestOAuthClientSecretTransport as SecretTransport,
        ManifestOAuthDynamicClientRegistrationAuthMethod as DcrAuth,
        ManifestOAuthFlowKind as FlowKind, ManifestOAuthPkceMode as Pkce,
        ManifestOAuthRedirectUriPortMode as PortMode,
        ManifestOAuthScopeDelimiter as ScopeDelimiter, parse_identity_manifest_yaml,
    };

    use super::{
        canonicalize_json, client_secret_transport_label, dynamic_auth_method_label,
        identity_spec_fingerprint, project_manifest, redirect_uri_port_mode_label,
        scope_delimiter_label,
    };

    macro_rules! assert_changes {
        ($manifest:ident, $(|$value:ident| $change:expr),+ $(,)?) => {{
            let original = fingerprint(&$manifest);
            $(
                let mut changed = $manifest.clone();
                let $value = &mut changed;
                $change;
                assert_ne!(
                    original,
                    fingerprint(&changed),
                    "mutation did not change fingerprint: {}",
                    stringify!($change)
                );
            )+
        }};
    }

    const FIXED_TOKEN: &str = "kind: identity\nspec_version: 1\nname: demo_token\nversion: 1.2.3\ndescription: Demo token\nissuer: demo\ntype: fixed_token\naudience: {host: api.example.com}\n";

    const FIXED_TOKEN_REORDERED: &str = "# Formatting, field order, and host case are normalized.\naudience: {host: API.EXAMPLE.COM}\ntype: fixed_token\nissuer: demo\ndescription: Demo token\nversion: 1.2.3\nname: demo_token\nspec_version: 1\nkind: identity\n";

    const FULL_OAUTH: &str = r"
kind: identity
spec_version: 1
name: demo_oauth
version: 2.0.0
description: Demo OAuth
issuer: demo
type: oauth
audience: {host: api.example.com, port: 8443}
inputs:
  CLIENT_ID: {kind: variable, required: false, default: fallback-client, hint: Client id}
  CLIENT_SECRET: {kind: secret, required: true, hint: Client secret}
oauth:
  method:
    label: Connect Demo
    description: Authorize Coral
    hint: Sign in in the browser
    flow: {type: authorization_code, pkce: required}
    resource: https://api.example.com/
    redirect_uri: http://127.0.0.1:53682/oauth/callback
    redirect_uri_port_mode: fixed
    endpoints:
      authorization_url: https://provider.example.com/authorize
      token_url: https://provider.example.com/token
    client:
      id: {default: demo-client, input: CLIENT_ID}
      secret: {input: CLIENT_SECRET, transport: request_body}
      dynamic_registration:
        registration_url: https://provider.example.com/register
        client_name: Coral
        token_endpoint_auth_method: client_secret_post
        request_refresh_token_grant: true
    scopes:
      scope: {delimiter: space, values: [read, write]}
";

    const DEVICE_OAUTH: &str = r"
kind: identity
spec_version: 1
name: demo_device
version: 1.0.0
issuer: demo
type: oauth
audience: {host: device.example.com}
oauth:
  method:
    flow: {type: device_code}
    endpoints: {device_authorization_url: https://provider.example.com/device, token_url: https://provider.example.com/token}
    client:
      dynamic_registration:
        registration_url: https://provider.example.com/register
        token_endpoint_auth_method: none
";

    #[test]
    fn alternate_v1_enum_labels_are_exact() {
        assert_eq!(
            client_secret_transport_label(SecretTransport::BasicAuth),
            "basic_auth"
        );
        assert_eq!(redirect_uri_port_mode_label(PortMode::Random), "random");
        assert_eq!(
            dynamic_auth_method_label(DcrAuth::ClientSecretBasic),
            "client_secret_basic"
        );
        assert_eq!(scope_delimiter_label(ScopeDelimiter::Comma), "comma");
    }

    #[test]
    fn fixed_token_fingerprint_is_stable_over_yaml_and_host_normalization() {
        let original = parse_identity_manifest_yaml(FIXED_TOKEN).expect("original manifest");
        let reordered =
            parse_identity_manifest_yaml(FIXED_TOKEN_REORDERED).expect("reordered manifest");
        assert_eq!(original, reordered);
        let actual = fingerprint(&original);
        assert_eq!(actual, fingerprint(&reordered));
        assert_eq!(
            actual,
            "identity-manifest-v1:sha256:4df6fa8c02ab513651e394ed25d00123ebfa0c19740f5a3d03b9a61a53a6f56f"
        );
    }

    #[test]
    fn rejects_non_normalized_audience_states() {
        for audience in [
            serde_json::json!({}),
            serde_json::json!({"host": "api.example.com", "unknown": true}),
            serde_json::json!({"host": 1}),
            serde_json::json!({"host": "API.EXAMPLE.COM"}),
            serde_json::json!({"host": "api.example.com", "port": 0}),
            serde_json::json!({"host": "api.example.com", "port": 65536}),
            serde_json::json!({"host": "api.example.com", "port": "443"}),
            serde_json::json!({"host": "api.example.com", "port": null}),
        ] {
            let mut manifest = parse_identity_manifest_yaml(FIXED_TOKEN).expect("valid manifest");
            manifest.audience = serde_json::from_value(audience).expect("test audience map");
            let error = identity_spec_fingerprint(&manifest).expect_err("invalid audience");
            assert!(error.to_string().contains("audience"));
        }
    }

    #[test]
    fn canonical_json_pins_escape_sensitive_utf8() {
        let mut manifest = parse_identity_manifest_yaml(FIXED_TOKEN).expect("valid manifest");
        manifest.description = "Snowman ☃ \"quoted\" \\ slash\nline".to_string();
        let canonical = serde_json::to_string(&canonicalize_json(
            project_manifest(&manifest).expect("project manifest"),
        ))
        .expect("serialize canonical JSON");
        assert_eq!(
            canonical,
            r#"{"audience":{"host":"api.example.com","port":null},"config":{"kind":"fixed_token"},"description":"Snowman ☃ \"quoted\" \\ slash\nline","identity_type":"fixed_token","inputs":[],"issuer":"demo","name":"demo_token","spec_version":1,"version":"1.2.3"}"#
        );
        assert_eq!(
            fingerprint(&manifest),
            "identity-manifest-v1:sha256:ff415916d340318b296da86232479f0fd4834c890987db6d53d57580499cb0b8"
        );
    }

    #[test]
    fn oauth_fingerprint_covers_every_normalized_field_family() {
        let manifest = parse_identity_manifest_yaml(FULL_OAUTH).expect("full OAuth manifest");
        assert_eq!(
            fingerprint(&manifest),
            "identity-manifest-v1:sha256:65b4babb9b553d7cc574b429f44b060de23a7985d374da75846fc8b1044b6780"
        );
        assert_changes!(
            manifest,
            |value| value.spec_version += 1,
            |value| value.name.push_str("_changed"),
            |value| value.version.push_str("-changed"),
            |value| value.description.push_str(" changed"),
            |value| value.issuer.push_str("_changed"),
            |value| value.identity_type = Type::FixedToken,
            |value| value
                .audience
                .insert("host".into(), serde_json::json!("other.example.com")),
            |value| value
                .audience
                .insert("port".into(), serde_json::json!(9443)),
            |value| value.inputs.swap(0, 1),
            |value| first_input(value).key.push_str("_CHANGED"),
            |value| first_input(value).kind = InputKind::Secret,
            |value| first_input(value).required = !first_input(value).required,
            |value| first_input(value).default_value.push_str("-changed"),
            |value| first_input(value).hint = None,
            |value| oauth_method(value).label = None,
            |value| oauth_method(value).description = None,
            |value| oauth_method(value).hint = None,
            |value| oauth(value).flow.kind = FlowKind::DeviceCode,
            |value| oauth(value).flow.pkce = Pkce::Disabled,
            |value| oauth(value).resource = None,
            |value| oauth(value).redirect_uri = None,
            |value| oauth(value).redirect_uri_port_mode = PortMode::Random,
            |value| oauth(value).authorization_url = None,
            |value| oauth(value).device_authorization_url =
                Some("https://provider.example.com/device".into()),
            |value| oauth(value).token_url.push_str("/changed"),
            |value| oauth(value).client.id.default = None,
            |value| oauth(value).client.id.input = None,
            |value| oauth(value)
                .client
                .secret
                .as_mut()
                .unwrap()
                .input
                .push_str("_CHANGED"),
            |value| oauth(value).client.secret.as_mut().unwrap().transport =
                SecretTransport::BasicAuth,
            |value| oauth(value)
                .client
                .dynamic_registration
                .as_mut()
                .unwrap()
                .registration_url
                .push_str("/changed"),
            |value| oauth(value)
                .client
                .dynamic_registration
                .as_mut()
                .unwrap()
                .client_name = None,
            |value| oauth(value)
                .client
                .dynamic_registration
                .as_mut()
                .unwrap()
                .token_endpoint_auth_method = DcrAuth::ClientSecretBasic,
            |value| oauth(value)
                .client
                .dynamic_registration
                .as_mut()
                .unwrap()
                .request_refresh_token_grant = false,
            |value| oauth(value).scopes.as_mut().unwrap().scope.delimiter = ScopeDelimiter::Comma,
            |value| oauth(value).scopes.as_mut().unwrap().scope.values.reverse(),
            |value| value.config = IdentitySpecConfig::FixedToken,
        );
    }

    #[test]
    fn device_code_fingerprint_pins_distinct_defaults() {
        let manifest = parse_identity_manifest_yaml(DEVICE_OAUTH).expect("device OAuth manifest");
        assert_eq!(
            fingerprint(&manifest),
            "identity-manifest-v1:sha256:ad62e00f99b8f9df616218c47fcbf9924b913ce23dc3e9448890b87e261a551a"
        );
    }

    #[test]
    fn rejects_inputs_outside_the_normalized_identity_domain() {
        let mut manifest = parse_identity_manifest_yaml(FULL_OAUTH).expect("full OAuth manifest");
        first_input(&mut manifest).credential = Some(ManifestCredentialSpec { methods: vec![] });
        let error = identity_spec_fingerprint(&manifest).expect_err("credential methods must fail");
        assert!(error.to_string().contains("unsupported credential methods"));
    }

    fn oauth_method(manifest: &mut coral_spec::IdentityManifest) -> &mut IdentityOAuthMethodSpec {
        let IdentitySpecConfig::OAuth(config) = &mut manifest.config else {
            panic!("expected OAuth config");
        };
        &mut config.method
    }

    fn first_input(
        manifest: &mut coral_spec::IdentityManifest,
    ) -> &mut coral_spec::ManifestInputSpec {
        manifest.inputs.first_mut().expect("fixture input")
    }

    fn oauth(
        manifest: &mut coral_spec::IdentityManifest,
    ) -> &mut coral_spec::ManifestOAuthCredentialSpec {
        &mut oauth_method(manifest).oauth
    }

    fn fingerprint(manifest: &coral_spec::IdentityManifest) -> String {
        identity_spec_fingerprint(manifest).expect("fingerprint")
    }
}
