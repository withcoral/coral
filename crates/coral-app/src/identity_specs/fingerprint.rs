//! Stable fingerprints for normalized identity manifests.

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

/// Fingerprint one normalized manifest using the durable v1 projection.
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
        "audience": audience,
        "inputs": inputs,
        "config": project_config(config),
    }))
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
        FINGERPRINT_PREFIX, canonicalize_json, client_secret_transport_label,
        dynamic_auth_method_label, flow_kind_label, identity_spec_fingerprint, identity_type_label,
        input_kind_label, pkce_label, project_manifest, redirect_uri_port_mode_label,
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

    macro_rules! assert_label {
        ($actual:expr => $expected:literal) => {
            assert_eq!($actual, $expected);
        };
    }

    const FIXED_TOKEN: &str = r"
kind: identity
spec_version: 1
name: demo_token
version: 1.2.3
description: Demo token
issuer: demo
type: fixed_token
audience:
  hosts: [api.example.com, uploads.example.com]
  nested: {z: 1, a: [{y: 2, x: 1}]}
";

    const FIXED_TOKEN_REORDERED: &str = r"
# Formatting and object order are outside the normalized manifest contract.
audience:
  nested:
    a: [{x: 1, y: 2}]
    z: 1
  hosts:
    - api.example.com
    - uploads.example.com
type: fixed_token
issuer: demo
description: Demo token
version: 1.2.3
name: demo_token
spec_version: 1
kind: identity
";

    const FULL_OAUTH: &str = r"
kind: identity
spec_version: 1
name: demo_oauth
version: 2.0.0
description: Demo OAuth
issuer: demo
type: oauth
audience:
  hosts: [api.example.com, uploads.example.com]
  claims: {tenant: coral, nested: [{z: 2, a: 1}]}
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
oauth:
  method:
    flow: {type: device_code}
    endpoints: {device_authorization_url: https://provider.example.com/device, token_url: https://provider.example.com/token}
    client:
      dynamic_registration:
        registration_url: https://provider.example.com/register
        token_endpoint_auth_method: none
";

    const OAUTH_CANONICAL: &str = concat!(
        r#"{"audience":{"claims":{"nested":[{"a":1,"z":2}],"tenant":"coral"},"hosts":["api.example.com","uploads.example.com"]},"config":{"kind":"oauth","method":{"description":"Authorize Coral","hint":"Sign in in the browser","label":"Connect Demo","oauth":{"authorization_url":"https://provider.example.com/authorize","client":{"dynamic_registration":{"client_name":"Coral","registration_url":"https://provider.example.com/register","request_refresh_token_grant":true,"token_endpoint_auth_method":"client_secret_post"},"#,
        r#""id":{"default":"demo-client","input":"CLIENT_ID"},"secret":{"input":"CLIENT_SECRET","transport":"request_body"}},"device_authorization_url":null,"flow":{"kind":"authorization_code","pkce":"required"},"redirect_uri":"http://127.0.0.1:53682/oauth/callback","redirect_uri_port_mode":"fixed","resource":"https://api.example.com/","scopes":{"scope":{"delimiter":"space","values":["read","write"]}},"token_url":"https://provider.example.com/token"}}},"#,
        r#""description":"Demo OAuth","identity_type":"oauth","inputs":[{"credential":null,"default_value":"fallback-client","hint":"Client id","key":"CLIENT_ID","kind":"variable","required":false},{"credential":null,"default_value":"","hint":"Client secret","key":"CLIENT_SECRET","kind":"secret","required":true}],"issuer":"demo","name":"demo_oauth","spec_version":1,"version":"2.0.0"}"#,
    );

    const DEVICE_CANONICAL: &str = concat!(
        r#"{"audience":{},"config":{"kind":"oauth","method":{"description":null,"hint":null,"label":null,"oauth":{"authorization_url":null,"client":{"dynamic_registration":{"client_name":null,"registration_url":"https://provider.example.com/register","request_refresh_token_grant":false,"token_endpoint_auth_method":"none"},"id":{"default":null,"input":null},"secret":null},"#,
        r#""device_authorization_url":"https://provider.example.com/device","flow":{"kind":"device_code","pkce":"disabled"},"redirect_uri":null,"redirect_uri_port_mode":"fixed","resource":null,"scopes":null,"token_url":"https://provider.example.com/token"}}},"#,
        r#""description":"","identity_type":"oauth","inputs":[],"issuer":"demo","name":"demo_device","spec_version":1,"version":"1.0.0"}"#,
    );

    #[test]
    fn every_v1_enum_label_is_exact() {
        assert_label!(identity_type_label(Type::OAuth) => "oauth");
        assert_label!(identity_type_label(Type::FixedToken) => "fixed_token");
        assert_label!(input_kind_label(InputKind::Variable) => "variable");
        assert_label!(input_kind_label(InputKind::Secret) => "secret");
        assert_label!(client_secret_transport_label(SecretTransport::BasicAuth) => "basic_auth");
        assert_label!(client_secret_transport_label(SecretTransport::RequestBody) => "request_body");
        assert_label!(flow_kind_label(FlowKind::AuthorizationCode) => "authorization_code");
        assert_label!(flow_kind_label(FlowKind::DeviceCode) => "device_code");
        assert_label!(pkce_label(Pkce::Required) => "required");
        assert_label!(pkce_label(Pkce::Disabled) => "disabled");
        assert_label!(redirect_uri_port_mode_label(PortMode::Fixed) => "fixed");
        assert_label!(redirect_uri_port_mode_label(PortMode::Random) => "random");
        assert_label!(dynamic_auth_method_label(DcrAuth::None) => "none");
        assert_label!(dynamic_auth_method_label(DcrAuth::ClientSecretBasic) => "client_secret_basic");
        assert_label!(dynamic_auth_method_label(DcrAuth::ClientSecretPost) => "client_secret_post");
        assert_label!(scope_delimiter_label(ScopeDelimiter::Space) => "space");
        assert_label!(scope_delimiter_label(ScopeDelimiter::Comma) => "comma");
    }

    #[test]
    fn fixed_token_projection_and_fingerprint_are_stable() {
        let manifest = parse_identity_manifest_yaml(FIXED_TOKEN).expect("fixed-token manifest");
        let canonical = canonicalize_json(project_manifest(&manifest).expect("projection"));
        assert_eq!(
            serde_json::to_string(&canonical).expect("canonical JSON"),
            r#"{"audience":{"hosts":["api.example.com","uploads.example.com"],"nested":{"a":[{"x":1,"y":2}],"z":1}},"config":{"kind":"fixed_token"},"description":"Demo token","identity_type":"fixed_token","inputs":[],"issuer":"demo","name":"demo_token","spec_version":1,"version":"1.2.3"}"#,
        );
        let fingerprint = fingerprint(&manifest);
        assert_eq!(
            fingerprint,
            "identity-manifest-v1:sha256:718233b7247bca4ab64ad114dae9e03cf81a79c74ef98678afe63ca5c7526a16"
        );
        let digest = fingerprint
            .strip_prefix(FINGERPRINT_PREFIX)
            .expect("versioned fingerprint prefix");
        assert_eq!(digest.len(), 64);
        assert!(
            digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
    }

    #[test]
    fn formatting_and_recursive_object_order_do_not_change_fingerprint() {
        let original = parse_identity_manifest_yaml(FIXED_TOKEN).expect("original manifest");
        let reordered =
            parse_identity_manifest_yaml(FIXED_TOKEN_REORDERED).expect("reordered manifest");
        assert_eq!(original, reordered);
        assert_eq!(fingerprint(&original), fingerprint(&reordered));
    }

    #[test]
    fn oauth_projection_golden_covers_every_semantic_field_family() {
        let manifest = parse_identity_manifest_yaml(FULL_OAUTH).expect("full OAuth manifest");
        assert_eq!(canonical_json(&manifest), OAUTH_CANONICAL);
        assert_eq!(
            fingerprint(&manifest),
            "identity-manifest-v1:sha256:2374165add17483b3e2c64c00f24369d50d0ef5ed3e9092fbb12d5652ae6f6db"
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
                .get_mut("hosts")
                .unwrap()
                .as_array_mut()
                .unwrap()
                .reverse(),
            |value| value.inputs.swap(0, 1),
            |value| first_input(value).key.push_str("_CHANGED"),
            |value| first_input(value).kind = InputKind::Secret,
            |value| {
                let input = first_input(value);
                input.required = !input.required;
            },
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
            |value| oauth(value).scopes.as_mut().unwrap().scope.delimiter = ScopeDelimiter::Comma,
            |value| oauth(value).scopes.as_mut().unwrap().scope.values.reverse(),
            |value| value.config = IdentitySpecConfig::FixedToken,
        );
    }

    #[test]
    fn device_code_projection_golden_covers_its_distinct_shape() {
        let manifest = parse_identity_manifest_yaml(DEVICE_OAUTH).expect("device OAuth manifest");
        assert_eq!(canonical_json(&manifest), DEVICE_CANONICAL);
        assert_eq!(
            fingerprint(&manifest),
            "identity-manifest-v1:sha256:08b36a2d9a3dfa169f37bdfaa2ee540436cfbfd316e933b8a3e0cbce7e5c195d"
        );
    }

    #[test]
    fn equal_floating_signed_zero_values_have_one_fingerprint() {
        let mut positive = parse_identity_manifest_yaml(FIXED_TOKEN).expect("positive zero");
        positive
            .audience
            .insert("zero".into(), serde_json::json!(0.0));
        let mut negative = positive.clone();
        negative
            .audience
            .insert("zero".into(), serde_json::json!(-0.0));
        assert_eq!(positive, negative);
        assert_eq!(fingerprint(&positive), fingerprint(&negative));
    }

    #[test]
    fn rejects_input_credentials_outside_the_normalized_identity_domain() {
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

    fn canonical_json(manifest: &coral_spec::IdentityManifest) -> String {
        serde_json::to_string(&canonicalize_json(
            project_manifest(manifest).expect("projection"),
        ))
        .expect("canonical JSON")
    }
}
