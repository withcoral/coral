//! Provider Setup and Publication over native gRPC, against real domain owners.
#![expect(
    clippy::indexing_slicing,
    reason = "fixtures assert and build known JSON members"
)]
#[path = "accounts.rs"]
mod accounts;

use super::{Fixture, error_reason, management, request};
use management::create_provider_setup_request::Input;
use management::provider_configuration::Authentication;
use serde_json::{Value, json};
use wiremock::matchers::{body_partial_json, header, method};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

type SetupClient = management::provider_setup_service_client::ProviderSetupServiceClient<
    tonic::transport::Channel,
>;

/// A new Setup in `parent` from `input`, with no discovery credential or target Provider.
fn setup_request(
    parent: impl Into<String>,
    input: Input,
) -> management::CreateProviderSetupRequest {
    management::CreateProviderSetupRequest {
        parent: parent.into(),
        input: Some(input),
        ..Default::default()
    }
}

/// A bearer token as the one Credential Field `token`.
fn token_values(token: &str) -> std::collections::HashMap<String, String> {
    std::collections::HashMap::from([("token".into(), token.into())])
}

/// One API key, the field `key` labelled "API key", sent after `prefix` in `header`.
fn header_key(
    header: &str,
    prefix: &str,
) -> (
    Authentication,
    std::collections::HashMap<String, management::CredentialField>,
) {
    (
        Authentication::HeaderCredentials(management::HeaderCredentialsAuthentication {
            headers: std::collections::HashMap::from([(
                header.into(),
                management::HeaderBinding {
                    source: Some(management::header_binding::Source::Field(
                        management::FieldHeaderValue {
                            field: "key".into(),
                            prefix: prefix.into(),
                        },
                    )),
                },
            )]),
        }),
        std::collections::HashMap::from([(
            "key".into(),
            management::CredentialField {
                label: "API key".into(),
                description: String::new(),
                sensitive: true,
            },
        )]),
    )
}

fn oauth(
    configuration: &management::ProviderConfiguration,
) -> &management::OAuthAuthorizationCodeAuthentication {
    match configuration.authentication.as_ref() {
        Some(Authentication::OauthAuthorizationCode(settings)) => settings,
        other => panic!("expected OAuth settings, got {other:?}"),
    }
}

fn oauth_mut(
    configuration: &mut management::ProviderConfiguration,
) -> &mut management::OAuthAuthorizationCodeAuthentication {
    match configuration.authentication.as_mut() {
        Some(Authentication::OauthAuthorizationCode(settings)) => settings,
        other => panic!("expected OAuth settings, got {other:?}"),
    }
}

trait ConfiguredSetupFixture {
    async fn create_configured_setup(
        &mut self,
        input: tonic::Request<management::CreateProviderSetupRequest>,
    ) -> Result<tonic::Response<management::ProviderSetup>, tonic::Status>;
}

impl ConfiguredSetupFixture for SetupClient {
    async fn create_configured_setup(
        &mut self,
        input: tonic::Request<management::CreateProviderSetupRequest>,
    ) -> Result<tonic::Response<management::ProviderSetup>, tonic::Status> {
        let mut input = input;
        let credential = std::mem::take(&mut input.get_mut().discovery_credential);
        let metadata = input.metadata().clone();
        let response = self.create_provider_setup(input).await?;
        let setup = response.get_ref();
        if setup.state() != management::ProviderSetupState::AwaitingConfiguration {
            return Ok(response);
        }
        let review = setup.configuration.as_ref().expect("configuration review");
        let mut configuration = review
            .confirmed
            .clone()
            .or_else(|| review.suggested.clone())
            .unwrap_or_default();
        if !credential.is_empty() {
            configuration.authentication = Some(Authentication::Bearer(()));
        } else if configuration.authentication.is_none() {
            configuration.authentication = Some(Authentication::None(()));
        }
        let mut confirmation =
            tonic::Request::new(management::ConfirmProviderSetupConfigurationRequest {
                redirect_uri: "https://reef.example/callback".into(),
                name: setup.name.clone(),
                expected_revision: review.revision,
                configuration: Some(configuration),
            });
        *confirmation.metadata_mut() = metadata.clone();
        let confirmed = self
            .confirm_provider_setup_configuration(confirmation)
            .await?;
        let errors = &confirmed.get_ref().configuration.as_ref().unwrap().errors;
        if !errors.is_empty() {
            // A Status, not a panic here, so the failure points at the calling test.
            return Err(tonic::Status::failed_precondition(format!(
                "suggested configuration was not accepted: {errors:?}"
            )));
        }
        if !credential.is_empty() {
            let mut authorization =
                tonic::Request::new(management::AuthorizeProviderSetupRequest {
                    configuration_revision: confirmed
                        .get_ref()
                        .configuration
                        .as_ref()
                        .unwrap()
                        .revision,
                    name: confirmed.get_ref().name.clone(),
                    credential_values: token_values(&credential),
                });
            *authorization.metadata_mut() = metadata;
            return self.authorize_provider_setup(authorization).await;
        }
        Ok(confirmed)
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table confirms and reads back every authentication method"
)]
async fn each_authentication_method_reads_back_with_the_settings_it_was_confirmed_with() {
    let fixture = Fixture::new().await;
    let token = fixture.token(&fixture.owner, "reef");
    let mut client = fixture.setups();
    let setup = client
        .create_provider_setup(request(
            setup_request(
                format!("workspaces/{}", fixture.workspace.id()),
                Input::OpenapiDocument(openapi_document(true).into()),
            ),
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let oauth_settings = management::OAuthAuthorizationCodeAuthentication {
        scope_format: management::OAuthScopeFormat::Space as i32,
        authorization_url: "https://login.example.test/authorize".into(),
        token_url: "https://login.example.test/token".into(),
        scopes: vec!["read".into(), "write".into()],
        application: Some(management::OAuthApplication {
            mode: Some(management::o_auth_application::Mode::ClientId(
                "manual-client".into(),
            )),
            ..Default::default()
        }),
        oauth_client_id: String::new(),
    };
    let device_settings = management::OAuthDeviceCodeAuthentication {
        device_authorization_url: "https://login.example.test/device".into(),
        token_url: "https://login.example.test/token".into(),
        scopes: vec!["read".into()],
        scope_format: management::OAuthScopeFormat::Space as i32,
        application: oauth_settings.application.clone(),
        oauth_client_id: String::new(),
    };
    let (header_credentials, key_field) = header_key("authorization", "Token token=");
    for (method, fields) in [
        (
            Authentication::None(()),
            std::collections::HashMap::default(),
        ),
        (
            Authentication::Bearer(()),
            std::collections::HashMap::default(),
        ),
        (header_credentials, key_field.clone()),
        // Mailgun's shape: the username is the constant `api`.
        (
            Authentication::BasicAuth(management::BasicAuthentication {
                username: Some(management::CredentialSource {
                    source: Some(management::credential_source::Source::Literal("api".into())),
                }),
                password: Some(management::CredentialSource {
                    source: Some(management::credential_source::Source::Field("key".into())),
                }),
            }),
            key_field.clone(),
        ),
        // A key beside a constant the service requires.
        (
            Authentication::QueryCredentials(management::QueryCredentialsAuthentication {
                parameters: std::collections::HashMap::from([
                    (
                        "api_key".into(),
                        management::CredentialSource {
                            source: Some(management::credential_source::Source::Field(
                                "key".into(),
                            )),
                        },
                    ),
                    (
                        "format".into(),
                        management::CredentialSource {
                            source: Some(management::credential_source::Source::Literal(
                                "json".into(),
                            )),
                        },
                    ),
                ]),
            }),
            key_field,
        ),
        (
            Authentication::OauthAuthorizationCode(oauth_settings.clone()),
            std::collections::HashMap::default(),
        ),
        (
            Authentication::OauthDeviceCode(device_settings.clone()),
            std::collections::HashMap::default(),
        ),
    ] {
        let current = client
            .get_provider_setup(request(
                management::GetProviderSetupRequest {
                    name: setup.name.clone(),
                },
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        let review = current.configuration.unwrap();
        let mut values = review.suggested.unwrap();
        values.authentication = Some(method.clone());
        values.credential_fields = fields.clone();
        let confirmed = client
            .confirm_provider_setup_configuration(request(
                management::ConfirmProviderSetupConfigurationRequest {
                    redirect_uri: "https://reef.example/callback".into(),
                    name: setup.name.clone(),
                    expected_revision: review.revision,
                    configuration: Some(values),
                },
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        let prompts = confirmed.credential_fields.clone();
        let review = confirmed.configuration.unwrap();
        assert!(review.errors.is_empty(), "{method:?}: {:?}", review.errors);
        let readback = review.confirmed.unwrap();
        // Both OAuth methods read back the resolved client beside the settings sent.
        let manual = Some(management::o_auth_application::Mode::ClientId(
            "manual-client".into(),
        ));
        match (&method, readback.authentication.as_ref()) {
            (
                Authentication::OauthAuthorizationCode(_),
                Some(Authentication::OauthAuthorizationCode(settings)),
            ) => {
                assert_eq!(settings.authorization_url, oauth_settings.authorization_url);
                assert_eq!(settings.token_url, oauth_settings.token_url);
                assert_eq!(settings.scopes, oauth_settings.scopes);
                assert_eq!(settings.oauth_client_id, "manual-client");
                assert_eq!(
                    settings.application.as_ref().and_then(|a| a.mode.clone()),
                    manual
                );
            }
            (
                Authentication::OauthDeviceCode(_),
                Some(Authentication::OauthDeviceCode(settings)),
            ) => {
                assert_eq!(
                    settings.device_authorization_url,
                    device_settings.device_authorization_url
                );
                assert_eq!(settings.token_url, device_settings.token_url);
                assert_eq!(settings.scopes, device_settings.scopes);
                assert_eq!(settings.oauth_client_id, "manual-client");
                assert_eq!(
                    settings.application.as_ref().and_then(|a| a.mode.clone()),
                    manual
                );
            }
            _ => {
                assert_eq!(readback.authentication, Some(method));
                assert_eq!(readback.credential_fields, fields);
            }
        }
        if let Some(
            Authentication::HeaderCredentials(_)
            | Authentication::BasicAuth(_)
            | Authentication::QueryCredentials(_),
        ) = &readback.authentication
        {
            assert_eq!(
                prompts.get("key").map(|field| field.label.as_str()),
                Some("API key"),
                "secret entry asks for each field as the owner presents it"
            );
        }
    }
    // Request headers configure an MCP connection; any other input refuses them.
    let refused = client
        .create_provider_setup(request(
            management::CreateProviderSetupRequest {
                request_headers: std::collections::HashMap::from([(
                    "x-tenant".into(),
                    "acme".into(),
                )]),
                ..setup_request(
                    format!("workspaces/{}", fixture.workspace.id()),
                    Input::OpenapiDocument(openapi_document(true).into()),
                )
            },
            &token,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(
        (refused.code(), error_reason(&refused).as_str()),
        (tonic::Code::InvalidArgument, "INVALID_ARGUMENT")
    );
}

/// coral-provider owns the device flow (`setup_oauth_tests/device.rs`). Over the
/// wire, the Setup shows the code a person enters and never the private device code.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "a device-code Setup needs its endpoints, configuration, and start before the code shows"
)]
async fn device_code_authorization_projects_the_user_code_but_never_the_device_code() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(wiremock::matchers::path("/device"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code": "private-device-canary", "user_code": "ABCD-EFGH",
            "verification_uri": "http://device.fixture.test/activate",
            "expires_in": 600, "interval": 5
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(wiremock::matchers::path("/token"))
        .respond_with(
            ResponseTemplate::new(400).set_body_json(json!({"error": "authorization_pending"})),
        )
        .mount(&server)
        .await;
    let mut setups = f.setups();
    let setup = setups
        .create_provider_setup(request(
            setup_request(
                format!("workspaces/{}", f.workspace.id()),
                Input::OpenapiDocument(openapi_document(true).into()),
            ),
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let review = setup.configuration.unwrap();
    let mut values = review.suggested.unwrap();
    values.authentication = Some(Authentication::OauthDeviceCode(
        management::OAuthDeviceCodeAuthentication {
            device_authorization_url: format!("{}/device", server.uri()),
            token_url: format!("{}/token", server.uri()),
            scopes: vec!["read".into()],
            application: Some(management::OAuthApplication {
                mode: Some(management::o_auth_application::Mode::ClientId(
                    "manual-client".into(),
                )),
                ..Default::default()
            }),
            ..Default::default()
        },
    ));
    let confirmed = setups
        .confirm_provider_setup_configuration(request(
            management::ConfirmProviderSetupConfigurationRequest {
                redirect_uri: "https://reef.example/callback".into(),
                name: setup.name.clone(),
                expected_revision: review.revision,
                configuration: Some(values),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let configuration = confirmed.configuration.unwrap();
    assert!(
        configuration.errors.is_empty(),
        "{:?}",
        configuration.errors
    );
    setups
        .start_provider_setup_authorization(request(
            management::StartProviderSetupAuthorizationRequest {
                name: setup.name.clone(),
                configuration_revision: configuration.revision,
                redirect_uri: "https://reef.example/callback".into(),
                ..Default::default()
            },
            &token,
            "reef",
        ))
        .await
        .unwrap();
    // The device request runs in the background; Setups have no change stream.
    let pending = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let current = setups
                .get_provider_setup(request(
                    management::GetProviderSetupRequest {
                        name: setup.name.clone(),
                    },
                    &token,
                    "reef",
                ))
                .await
                .unwrap()
                .into_inner();
            if current
                .authorization
                .as_ref()
                .and_then(|authorization| authorization.user_code.as_deref())
                .is_some_and(|code| !code.is_empty())
            {
                break current;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the device request completes");
    let authorization = pending.authorization.as_ref().unwrap();
    assert_eq!(authorization.user_code.as_deref(), Some("ABCD-EFGH"));
    assert_eq!(
        authorization.state(),
        management::SetupAuthorizationState::Pending
    );
    assert!(!format!("{pending:?}").contains("private-device-canary"));
}

/// coral-provider owns the revision rule
/// (`stale_secret_submission_cannot_authorize_replacement_configuration`).
#[tokio::test]
async fn stale_secret_form_is_rejected_before_sealing_against_new_configuration() {
    let fixture = Fixture::new().await;
    let token = fixture.token(&fixture.owner, "reef");
    let mut client = fixture.setups();
    let setup = client
        .create_configured_setup(request(
            setup_request(
                format!("workspaces/{}", fixture.workspace.id()),
                Input::OpenapiDocument(openapi_document(true).into()),
            ),
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let review = setup.configuration.unwrap();
    let mut values = review.confirmed.unwrap();
    let (header_credentials, key_field) = header_key("x-api-key", "");
    values.authentication = Some(header_credentials);
    values.credential_fields = key_field;
    client
        .confirm_provider_setup_configuration(request(
            management::ConfirmProviderSetupConfigurationRequest {
                redirect_uri: "https://reef.example/callback".into(),
                name: setup.name.clone(),
                expected_revision: review.revision,
                configuration: Some(values),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap();
    let error = client
        .authorize_provider_setup(request(
            management::AuthorizeProviderSetupRequest {
                name: setup.name,
                credential_values: token_values(TOKEN),
                configuration_revision: review.revision,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(
        (error.code(), error_reason(&error).as_str()),
        (tonic::Code::Aborted, "ETAG_MISMATCH")
    );
    assert!(!format!("{error:?}").contains(TOKEN));
}

const TOKEN: &str = "grpc-bearer-canary";

fn openapi_document(security: bool) -> String {
    let mut document = json!({
        "openapi": "3.1.0", "info": {"title": "Weather", "version": "1"},
        "servers": [{"url": "https://weather.example.test"}],
        "paths": {"/forecast": {"get": {"operationId": "getForecast",
            "responses": {"200": {"description": "Forecast"}}}}}
    });
    if security {
        document["security"] = json!([{"bearer": []}]);
        document["components"] =
            json!({"securitySchemes": {"bearer": {"type": "http", "scheme": "bearer"}}});
    }
    document["paths"]["/forecast"]["get"]["tags"] = json!(["forecast"]);
    document["paths"]["/forecast"]["get"]["summary"] = json!("Get forecast");
    document["paths"]["/forecast"]["get"]["description"] =
        json!("Get the **daily forecast**.\n\nResults use the location's time zone.");
    document.to_string()
}

fn envelope(request: &Request, result: &Value) -> ResponseTemplate {
    let request: Value = request.body_json().expect("json-rpc");
    ResponseTemplate::new(200).set_body_json(json!({
        "jsonrpc": "2.0", "id": request["id"], "result": result
    }))
}

async fn mcp_server(token: &str) -> MockServer {
    let server = MockServer::start().await;
    let bearer = format!("Bearer {token}");
    Mock::given(method("POST"))
        .and(header("authorization", bearer.as_str()))
        .and(body_partial_json(json!({"method": "initialize"})))
        .respond_with(|request: &Request| {
            envelope(
                request,
                &json!({"protocolVersion": "2025-11-25", "capabilities": {"tools": {}},
                        "serverInfo": {"name": "fixture", "title": "Fixture MCP", "version": "1"}}),
            )
        })
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(header("authorization", bearer.as_str()))
        .and(body_partial_json(
            json!({"method": "notifications/initialized"}),
        ))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(header("authorization", bearer.as_str()))
        .and(body_partial_json(json!({"method": "tools/list"})))
        .respond_with(|request: &Request| {
            envelope(
                request,
                &json!({"tools": [{"name": "lookup", "inputSchema": {"type": "object"},
                                   "annotations": {"readOnlyHint": true,
                                                   "openWorldHint": false}}]}),
            )
        })
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401).insert_header("www-authenticate", "Bearer"))
        .mount(&server)
        .await;
    server
}

fn route(kind: management::credential_route::Kind) -> management::CredentialRoute {
    management::CredentialRoute { kind: kind.into() }
}

#[tokio::test]
async fn openapi_upload_accepts_64_mib_and_reports_the_document_limit() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let parent = format!("workspaces/{}", f.workspace.id());
    let mut client = f.setups();
    let mut document = openapi_document(false);
    document.extend(std::iter::repeat_n(' ', 64 * 1024 * 1024 - document.len()));
    let setup = client
        .create_configured_setup(request(
            setup_request(
                parent.clone(),
                Input::OpenapiDocument(document.clone().into()),
            ),
            &token,
            "reef",
        ))
        .await
        .expect("the full 64 MiB document must fit through the native gRPC listener")
        .into_inner();
    assert_eq!(setup.state(), management::ProviderSetupState::Ready);
    document.push(' ');
    let error = client
        .create_configured_setup(request(
            setup_request(parent, Input::OpenapiDocument(document.into())),
            &token,
            "reef",
        ))
        .await
        .expect_err("one extra byte exceeds the document budget, not the RPC envelope budget");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(error.message(), "OPENAPI_DOCUMENT_TOO_LARGE");
}

/// coral-provider owns replay, consumption and request identity
/// (`publication_replay_returns_the_committed_result_without_a_second_account`,
/// `tests/workspace_provider.rs`). This checks the Setup and Publication projection.
#[tokio::test]
async fn workspace_owner_publishes_an_inline_document_and_a_reused_request_id_conflicts() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let parent = format!("workspaces/{}", f.workspace.id());
    let mut setups = f.setups();
    let setup = setups
        .create_configured_setup(request(
            setup_request(
                parent.clone(),
                Input::OpenapiDocument(openapi_document(false).into()),
            ),
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(setup.name.starts_with(&format!("{parent}/providerSetups/")));
    assert_eq!(setup.state(), management::ProviderSetupState::Ready);
    assert_eq!(
        setup.input_kind(),
        management::ProviderSetupInputKind::Openapi
    );
    assert_eq!(
        setup.credential_method(),
        management::CredentialMethod::None
    );
    // Coral says which routes suit the source and that nothing can be connected.
    assert_eq!(
        setup.allowed_credential_routes,
        vec![route(management::credential_route::Kind::None)]
    );
    assert!(!setup.connect_available && !setup.connect_later_available);
    assert!(!setup.authorized && setup.create_time.is_some());
    let fetched = setups
        .get_provider_setup(request(
            management::GetProviderSetupRequest {
                name: setup.name.clone(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched, setup);

    let mut providers = f.providers();
    let publish = management::PublishProviderRequest {
        configuration_revision: 1,
        allow_incompatible_configuration: true,
        parent: parent.clone(),
        setup: setup.name.clone(),
        request_id: "b0000000-0000-4000-8000-000000000001".into(),
        namespace: "weather".into(),
        credential_route: Some(route(management::credential_route::Kind::None)),
        ..Default::default()
    };
    let publication = providers
        .publish_provider(request(publish.clone(), &token, "reef"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        publication.outcome(),
        management::ProviderPublicationOutcome::Succeeded
    );
    assert!(
        publication
            .provider
            .starts_with(&format!("{parent}/providers/"))
    );
    assert!(
        publication
            .name
            .starts_with(&format!("{parent}/providerPublications/"))
    );
    let consumed = setups
        .get_provider_setup(request(
            management::GetProviderSetupRequest { name: setup.name },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(consumed.state(), management::ProviderSetupState::Consumed);
    assert_eq!(consumed.publication, publication.name);
    let conflict = providers
        .publish_provider(request(
            management::PublishProviderRequest {
                namespace: "other".into(),
                ..publish
            },
            &token,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(
        (conflict.code(), error_reason(&conflict).as_str()),
        (tonic::Code::AlreadyExists, "REQUEST_ID_CONFLICT")
    );
}

#[tokio::test]
async fn members_cannot_prepare_setups_and_malformed_names_are_safe() {
    let f = Fixture::new().await;
    let member = f.token(&f.member, "reef");
    let mut setups = f.setups();
    for (parent, code, reason) in [
        (
            format!("workspaces/{}", f.workspace.id()),
            tonic::Code::PermissionDenied,
            "PERMISSION_DENIED",
        ),
        (
            "organization".into(),
            tonic::Code::InvalidArgument,
            "INVALID_ARGUMENT",
        ),
    ] {
        let error = setups
            .create_configured_setup(request(
                setup_request(
                    parent,
                    Input::OpenapiDocument(openapi_document(false).into()),
                ),
                &member,
                "reef",
            ))
            .await
            .unwrap_err();
        assert_eq!(
            (error.code(), error_reason(&error).as_str()),
            (code, reason)
        );
    }
    let owner = f.token(&f.owner, "reef");
    let absent = setups
        .get_provider_setup(request(
            management::GetProviderSetupRequest {
                name: format!(
                    "workspaces/{}/providerSetups/c0000000-0000-4000-8000-000000000001",
                    f.workspace.id()
                ),
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(absent.code(), tonic::Code::NotFound);
    let malformed = setups
        .get_provider_setup(request(
            management::GetProviderSetupRequest {
                name: "providerSetups/../secret".into(),
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(malformed.code(), tonic::Code::InvalidArgument);
    assert_eq!(error_reason(&malformed), "INVALID_ARGUMENT");
    let missing_input = setups
        .create_configured_setup(request(
            management::CreateProviderSetupRequest {
                parent: format!("workspaces/{}", f.workspace.id()),
                ..Default::default()
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(missing_input.code(), tonic::Code::InvalidArgument);
}

/// coral-provider owns when configuration must be confirmed
/// (`anonymous_mcp_catalog_still_requires_explicit_configuration_confirmation`,
/// `mcp_without_a_challenge_offers_explicit_bearer_without_fabricating_oauth`).
/// This checks the review projection, the adapter's discovery-credential rule,
/// and the refused authorization.
#[tokio::test]
async fn configuration_review_precedes_credentials_and_refuses_early_authorization() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let server = mcp_server(TOKEN).await;
    let parent = format!("workspaces/{}", f.workspace.id());
    let mut setups = f.setups();
    let input = || Input::McpUrl(format!("{}/mcp", server.uri()));
    let rejected = setups
        .create_provider_setup(request(
            management::CreateProviderSetupRequest {
                discovery_credential: TOKEN.into(),
                ..setup_request(parent.clone(), input())
            },
            &token,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(rejected.code(), tonic::Code::InvalidArgument);
    assert!(!format!("{rejected:?}").contains(TOKEN));
    let setup = setups
        .create_provider_setup(request(setup_request(parent, input()), &token, "reef"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        setup.state(),
        management::ProviderSetupState::AwaitingConfiguration
    );
    assert_eq!(setup.input_kind(), management::ProviderSetupInputKind::Mcp);
    let review = setup.configuration.as_ref().unwrap();
    assert!(!review.configuration_confirmed);
    assert!(review.confirmed.is_none());
    assert!(!setup.authorized);
    assert_eq!(
        review.authentication_methods,
        vec![i32::from(management::CredentialMethod::Bearer)]
    );
    assert!(review.discovery_authorization_required);
    assert!(!review.automatic_client_available);
    // No route suits a configuration nobody has confirmed.
    assert!(setup.allowed_credential_routes.is_empty());
    assert!(review.issuer.is_none());
    assert!(review.resource.is_none());
    let refused = setups
        .authorize_provider_setup(request(
            management::AuthorizeProviderSetupRequest {
                configuration_revision: 1,
                name: setup.name,
                credential_values: token_values(TOKEN),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(
        (refused.code(), error_reason(&refused).as_str()),
        (tonic::Code::FailedPrecondition, "SETUP_NOT_READY")
    );
    assert!(!format!("{refused:?}").contains(TOKEN));
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one native-wire journey covers saved review, compatible confirmation, interruption acceptance and exact replay"
)]
async fn update_projects_saved_differences_and_requires_exact_interruption_acceptance() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let parent = format!("workspaces/{}", f.workspace.id());
    let mut setups = f.setups();
    let mut providers = f.providers();
    let first = setups
        .create_configured_setup(request(
            setup_request(
                parent.clone(),
                Input::OpenapiDocument(openapi_document(true).into()),
            ),
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        first.configuration.as_ref().unwrap().operations,
        vec![management::ReviewedProviderOperation {
            key: "getForecast".into(),
            relative_path: "getForecast".into(),
            summary: "Get forecast".into(),
            tags: vec!["forecast".into()],
        }],
        "configuration review exposes the previewed operations before publication"
    );
    let published = providers
        .publish_provider(request(
            management::PublishProviderRequest {
                configuration_revision: 1,
                parent: parent.clone(),
                setup: first.name,
                request_id: "c0000000-0000-4000-8000-000000000001".into(),
                namespace: "review".into(),
                credential_route: Some(route(management::credential_route::Kind::Caller)),
                ..Default::default()
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let provider = providers
        .get_provider(request(
            management::GetProviderRequest {
                name: published.provider,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let mut replacement: Value = serde_json::from_str(&openapi_document(false)).unwrap();
    replacement["security"] = json!([]);
    let setup = setups
        .create_provider_setup(request(
            management::CreateProviderSetupRequest {
                provider: provider.name.clone(),
                ..setup_request(
                    parent.clone(),
                    Input::OpenapiDocument(replacement.to_string().into()),
                )
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let review = setup.configuration.as_ref().unwrap();
    assert!(review.updating);
    assert!(!review.configuration_confirmed);
    assert_eq!(review.interrupts_access, None);
    assert!(!review.differences.is_empty());
    let mut choices = review.confirmed.clone().unwrap();
    assert_eq!(choices.authentication, Some(Authentication::Bearer(())));
    let compatible = setups
        .confirm_provider_setup_configuration(request(
            management::ConfirmProviderSetupConfigurationRequest {
                redirect_uri: "https://reef.example/callback".into(),
                name: setup.name.clone(),
                expected_revision: review.revision,
                configuration: Some(choices.clone()),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        compatible.configuration.as_ref().unwrap().interrupts_access,
        Some(false)
    );
    choices.authentication = Some(Authentication::None(()));
    let incompatible = setups
        .confirm_provider_setup_configuration(request(
            management::ConfirmProviderSetupConfigurationRequest {
                redirect_uri: "https://reef.example/callback".into(),
                name: setup.name.clone(),
                expected_revision: compatible.configuration.as_ref().unwrap().revision,
                configuration: Some(choices),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        incompatible
            .configuration
            .as_ref()
            .unwrap()
            .interrupts_access,
        Some(true)
    );
    let mut update = management::PublishProviderRequest {
        configuration_revision: incompatible.configuration.as_ref().unwrap().revision,
        parent,
        setup: setup.name,
        provider: provider.name,
        etag: provider.etag,
        request_id: "c0000000-0000-4000-8000-000000000002".into(),
        // A credential-free source takes only the `none` route.
        credential_route: Some(route(management::credential_route::Kind::None)),
        ..Default::default()
    };
    let refused = providers
        .publish_provider(request(update.clone(), &token, "reef"))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    update.allow_incompatible_configuration = true;
    let accepted = providers
        .publish_provider(request(update.clone(), &token, "reef"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        accepted.outcome(),
        management::ProviderPublicationOutcome::Succeeded
    );
    assert_eq!(
        providers
            .publish_provider(request(update.clone(), &token, "reef"))
            .await
            .unwrap()
            .into_inner(),
        accepted
    );
    update.allow_incompatible_configuration = false;
    providers
        .publish_provider(request(update, &token, "reef"))
        .await
        .unwrap_err();
}

/// Publishes the MCP fixture with its discovery credential and returns the Provider name.
async fn published_mcp(f: &Fixture, token: &str, server: &MockServer) -> String {
    let parent = format!("workspaces/{}", f.workspace.id());
    let setup = f
        .setups()
        .create_configured_setup(request(
            management::CreateProviderSetupRequest {
                discovery_credential: TOKEN.into(),
                ..setup_request(
                    parent.clone(),
                    Input::McpUrl(format!("{}/mcp", server.uri())),
                )
            },
            token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    f.providers()
        .publish_provider(request(
            management::PublishProviderRequest {
                configuration_revision: 1,
                allow_incompatible_configuration: true,
                parent,
                setup: setup.name,
                request_id: "b0000000-0000-4000-8000-000000000003".into(),
                namespace: "fixture".into(),
                account_grant: setup.provisional_grant_id,
                credential_route: Some(route(management::credential_route::Kind::Fixed)),
                provider: String::new(),
                etag: String::new(),
            },
            token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner()
        .provider
}

#[tokio::test]
async fn list_operations_projects_exactly_the_declared_hints() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let server = mcp_server(TOKEN).await;
    let provider = published_mcp(&f, &token, &server).await;
    let page = f
        .providers()
        .list_operations(request(
            management::ListOperationsRequest {
                parent: provider,
                page_size: 0,
                page_token: String::new(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let lookup = page
        .operations
        .iter()
        .find(|operation| operation.relative_path == "lookup")
        .expect("lookup Operation");
    // A declared false stays false and an undeclared hint stays unset.
    assert_eq!(
        lookup.hints,
        Some(management::OperationHints {
            read_only: Some(true),
            destructive: None,
            idempotent: None,
            open_world: Some(false),
        })
    );
}

#[tokio::test]
async fn mcp_url_with_a_discovery_credential_publishes_a_fixed_route_without_echoing_it() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let server = mcp_server(TOKEN).await;
    let parent = format!("workspaces/{}", f.workspace.id());
    let mut setups = f.setups();
    let setup = setups
        .create_configured_setup(request(
            management::CreateProviderSetupRequest {
                discovery_credential: TOKEN.into(),
                ..setup_request(
                    parent.clone(),
                    Input::McpUrl(format!("{}/mcp", server.uri())),
                )
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(setup.state(), management::ProviderSetupState::Ready);
    assert_eq!(setup.input_kind(), management::ProviderSetupInputKind::Mcp);
    assert_eq!(
        setup.credential_method(),
        management::CredentialMethod::Bearer
    );
    assert!(setup.authorized);
    assert!(!setup.provisional_grant_id.is_empty());
    assert!(setup.credential_expire_time.is_some());
    assert!(!format!("{setup:?}").contains(TOKEN));
    let mut providers = f.providers();
    let publication = providers
        .publish_provider(request(
            management::PublishProviderRequest {
                configuration_revision: 1,
                allow_incompatible_configuration: true,
                parent,
                setup: setup.name,
                request_id: "b0000000-0000-4000-8000-000000000002".into(),
                namespace: "fixture".into(),
                account_grant: setup.provisional_grant_id,
                credential_route: Some(route(management::credential_route::Kind::Fixed)),
                provider: String::new(),
                etag: String::new(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        publication.outcome(),
        management::ProviderPublicationOutcome::Succeeded
    );
    assert!(!format!("{publication:?}").contains(TOKEN));
    // A refused credential returns safe review progress, never credential material.
    let refused = setups
        .create_configured_setup(request(
            management::CreateProviderSetupRequest {
                discovery_credential: "wrong-canary".into(),
                ..setup_request(
                    format!("workspaces/{}", f.workspace.id()),
                    Input::McpUrl(format!("{}/mcp", server.uri())),
                )
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!refused.authorized);
    assert_eq!(
        refused.state(),
        management::ProviderSetupState::AwaitingConfiguration
    );
    assert!(
        !refused
            .configuration
            .as_ref()
            .unwrap()
            .configuration_confirmed
    );
    assert!(!format!("{refused:?}").contains("wrong-canary"));
}

/// coral-provider owns which choice retains which Account
/// (`publication/tests/creation.rs`, `setup_oauth_tests/retention.rs`). This
/// checks how each retained Account reads back over the wire.
#[tokio::test]
async fn workspace_owner_publishes_with_explicit_shared_or_self_account_retention() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let parent = format!("workspaces/{}", f.workspace.id());
    for (namespace, kind, scope) in [
        (
            "account_shared",
            management::credential_route::Kind::Fixed,
            management::ProviderAccountScope::Shared,
        ),
        (
            "account_self",
            management::credential_route::Kind::Caller,
            management::ProviderAccountScope::User,
        ),
    ] {
        let mut setups = f.setups();
        let setup = setups
            .create_configured_setup(request(
                setup_request(
                    parent.clone(),
                    Input::OpenapiDocument(openapi_document(true).into()),
                ),
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        let authorized = setups
            .authorize_provider_setup(request(
                management::AuthorizeProviderSetupRequest {
                    configuration_revision: 1,
                    name: setup.name.clone(),
                    credential_values: token_values(TOKEN),
                },
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(authorized.authorized);
        assert!(!authorized.provisional_grant_id.is_empty());
        assert!(!format!("{authorized:?}").contains(TOKEN));
        let publication = f
            .providers()
            .publish_provider(request(
                management::PublishProviderRequest {
                    configuration_revision: 1,
                    allow_incompatible_configuration: true,
                    parent: parent.clone(),
                    setup: setup.name,
                    request_id: uuid::Uuid::new_v4().to_string(),
                    namespace: namespace.into(),
                    account_grant: authorized.provisional_grant_id.clone(),
                    credential_route: Some(route(kind)),
                    ..Default::default()
                },
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            publication.outcome(),
            management::ProviderPublicationOutcome::Succeeded
        );
        let account = f
            .accounts()
            .get_provider_account(request(
                management::GetProviderAccountRequest {
                    parent: parent.clone(),
                    provider: publication.provider,
                    scope: scope.into(),
                    agent: String::new(),
                },
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner()
            .account
            .unwrap();
        assert_eq!(
            account.id,
            authorized
                .provisional_grant_id
                .split('/')
                .next_back()
                .unwrap(),
            "{scope:?}"
        );
        assert_eq!(account.state(), management::ProviderAccountState::Ready);
        assert!(account.connected_time.is_some());
        assert_eq!(account.configured_by_user, f.owner.user_id().to_string());
        assert!(!format!("{account:?}").contains(TOKEN));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_secret_authorizations_return_their_own_grants() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let parent = format!("workspaces/{}", f.workspace.id());
    let mut setups = f.setups();
    let setup = setups
        .create_configured_setup(request(
            setup_request(
                parent,
                Input::OpenapiDocument(openapi_document(true).into()),
            ),
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let mut requests = tokio::task::JoinSet::new();
    for index in 0..16 {
        let mut client = setups.clone();
        let token = token.clone();
        let name = setup.name.clone();
        requests.spawn(async move {
            client
                .authorize_provider_setup(request(
                    management::AuthorizeProviderSetupRequest {
                        name,
                        configuration_revision: 1,
                        credential_values: std::collections::HashMap::from([(
                            "token".to_string(),
                            format!("private-submission-{index}"),
                        )]),
                    },
                    &token,
                    "reef",
                ))
                .await
                .unwrap()
                .into_inner()
        });
    }
    let mut grants = std::collections::BTreeSet::new();
    while let Some(result) = requests.join_next().await {
        let receipt = result.unwrap();
        assert!(receipt.authorized);
        assert!(!receipt.provisional_grant_id.is_empty());
        assert!(
            grants.insert(receipt.provisional_grant_id),
            "a command must not substitute a concurrent command's grant during response inspection"
        );
    }
    assert_eq!(grants.len(), 16);
    let current = setups
        .get_provider_setup(request(
            management::GetProviderSetupRequest { name: setup.name },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(grants.contains(&current.provisional_grant_id));
}

async fn publish_workspace_document(
    f: &Fixture,
    token: &str,
    document: String,
    parent: &str,
    request_id: &str,
) -> management::ProviderPublication {
    let mut setups = f.setups();
    let setup = setups
        .create_configured_setup(request(
            setup_request(parent, Input::OpenapiDocument(document.into())),
            token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let mut providers = f.providers();
    providers
        .publish_provider(request(
            management::PublishProviderRequest {
                configuration_revision: 1,
                allow_incompatible_configuration: true,
                parent: parent.into(),
                setup: setup.name,
                request_id: request_id.into(),
                namespace: "weather".into(),
                account_grant: String::new(),
                credential_route: Some(route(management::credential_route::Kind::None)),
                provider: String::new(),
                etag: String::new(),
            },
            token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner()
}

/// Measures the private reads Reef makes when opening a large Provider catalog.
#[tokio::test]
#[ignore = "manual providers-screen latency measurement"]
#[expect(
    clippy::print_stderr,
    reason = "manual benchmark reports measured read latency"
)]
async fn providers_screen_read_latency() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let mut document: Value = serde_json::from_str(&openapi_document(false)).unwrap();
    document["paths"] = (0..1229)
        .map(|index| {
            (
                format!("/operation{index}"),
                json!({"get": {
                    "operationId": format!("operation{index}"),
                    "responses": {"200": {"description": "Result"}}
                }}),
            )
        })
        .collect::<serde_json::Map<_, _>>()
        .into();
    let publication = publish_workspace_document(
        &f,
        &token,
        document.to_string(),
        &format!("workspaces/{}", f.workspace.id()),
        "b0000000-0000-4000-8000-000000000099",
    )
    .await;
    let mut providers = f.providers();
    let mut policies = f.policies();
    for _ in 0..3 {
        let start = std::time::Instant::now();
        let mut page_token = String::new();
        let mut operations = Vec::new();
        loop {
            let page = providers
                .list_operations(request(
                    management::ListOperationsRequest {
                        parent: publication.provider.clone(),
                        page_size: 100,
                        page_token,
                    },
                    &token,
                    "reef",
                ))
                .await
                .unwrap()
                .into_inner();
            operations.extend(page.operations.into_iter().map(|operation| operation.name));
            page_token = page.next_page_token;
            if page_token.is_empty() {
                break;
            }
        }
        let catalog = start.elapsed();
        let policy_start = std::time::Instant::now();
        for batch in operations.chunks(100) {
            let decisions = policies
                .get_operation_policies(request(
                    management::GetOperationPoliciesRequest {
                        parent: publication.provider.clone(),
                        operations: batch.to_vec(),
                    },
                    &token,
                    "reef",
                ))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(decisions.policies.len(), batch.len());
        }
        eprintln!(
            "Provider reads: catalog={catalog:?}, policies={:?}",
            policy_start.elapsed()
        );
    }
}

/// coral-policy owns defaults and explicit decisions
/// (`openapi_forecast_defaults_to_approval_and_owner_can_enable_it`). This checks
/// the Operation name encoding, the state projection, and one denial.
#[tokio::test]
async fn policy_service_projects_operation_decisions_and_refuses_members() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let publication = publish_workspace_document(
        &f,
        &token,
        openapi_document(false),
        &format!("workspaces/{}", f.workspace.id()),
        "b0000000-0000-4000-8000-000000000011",
    )
    .await;
    // The fixture's exact Operation Key is getForecast, encoded in the wire name.
    let operation = format!("{}/operations/Z2V0Rm9yZWNhc3Q", publication.provider);
    let mut policies = f.policies();
    let update = |access_state: management::OperationAccessState| {
        management::UpdateOperationPoliciesRequest {
            parent: publication.provider.clone(),
            policies: vec![management::OperationPolicy {
                operation: operation.clone(),
                access_state: access_state.into(),
                explicit: false,
            }],
        }
    };
    let denied = policies
        .update_operation_policies(request(
            update(management::OperationAccessState::Enabled),
            &f.token(&f.member, "reef"),
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(
        (denied.code(), error_reason(&denied).as_str()),
        (tonic::Code::PermissionDenied, "PERMISSION_DENIED")
    );
    policies
        .update_operation_policies(request(
            update(management::OperationAccessState::Enabled),
            &token,
            "reef",
        ))
        .await
        .unwrap();
    let updated = policies
        .get_operation_policies(request(
            management::GetOperationPoliciesRequest {
                parent: publication.provider.clone(),
                operations: vec![operation.clone()],
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        updated.policies,
        vec![management::OperationPolicy {
            operation,
            access_state: management::OperationAccessState::Enabled.into(),
            explicit: true,
        }]
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one route mutation journey verifies private reachability, owner authority and independent version evidence"
)]
async fn provider_route_mutation_requires_reef_user_owner_and_current_provider_etag() {
    let f = Fixture::new().await;
    let owner = f.token(&f.owner, "reef");
    let parent = format!("workspaces/{}", f.workspace.id());
    // A shared Account on a bearer source, so the route can move to `caller`.
    let mut setups = f.setups();
    let shared = setups
        .create_configured_setup(request(
            setup_request(
                parent.clone(),
                Input::OpenapiDocument(openapi_document(true).into()),
            ),
            &owner,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let authorized = setups
        .authorize_provider_setup(request(
            management::AuthorizeProviderSetupRequest {
                configuration_revision: 1,
                name: shared.name.clone(),
                credential_values: token_values(TOKEN),
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let mut providers = f.providers();
    let publication = providers
        .publish_provider(request(
            management::PublishProviderRequest {
                configuration_revision: 1,
                parent: parent.clone(),
                setup: shared.name,
                request_id: "b0000000-0000-4000-8000-000000000012".into(),
                namespace: "weather".into(),
                account_grant: authorized.provisional_grant_id,
                credential_route: Some(route(management::credential_route::Kind::Fixed)),
                ..Default::default()
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let original = providers
        .get_provider(request(
            management::GetProviderRequest {
                name: publication.provider.clone(),
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let mut setups = f.setups();
    let setup = setups
        .create_configured_setup(request(
            management::CreateProviderSetupRequest {
                provider: original.name.clone(),
                ..setup_request(parent.clone(), Input::Current(()))
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let command = management::PublishProviderRequest {
        parent,
        provider: original.name.clone(),
        setup: setup.name,
        request_id: uuid::Uuid::new_v4().to_string(),
        etag: original.etag.clone(),
        credential_route: Some(route(management::credential_route::Kind::Caller)),
        configuration_revision: setup.configuration.unwrap().revision,
        allow_incompatible_configuration: true,
        ..Default::default()
    };
    let agent = f
        .workspaces
        .create_agent(
            &super::change(&f.owner),
            f.workspace.id(),
            coral_workspace::AgentDisplayName::parse("route agent").unwrap(),
            None,
        )
        .await
        .unwrap();
    for (token, audience, code) in [
        (
            f.token(&f.owner, "mcp"),
            "mcp",
            tonic::Code::PermissionDenied,
        ),
        (
            f.token(&f.member, "reef"),
            "reef",
            tonic::Code::PermissionDenied,
        ),
        (
            agent.credential.expose().to_owned(),
            "reef",
            tonic::Code::Unauthenticated,
        ),
    ] {
        let denied = providers
            .publish_provider(request(command.clone(), &token, audience))
            .await
            .unwrap_err();
        assert_eq!(denied.code(), code);
    }
    let stale = providers
        .publish_provider(request(
            management::PublishProviderRequest {
                etag: uuid::Uuid::new_v4().to_string(),
                ..command.clone()
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), tonic::Code::Aborted);
    providers
        .publish_provider(request(command.clone(), &owner, "reef"))
        .await
        .unwrap();
    let updated = providers
        .get_provider(request(
            management::GetProviderRequest {
                name: original.name.clone(),
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(updated.name, original.name);
    assert_eq!(updated.namespace, original.namespace);
    assert_eq!(updated.display_name, original.display_name);
    assert_eq!(
        updated.credential_route.as_ref().unwrap().kind(),
        management::credential_route::Kind::Caller
    );
    assert_eq!(
        updated.account_setting_version,
        original.account_setting_version + 1
    );
    assert_ne!(updated.etag, original.etag);
    providers
        .publish_provider(request(
            management::PublishProviderRequest {
                etag: updated.etag.clone(),
                ..command
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap();
    let unchanged = providers
        .get_provider(request(
            management::GetProviderRequest {
                name: original.name,
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(unchanged, updated);
}

/// Publishes the weather document in the fixture Workspace and reads it back.
async fn published_weather(f: &Fixture, token: &str) -> management::Provider {
    let publication = publish_workspace_document(
        f,
        token,
        openapi_document(false),
        &format!("workspaces/{}", f.workspace.id()),
        &uuid::Uuid::new_v4().to_string(),
    )
    .await;
    f.providers()
        .get_provider(request(
            management::GetProviderRequest {
                name: publication.provider,
            },
            token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner()
}

// Catches: Reef told a Provider is Coral's own by anything but the
// builtin field, or an added Provider marked as Coral's own.
#[tokio::test]
async fn list_providers_projects_the_provider_that_get_returns() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    f.install_builtin().await;
    let provider = published_weather(&f, &token).await;
    assert_eq!(provider.display_name, "Weather");
    assert_eq!(provider.state(), management::ProviderState::Active);
    assert_eq!(provider.namespace, "weather");
    assert!(!provider.builtin);
    let listed = f
        .providers()
        .list_providers(request(
            management::ListProvidersRequest {
                parent: format!("workspaces/{}", f.workspace.id()),
                ..Default::default()
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let (builtin, added): (Vec<_>, Vec<_>) = listed
        .providers
        .into_iter()
        .partition(|listed| listed.builtin);
    assert_eq!(
        builtin
            .iter()
            .map(|builtin| (builtin.call_root.as_str(), builtin.namespace.as_str()))
            .collect::<Vec<_>>(),
        [("coral.workspace", "")]
    );
    assert_eq!(added, vec![provider]);
}

/// Pages each Provider once in name order; a token is bound to the caller who got it.
#[tokio::test]
async fn provider_pages_round_trip_and_bind_their_token() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let parent = format!("workspaces/{}", f.workspace.id());
    for namespace in ["first", "second"] {
        let setup = f
            .setups()
            .create_configured_setup(request(
                setup_request(
                    parent.clone(),
                    Input::OpenapiDocument(openapi_document(false).into()),
                ),
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        f.providers()
            .publish_provider(request(
                management::PublishProviderRequest {
                    configuration_revision: 1,
                    parent: parent.clone(),
                    setup: setup.name,
                    request_id: uuid::Uuid::new_v4().to_string(),
                    namespace: namespace.into(),
                    credential_route: Some(route(management::credential_route::Kind::None)),
                    ..Default::default()
                },
                &token,
                "reef",
            ))
            .await
            .unwrap();
    }
    let list = async |caller: &str, page_token: String| {
        f.providers()
            .list_providers(request(
                management::ListProvidersRequest {
                    parent: parent.clone(),
                    page_size: 1,
                    page_token,
                },
                caller,
                "reef",
            ))
            .await
            .map(tonic::Response::into_inner)
    };
    let first = list(&token, String::new()).await.unwrap();
    let second = list(&token, first.next_page_token.clone()).await.unwrap();
    let [one] = first.providers.as_slice() else {
        panic!("one Provider per page: {first:?}")
    };
    let [two] = second.providers.as_slice() else {
        panic!("one Provider per page: {second:?}")
    };
    assert!(one.name < two.name, "name order without repeats");
    assert!(second.next_page_token.is_empty());
    let foreign = list(&f.token(&f.member, "reef"), first.next_page_token)
        .await
        .unwrap_err();
    assert_eq!(
        (foreign.code(), error_reason(&foreign).as_str()),
        (tonic::Code::InvalidArgument, "INVALID_ARGUMENT")
    );
}

#[tokio::test]
async fn list_operations_projects_each_operation() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let provider = published_weather(&f, &token).await;
    let page = f
        .providers()
        .list_operations(request(
            management::ListOperationsRequest {
                parent: provider.name.clone(),
                page_size: 1,
                page_token: String::new(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let [operation] = page.operations.as_slice() else {
        panic!("one Operation: {page:?}")
    };
    assert_eq!(
        operation.name,
        format!("{}/operations/Z2V0Rm9yZWNhc3Q", provider.name)
    );
    assert_eq!(operation.relative_path, "getForecast");
    assert_eq!(
        operation.hints,
        Some(management::OperationHints::default()),
        "an OpenAPI Operation declares no hints"
    );
    assert!(operation.available);
    assert_eq!(operation.tags, vec!["forecast"]);
    assert_eq!(operation.summary, "Get forecast");
    assert_eq!(
        operation.description,
        "Get the **daily forecast**.\n\nResults use the location's time zone."
    );
    assert!(page.next_page_token.is_empty());
}

#[tokio::test]
async fn metadata_and_lifecycle_changes_require_the_current_etag() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let provider = published_weather(&f, &token).await;
    let mut providers = f.providers();
    let rename = |etag: String| management::UpdateProviderRequest {
        name: provider.name.clone(),
        display_name: "Forecasts".into(),
        etag,
    };
    let stale = providers
        .update_provider(request(
            rename(uuid::Uuid::new_v4().to_string()),
            &token,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(
        (stale.code(), error_reason(&stale).as_str()),
        (tonic::Code::Aborted, "ETAG_MISMATCH")
    );
    let renamed = providers
        .update_provider(request(rename(provider.etag.clone()), &token, "reef"))
        .await
        .unwrap()
        .into_inner();
    assert_ne!(renamed.etag, provider.etag);
    assert_eq!(renamed.display_name, "Forecasts");
    let disabled = providers
        .disable_provider(request(
            management::DisableProviderRequest {
                name: provider.name.clone(),
                etag: renamed.etag,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(disabled.state(), management::ProviderState::Disabled);
    let restored = providers
        .restore_provider(request(
            management::RestoreProviderRequest {
                name: provider.name,
                etag: disabled.etag,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(restored.state(), management::ProviderState::Active);
}

/// Regeneration is a Publication against the exact Provider and its etag.
#[tokio::test]
async fn regeneration_publishes_against_the_exact_provider_and_etag() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let provider = published_weather(&f, &token).await;
    let parent = format!("workspaces/{}", f.workspace.id());
    let replacement = f
        .setups()
        .create_configured_setup(request(
            management::CreateProviderSetupRequest {
                provider: provider.name.clone(),
                ..setup_request(
                    parent.clone(),
                    Input::OpenapiDocument(
                        openapi_document(false)
                            .replace("Get forecast", "Get current forecast")
                            .into(),
                    ),
                )
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let regenerated = f
        .providers()
        .publish_provider(request(
            management::PublishProviderRequest {
                configuration_revision: replacement.configuration.as_ref().unwrap().revision,
                allow_incompatible_configuration: true,
                parent,
                setup: replacement.name,
                request_id: uuid::Uuid::new_v4().to_string(),
                provider: provider.name.clone(),
                etag: provider.etag.clone(),
                ..Default::default()
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        regenerated.outcome(),
        management::ProviderPublicationOutcome::Succeeded
    );
    assert_eq!(regenerated.provider, provider.name);
    let current = f
        .providers()
        .get_provider(request(
            management::GetProviderRequest {
                name: provider.name,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_ne!(current.etag, provider.etag);
}

#[tokio::test]
async fn a_deleted_provider_is_not_found() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let provider = published_weather(&f, &token).await;
    let mut providers = f.providers();
    providers
        .delete_provider(request(
            management::DeleteProviderRequest {
                name: provider.name.clone(),
                etag: provider.etag,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap();
    let gone = providers
        .get_provider(request(
            management::GetProviderRequest {
                name: provider.name,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(
        (gone.code(), error_reason(&gone).as_str()),
        (tonic::Code::NotFound, "NOT_FOUND")
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey follows independent Workspace publications through concealment and authorized removal"
)]
async fn workspace_providers_are_independent_and_require_exact_scope_and_owner_mutation() {
    let f = Fixture::new().await;
    let owner = f.token(&f.owner, "reef");
    let member = f.token(&f.member, "reef");
    let parent = format!("workspaces/{}", f.workspace.id());
    let other = f
        .workspaces
        .create(
            &super::change(&f.owner),
            coral_workspace::WorkspaceDisplayName::parse("other").unwrap(),
        )
        .await
        .unwrap();
    let other_parent = format!("workspaces/{}", other.id());
    let local = publish_workspace_document(
        &f,
        &owner,
        openapi_document(false),
        &parent,
        "b0000000-0000-4000-8000-000000000020",
    )
    .await;
    let foreign = publish_workspace_document(
        &f,
        &owner,
        openapi_document(false),
        &other_parent,
        "b0000000-0000-4000-8000-000000000020",
    )
    .await;
    assert_ne!(local.provider, foreign.provider);
    let duplicate = publish_workspace_document(
        &f,
        &owner,
        openapi_document(false),
        &parent,
        "b0000000-0000-4000-8000-000000000021",
    )
    .await;
    assert_eq!(
        duplicate.outcome(),
        management::ProviderPublicationOutcome::Failed
    );
    assert!(duplicate.provider.is_empty());
    assert_eq!(
        duplicate.diagnostic.unwrap().code,
        "provider_namespace_conflict"
    );
    let mut providers = f.providers();
    let visible = providers
        .list_providers(request(
            management::ListProvidersRequest {
                parent: parent.clone(),
                page_size: 0,
                page_token: String::new(),
            },
            &member,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(visible.providers.len(), 1);
    let provider = &visible.providers[0];
    assert_eq!(provider.name, local.provider);
    assert_eq!(provider.namespace, "weather");
    assert!(provider.blockers.is_empty());

    // Even a Workspace Owner cannot address an existing ID through another Workspace.
    let misaddressed = format!(
        "{parent}/providers/{}",
        foreign.provider.split('/').next_back().unwrap()
    );
    for (name, token) in [(&foreign.provider, &member), (&misaddressed, &owner)] {
        let concealed = providers
            .get_provider(request(
                management::GetProviderRequest { name: name.clone() },
                token,
                "reef",
            ))
            .await
            .unwrap_err();
        assert_eq!(concealed.code(), tonic::Code::NotFound);
        let catalog = providers
            .list_operations(request(
                management::ListOperationsRequest {
                    parent: name.clone(),
                    page_size: 0,
                    page_token: String::new(),
                },
                token,
                "reef",
            ))
            .await
            .unwrap_err();
        assert_eq!(catalog.code(), tonic::Code::NotFound);
    }
    let member_update = providers
        .update_provider(request(
            management::UpdateProviderRequest {
                name: provider.name.clone(),
                etag: provider.etag.clone(),
                display_name: "Changed".into(),
            },
            &member,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(member_update.code(), tonic::Code::PermissionDenied);
    let stale = providers
        .delete_provider(request(
            management::DeleteProviderRequest {
                name: provider.name.clone(),
                etag: uuid::Uuid::new_v4().to_string(),
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), tonic::Code::Aborted);
    let member_removal = providers
        .delete_provider(request(
            management::DeleteProviderRequest {
                name: provider.name.clone(),
                etag: provider.etag.clone(),
            },
            &member,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(member_removal.code(), tonic::Code::PermissionDenied);
    let wrong_scope = providers
        .delete_provider(request(
            management::DeleteProviderRequest {
                name: misaddressed,
                etag: provider.etag.clone(),
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(wrong_scope.code(), tonic::Code::NotFound);
    providers
        .delete_provider(request(
            management::DeleteProviderRequest {
                name: provider.name.clone(),
                etag: provider.etag.clone(),
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap();
    let remaining = providers
        .get_provider(request(
            management::GetProviderRequest {
                name: foreign.provider,
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(remaining.namespace, "weather");
    assert_eq!(remaining.state(), management::ProviderState::Active);
}

/// An MCP server that demands OAuth before listing tools, with metadata,
/// registration, and token endpoints on the same origin.
async fn oauth_mcp_server(access: &str) -> MockServer {
    let server = MockServer::start().await;
    let uri = server.uri();
    let bearer = format!("Bearer {access}");
    Mock::given(method("POST"))
        .and(wiremock::matchers::path("/mcp"))
        .and(header("authorization", bearer.as_str()))
        .and(body_partial_json(json!({"method": "initialize"})))
        .respond_with(|request: &Request| {
            envelope(
                request,
                &json!({"protocolVersion": "2025-11-25", "capabilities": {"tools": {}},
                        "serverInfo": {"name": "oauth", "title": "OAuth MCP", "version": "1"}}),
            )
        })
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(wiremock::matchers::path("/mcp"))
        .and(header("authorization", bearer.as_str()))
        .and(body_partial_json(
            json!({"method": "notifications/initialized"}),
        ))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(wiremock::matchers::path("/mcp"))
        .and(header("authorization", bearer.as_str()))
        .and(body_partial_json(json!({"method": "tools/list"})))
        .respond_with(|request: &Request| {
            envelope(
                request,
                &json!({"tools": [{"name": "lookup", "inputSchema": {"type": "object"}}]}),
            )
        })
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(wiremock::matchers::path("/mcp"))
        .respond_with(ResponseTemplate::new(401).insert_header(
            "www-authenticate",
            format!(
                r#"Bearer resource_metadata="{uri}/.well-known/oauth-protected-resource/mcp", scope="read""#
            )
            .as_str(),
        ))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(wiremock::matchers::path(
            "/.well-known/oauth-protected-resource/mcp",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "resource": format!("{uri}/mcp"), "authorization_servers": [uri]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(wiremock::matchers::path("/.well-known/oauth-authorization-server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "issuer": uri, "authorization_endpoint": format!("{uri}/authorize"),
            "token_endpoint": format!("{uri}/token"), "registration_endpoint": format!("{uri}/register"),
            "code_challenge_methods_supported": ["S256"]
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(wiremock::matchers::path("/register"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "client_id": "dcr-client", "token_endpoint_auth_method": "none"
        })))
        .mount(&server)
        .await;
    let access = access.to_owned();
    Mock::given(method("POST"))
        .and(wiremock::matchers::path("/token"))
        .respond_with(move |_request: &Request| {
            ResponseTemplate::new(200).set_body_json(json!({
                "access_token": access, "token_type": "Bearer", "expires_in": 3600,
                "scope": "read"
            }))
        })
        .mount(&server)
        .await;
    server
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey covers challenge, consent, discovery, publication and cancellation"
)]
async fn oauth_challenge_setup_completes_through_the_contract_without_echoing_secrets() {
    let f = Fixture::new().await;
    let mut callbacks = f.accounts();
    let token = f.token(&f.owner, "reef");
    let server = oauth_mcp_server("grpc-oauth-access-canary").await;
    let parent = format!("workspaces/{}", f.workspace.id());
    let mut setups = f.setups();
    let create = |url: String| setup_request(parent.clone(), Input::McpUrl(url));
    let inspected = setups
        .create_provider_setup(request(
            create(format!("{}/mcp", server.uri())),
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        inspected.state(),
        management::ProviderSetupState::AwaitingConfiguration
    );
    let review = inspected.configuration.as_ref().unwrap();
    assert!(!review.configuration_confirmed);
    assert!(review.discovery_authorization_required);
    assert!(review.automatic_client_available);
    assert_eq!(review.issuer.as_deref(), Some(server.uri().as_str()));
    assert_eq!(review.resource, Some(format!("{}/mcp", server.uri())));
    assert_eq!(
        review.authentication_methods,
        vec![
            i32::from(management::CredentialMethod::Bearer),
            i32::from(management::CredentialMethod::OauthAuthorizationCode)
        ]
    );
    let suggestions = review.suggested.clone().unwrap();
    assert_eq!(oauth(&suggestions).scopes, vec!["read"]);
    let setup = setups
        .confirm_provider_setup_configuration(request(
            management::ConfirmProviderSetupConfigurationRequest {
                redirect_uri: "https://reef.example/callback".into(),
                name: inspected.name,
                expected_revision: review.revision,
                configuration: Some(suggestions),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        setup.state(),
        management::ProviderSetupState::AwaitingAuthorization
    );
    assert_eq!(
        setup.credential_method(),
        management::CredentialMethod::OauthAuthorizationCode
    );
    assert_eq!(setup.input_url, format!("{}/mcp", server.uri()));
    assert!(setup.expire_time.is_some());
    assert!(setup.authorization.is_none());
    let started = setups
        .start_provider_setup_authorization(request(
            management::StartProviderSetupAuthorizationRequest {
                configuration_revision: 1,
                name: setup.name.clone(),
                replacement_grant: String::new(),
                redirect_uri: "https://reef.example/provider-setup/oauth/callback".into(),
                application: None,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let authorization = started.authorization.clone().unwrap();
    assert_eq!(
        authorization.state(),
        management::SetupAuthorizationState::Pending
    );
    assert_eq!(
        authorization.client_bootstrap(),
        management::ClientBootstrap::DynamicClientRegistration
    );
    assert_eq!(authorization.requested_scopes, vec!["read"]);
    let navigation =
        url::Url::parse(&authorization.consent_navigation.as_ref().unwrap().url).unwrap();
    let state = navigation
        .query_pairs()
        .find(|(key, _)| key == "state")
        .unwrap()
        .1
        .into_owned();
    for query in [
        "state=not-the-state&code=code-canary".into(),
        format!("state={state}&code=code-canary&state={state}"),
        format!("state={state}&code=code-canary&code=duplicate"),
        format!("state={state}&code=code-canary&error=access_denied"),
        format!("state={state}&code="),
    ] {
        let rejected = callbacks
            .complete_provider_setup_authorization(request(
                management::CompleteProviderSetupAuthorizationRequest { query },
                &token,
                "reef",
            ))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::InvalidArgument);
    }
    let completion = callbacks
        .complete_provider_setup_authorization(request(
            management::CompleteProviderSetupAuthorizationRequest {
                query: format!("state={state}&code=code-canary"),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        completion.workflow,
        Some(
            management::complete_provider_setup_authorization_response::Workflow::ProviderSetup(
                started.name.clone()
            )
        )
    );
    let completed = setups
        .get_provider_setup(request(
            management::GetProviderSetupRequest {
                name: started.name.clone(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(completed.state(), management::ProviderSetupState::Ready);
    assert!(completed.authorized);
    assert_eq!(completed.granted_scope, "read");
    assert!(!completed.provisional_grant_id.is_empty());
    assert_eq!(
        completed.authorization.as_ref().unwrap().state(),
        management::SetupAuthorizationState::Succeeded
    );
    let rendered = format!("{started:?}{completed:?}");
    assert!(!rendered.contains("grpc-oauth-access-canary"));
    assert!(!rendered.contains("code-canary"));
    let mut providers = f.providers();
    let publication = providers
        .publish_provider(request(
            management::PublishProviderRequest {
                configuration_revision: 1,
                allow_incompatible_configuration: true,
                parent: parent.clone(),
                setup: setup.name.clone(),
                request_id: "b0000000-0000-4000-8000-000000000030".into(),
                namespace: "oauth".into(),
                account_grant: completed.provisional_grant_id,
                credential_route: Some(route(management::credential_route::Kind::Fixed)),
                provider: String::new(),
                etag: String::new(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        publication.outcome(),
        management::ProviderPublicationOutcome::Succeeded
    );
    // Abandoning a fresh Setup is terminal and safe.
    let fresh = setups
        .create_configured_setup(request(
            create(format!("{}/mcp", server.uri())),
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let canceled = setups
        .cancel_provider_setup(request(
            management::CancelProviderSetupRequest { name: fresh.name },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(canceled.state(), management::ProviderSetupState::Canceled);
    let retry = setups
        .retry_provider_setup_discovery(request(
            management::RetryProviderSetupDiscoveryRequest {
                name: canceled.name,
                ..Default::default()
            },
            &token,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(retry.code(), tonic::Code::FailedPrecondition);
}

/// An OpenAPI document whose one security scheme is an authorization-code flow
/// against `server`, which also answers the token exchange.
async fn oauth_openapi_document(server: &MockServer, access: &str) -> String {
    let uri = server.uri();
    let access = access.to_owned();
    Mock::given(method("POST"))
        .and(wiremock::matchers::path("/token"))
        .respond_with(move |request: &Request| {
            use base64::Engine as _;
            let form = String::from_utf8_lossy(&request.body).into_owned();
            let basic_client = request
                .headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Basic "))
                .and_then(|value| base64::engine::general_purpose::STANDARD.decode(value).ok())
                .is_some_and(|value| value.starts_with(b"manual-client:"));
            assert!(
                form.contains("client_id=manual-client") || basic_client,
                "{form}"
            );
            assert!(!form.contains("resource="), "{form}");
            ResponseTemplate::new(200).set_body_json(json!({
                "access_token": access, "token_type": "Bearer", "expires_in": 3600,
                "scope": "read"
            }))
        })
        .mount(server)
        .await;
    json!({
        "openapi": "3.1.0", "info": {"title": "Records", "version": "1"},
        "servers": [{"url": uri}], "security": [{"account": ["read"]}],
        "components": {"securitySchemes": {"account": {"type": "oauth2", "flows": {
            "authorizationCode": {
                "authorizationUrl": format!("{uri}/authorize"),
                "tokenUrl": format!("{uri}/token"),
                "scopes": {"read": "Read records"}
            }}}}},
        "paths": {"/records": {"get": {"operationId": "listRecords",
            "responses": {"200": {"description": "Records"}}}}}
    })
    .to_string()
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one wire journey verifies nested secret input, safe readback, retention and authenticated exchange"
)]
async fn oauth_configuration_owns_a_write_only_application_secret() {
    use management::o_auth_client_authentication::Method;
    for basic in [false, true] {
        let fixture = Fixture::new().await;
        let mut callbacks = fixture.accounts();
        let token = fixture.token(&fixture.owner, "reef");
        let upstream = MockServer::start().await;
        let document = oauth_openapi_document(&upstream, "configuration-account-canary").await;
        let mut setups = fixture.setups();
        let setup = setups
            .create_provider_setup(request(
                setup_request(
                    format!("workspaces/{}", fixture.workspace.id()),
                    Input::OpenapiDocument(document.into()),
                ),
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        let review = setup.configuration.unwrap();
        let mut configuration = review.suggested.unwrap();
        oauth_mut(&mut configuration).application = Some(management::OAuthApplication {
            mode: Some(management::o_auth_application::Mode::ClientId(
                "manual-client".into(),
            )),
            authentication: Some(management::OAuthClientAuthentication {
                method: Some(if basic {
                    Method::ClientSecretBasic(management::OAuthClientSecret {
                        secret: "configuration-application-canary".into(),
                    })
                } else {
                    Method::ClientSecretPost(management::OAuthClientSecret {
                        secret: "configuration-application-canary".into(),
                    })
                }),
            }),
            ..Default::default()
        });
        let confirmation = management::ConfirmProviderSetupConfigurationRequest {
            name: setup.name.clone(),
            expected_revision: review.revision,
            configuration: Some(configuration),
            redirect_uri: "https://reef.example/provider-setup/oauth/callback".into(),
        };
        let confirmed = setups
            .confirm_provider_setup_configuration(request(confirmation.clone(), &token, "reef"))
            .await
            .unwrap()
            .into_inner();
        assert!(confirmed.client_secret_configured);
        assert!(!format!("{confirmed:?}").contains("configuration-application-canary"));
        let fetched = setups
            .get_provider_setup(request(
                management::GetProviderSetupRequest {
                    name: setup.name.clone(),
                },
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        let review = fetched.configuration.unwrap();
        let configuration = review.confirmed.unwrap();
        let readback = oauth(&configuration)
            .application
            .as_ref()
            .unwrap()
            .authentication
            .as_ref()
            .unwrap()
            .method
            .as_ref()
            .unwrap();
        assert_eq!(matches!(readback, Method::ClientSecretBasic(_)), basic);
        assert!(matches!(readback,
            Method::ClientSecretBasic(value) | Method::ClientSecretPost(value) if value.secret.is_empty()
        ));
        // Saving the inspected configuration retains the write-only secret.
        let retained = setups
            .confirm_provider_setup_configuration(request(
                management::ConfirmProviderSetupConfigurationRequest {
                    expected_revision: review.revision,
                    configuration: Some(configuration),
                    ..confirmation
                },
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(retained.client_secret_configured);
        let started = setups
            .start_provider_setup_authorization(request(
                management::StartProviderSetupAuthorizationRequest {
                    name: setup.name,
                    redirect_uri: "https://reef.example/provider-setup/oauth/callback".into(),
                    configuration_revision: retained.configuration.unwrap().revision,
                    replacement_grant: String::new(),
                    application: None,
                },
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        let authorization = started.authorization.unwrap();
        assert!(
            !authorization
                .consent_navigation
                .as_ref()
                .unwrap()
                .url
                .contains("configuration-application-canary")
        );
        let navigation =
            url::Url::parse(&authorization.consent_navigation.as_ref().unwrap().url).unwrap();
        let state = navigation
            .query_pairs()
            .find(|(key, _)| key == "state")
            .unwrap()
            .1
            .into_owned();
        let completion = callbacks
            .complete_provider_setup_authorization(request(
                management::CompleteProviderSetupAuthorizationRequest {
                    query: format!("state={state}&code=configuration-code-canary"),
                },
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            completion.workflow,
            Some(
                management::complete_provider_setup_authorization_response::Workflow::ProviderSetup(
                    started.name.clone()
                )
            )
        );
        let completed = setups
            .get_provider_setup(request(
                management::GetProviderSetupRequest {
                    name: started.name.clone(),
                },
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(completed.authorized);
        let requests = upstream.received_requests().await.unwrap();
        let exchange = requests
            .iter()
            .find(|request| request.url.path() == "/token")
            .unwrap();
        let body: std::collections::BTreeMap<_, _> =
            url::form_urlencoded::parse(&exchange.body).collect();
        if basic {
            use base64::Engine as _;
            let expected = base64::engine::general_purpose::STANDARD
                .encode("manual-client:configuration-application-canary");
            assert_eq!(
                exchange
                    .headers
                    .get("authorization")
                    .unwrap()
                    .to_str()
                    .unwrap(),
                format!("Basic {expected}")
            );
            assert!(!body.contains_key("client_secret"));
            assert!(!body.contains_key("client_id"));
        } else {
            assert_eq!(
                body.get("client_secret").map(AsRef::as_ref),
                Some("configuration-application-canary")
            );
            assert!(!exchange.headers.contains_key("authorization"));
        }
    }
}

/// coral-provider owns the consent URL, the state lookup, and who may complete
/// it (`a_ready_openapi_oauth_setup_signs_in_with_an_explicit_client_that_publication_promotes`,
/// `an_explicit_client_is_used_as_given_and_never_replaced_by_bootstrap`). This
/// checks the application input, the authorization projection, and completion.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one Setup projects each application input, the pending authorization, and its completion"
)]
async fn openapi_oauth_document_signs_in_through_an_explicit_application_from_state_alone() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let server = MockServer::start().await;
    let document = oauth_openapi_document(&server, "grpc-openapi-access-canary").await;
    let parent = format!("workspaces/{}", f.workspace.id());
    let mut setups = f.setups();
    let setup = setups
        .create_configured_setup(request(
            setup_request(parent, Input::OpenapiDocument(document.into())),
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(setup.state(), management::ProviderSetupState::Ready);
    assert_eq!(
        setup.credential_method(),
        management::CredentialMethod::OauthAuthorizationCode
    );
    assert!(setup.authorization.is_none());
    let start = |mode: Option<management::o_auth_application::Mode>| {
        management::StartProviderSetupAuthorizationRequest {
            configuration_revision: 1,
            name: setup.name.clone(),
            redirect_uri: "https://reef.example/provider-setup/oauth/callback".into(),
            replacement_grant: String::new(),
            application: mode.map(|mode| management::OAuthApplication {
                mode: Some(mode),
                ..Default::default()
            }),
        }
    };
    // The document carries nothing to bootstrap a client from.
    for (mode, code, reason) in [
        (
            None,
            tonic::Code::FailedPrecondition,
            "CLIENT_BOOTSTRAP_UNAVAILABLE",
        ),
        (
            Some(management::o_auth_application::Mode::ClientId(String::new())),
            tonic::Code::InvalidArgument,
            "INVALID_PROVIDER_INPUT",
        ),
    ] {
        let refused = setups
            .start_provider_setup_authorization(request(start(mode.clone()), &token, "reef"))
            .await
            .unwrap_err();
        assert_eq!(
            (refused.code(), error_reason(&refused).as_str()),
            (code, reason),
            "{mode:?}"
        );
    }
    let started = setups
        .start_provider_setup_authorization(request(
            start(Some(management::o_auth_application::Mode::ClientId(
                "manual-client".into(),
            ))),
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(started.state(), management::ProviderSetupState::Ready);
    let authorization = started.authorization.clone().unwrap();
    assert_eq!(
        authorization.state(),
        management::SetupAuthorizationState::Pending
    );
    assert_eq!(
        authorization.client_bootstrap(),
        management::ClientBootstrap::Manual
    );
    assert_eq!(authorization.client_id, "manual-client");
    assert_eq!(authorization.requested_scopes, vec!["read"]);
    assert!(authorization.expire_time.is_some());
    let navigation =
        url::Url::parse(&authorization.consent_navigation.as_ref().unwrap().url).unwrap();
    let state = navigation
        .query_pairs()
        .find(|(key, _)| key == "state")
        .unwrap()
        .1
        .into_owned();
    let completion = f
        .accounts()
        .complete_provider_setup_authorization(request(
            management::CompleteProviderSetupAuthorizationRequest {
                query: format!("state={state}&code=code-canary"),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        completion.workflow,
        Some(
            management::complete_provider_setup_authorization_response::Workflow::ProviderSetup(
                started.name.clone()
            )
        )
    );
    let completed = setups
        .get_provider_setup(request(
            management::GetProviderSetupRequest {
                name: started.name.clone(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(completed.name, setup.name);
    assert_eq!(completed.state(), management::ProviderSetupState::Ready);
    assert!(completed.authorized);
    assert_eq!(completed.granted_scope, "read");
    assert!(!completed.provisional_grant_id.is_empty());
    assert_eq!(
        completed.authorization.as_ref().unwrap().state(),
        management::SetupAuthorizationState::Succeeded
    );
    assert_eq!(
        completed.authorization.as_ref().unwrap().client_id,
        "manual-client"
    );
    let rendered = format!("{started:?}{completed:?}");
    assert!(!rendered.contains("grpc-openapi-access-canary"));
    assert!(!rendered.contains("code-canary"));
}

/// Without a retained Account, only the caller route publishes: each person
/// connects their own. The other routes report `account_required`.
#[tokio::test]
async fn publication_without_account_material_succeeds_only_for_the_caller_route() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let parent = format!("workspaces/{}", f.workspace.id());
    let mut setups = f.setups();
    let mut providers = f.providers();
    for (namespace, kind, failure) in [
        ("mine", management::credential_route::Kind::Caller, None),
        (
            "nobody",
            management::credential_route::Kind::None,
            Some("account_required"),
        ),
        (
            "shared",
            management::credential_route::Kind::Fixed,
            Some("account_required"),
        ),
    ] {
        let setup = setups
            .create_configured_setup(request(
                setup_request(
                    parent.clone(),
                    Input::OpenapiDocument(openapi_document(true).into()),
                ),
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(!setup.authorized);
        let publication = providers
            .publish_provider(request(
                management::PublishProviderRequest {
                    configuration_revision: 1,
                    allow_incompatible_configuration: true,
                    parent: parent.clone(),
                    setup: setup.name,
                    request_id: uuid::Uuid::new_v4().to_string(),
                    namespace: namespace.into(),
                    credential_route: Some(route(kind)),
                    ..Default::default()
                },
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            publication
                .diagnostic
                .as_ref()
                .map(|diagnostic| diagnostic.code.clone()),
            failure.map(String::from),
            "{namespace}"
        );
        assert_eq!(
            publication.outcome(),
            if failure.is_some() {
                management::ProviderPublicationOutcome::Failed
            } else {
                management::ProviderPublicationOutcome::Succeeded
            },
            "{namespace}"
        );
        if failure.is_none() {
            let provider = providers
                .get_provider(request(
                    management::GetProviderRequest {
                        name: publication.provider,
                    },
                    &token,
                    "reef",
                ))
                .await
                .unwrap()
                .into_inner();
            assert!(
                provider.blockers.is_empty(),
                "a personal Account is not Provider readiness"
            );
        }
    }
}
