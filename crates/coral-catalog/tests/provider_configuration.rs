//! Configuration behavior through the catalog interface, independent of Setup transport.
#![allow(
    unused_crate_dependencies,
    reason = "integration tests inherit library dependencies"
)]
#![expect(
    clippy::indexing_slicing,
    reason = "fixture assertions address known members"
)]
use coral_catalog::{
    AuthenticationConfiguration, CredentialField, CredentialSource, HeaderBinding,
    MaterializationOutcome, OpenApiDocument, ToolAvailability, materialize,
};
use coral_outbound::DestinationPolicy;
use std::collections::BTreeMap;

fn prepared(document: &OpenApiDocument) -> coral_catalog::PreparedProviderCatalog {
    let MaterializationOutcome::Ready(catalog) =
        materialize(DestinationPolicy::default(), &document.clone().into(), &[])
    else {
        panic!("configured catalog must materialize");
    };
    catalog
}

#[test]
fn configuration_metadata_does_not_build_the_tool_schema_graph() {
    let document = OpenApiDocument::parse(
        r##"{
        "openapi":"3.0.3", "info":{"title":"Review first","version":"1"},
        "security":[], "servers":[{"url":"https://example.test"}],
        "paths":{"/data":{"get":{"operationId":"data","responses":{"200":{"description":"OK",
            "content":{"application/json":{"schema":{"$ref":"#/components/schemas/Broken"}}}}}}}},
        "components":{"schemas":{"Broken":{"$ref":"#/components/schemas/Missing"}}}
    }"##,
    )
    .unwrap();
    let review = document
        .configuration_review()
        .expect("schema validation belongs to catalog preparation, not metadata inspection");
    assert_eq!(review.suggested.display_name, "Review first");
    assert_eq!(review.servers[0].tools, ["data"]);
    let configured = document.configure(review.suggested).unwrap();
    let catalog = prepared(&configured);
    assert_eq!(
        catalog.tools()[0].availability(),
        ToolAvailability::Unavailable,
        "metadata inspection must not weaken catalog validation"
    );
}

/// One API key, in the field `key`, sent in `header`.
fn one_header_key(header: &str) -> AuthenticationConfiguration {
    AuthenticationConfiguration::HeaderCredentials {
        headers: BTreeMap::from([(
            header.into(),
            HeaderBinding::Field {
                field: "key".into(),
                prefix: String::new(),
            },
        )]),
    }
}

/// Presentation for the field `key` when the method sends one.
fn key_field(authentication: &AuthenticationConfiguration) -> BTreeMap<String, CredentialField> {
    authentication
        .credential_fields()
        .into_iter()
        .filter(|name| *name == "key")
        .map(|name| {
            (
                name.to_string(),
                CredentialField {
                    label: "API key".into(),
                    description: String::new(),
                    sensitive: true,
                },
            )
        })
        .collect()
}

fn simple_document() -> OpenApiDocument {
    OpenApiDocument::parse(r#"{"openapi":"3.0.3","info":{"title":"Configuration","version":"1"},"security":[],"servers":[{"url":"https://example.test/v1"}],"paths":{"/data":{"get":{"operationId":"data","responses":{"200":{"description":"OK"}}}}}}"#).expect("valid fixture")
}

#[tokio::test]
async fn fingerprints_retain_confirmed_selection_provenance_and_persisted_configuration() {
    use coral_catalog::{ProviderSetupInput, ProviderSetupInputDigest, ServerSelection};
    let document = simple_document();
    let declared = document
        .configure(document.configuration_review().unwrap().suggested)
        .unwrap();
    let mut custom_configuration = declared.configuration().unwrap().clone();
    custom_configuration.servers.insert(
        "#/servers".into(),
        ServerSelection::Custom {
            url: "https://example.test/v1".into(),
        },
    );
    let custom = declared.configure(custom_configuration).unwrap();
    assert_eq!(declared.as_str(), document.as_str());
    assert_eq!(custom.as_str(), document.as_str());
    assert_eq!(prepared(&declared).tools(), prepared(&custom).tools());
    assert_ne!(
        prepared(&declared).fingerprint(),
        prepared(&custom).fingerprint(),
        "same execution must not discard changed Update provenance"
    );
    assert_eq!(
        prepared(&custom).fingerprint(),
        prepared(
            &custom
                .configure(custom.configuration().unwrap().clone())
                .unwrap()
        )
        .fingerprint()
    );

    let fixture = coral_test_support::TestDatabase::new(coral_db::migration_plan())
        .await
        .unwrap();
    let database = fixture.database();
    for document in [declared, custom] {
        let expected_configuration = document.configuration().unwrap().clone();
        let catalog = prepared(&document);
        let fingerprint = catalog.fingerprint();
        let input = ProviderSetupInput::from(document);
        let mut transaction = database.begin().await.unwrap();
        let generation = coral_catalog::publish_generation_in_transaction(
            &mut transaction,
            &input,
            &ProviderSetupInputDigest::of_setup_input(&input),
            catalog,
            1,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        let mut transaction = database.begin().await.unwrap();
        assert_eq!(
            coral_catalog::generation_fingerprint_in_transaction(&mut transaction, &generation)
                .await
                .unwrap(),
            fingerprint
        );
        assert_eq!(
            coral_catalog::configuration_in_transaction(&mut transaction, &generation)
                .await
                .unwrap(),
            Some(expected_configuration)
        );
        coral_catalog::verify_generation_in_transaction(&mut transaction, &generation)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
    }
}

#[test]
fn replacement_removes_old_security_parameters_without_removing_ordinary_arguments() {
    let source = serde_json::json!({
        "openapi":"3.1.0", "info":{"title":"Old API key","version":"1"},
        "servers":[{"url":"https://api.example.test"}],
        "components":{"securitySchemes":{
            "oldHeader":{"type":"apiKey","in":"header","name":"X-Old-Key"},
            "oldQuery":{"type":"apiKey","in":"query","name":"old_key"}
        }},
        "security":[{"oldHeader":[],"oldQuery":[]}],
        "paths":{"/data":{"get":{"parameters":[
            {"name":"X-Old-Key","in":"header","required":true,"schema":{"type":"string"}},
            {"name":"old_key","in":"query","required":true,"schema":{"type":"string"}},
            {"name":"locale","in":"query","schema":{"type":"string"}}
        ],"responses":{"200":{"description":"OK"}}}}}
    });
    let document = OpenApiDocument::parse(&source.to_string()).unwrap();
    let mut values = document.configuration_review().unwrap().suggested;
    values.authentication = Some(AuthenticationConfiguration::Bearer);
    let MaterializationOutcome::Ready(catalog) = materialize(
        DestinationPolicy::default(),
        &document.configure(values).unwrap().into(),
        &[],
    ) else {
        panic!("replacement resolves old security requirements");
    };
    let coral_catalog::ExecutionBinding::Http(binding) = catalog.tools()[0].execution_binding()
    else {
        panic!("HTTP binding");
    };
    assert_eq!(
        binding
            .parameters()
            .iter()
            .map(coral_catalog::HttpParameterBinding::wire_name)
            .collect::<Vec<_>>(),
        ["locale"]
    );
}

#[test]
fn confirmed_bearer_replaces_document_authentication_without_changing_the_source() {
    let source = r#"{
      "openapi":"3.0.3", "info":{"title":"GitHub","version":"1"},
      "servers":[{"url":"https://api.example.test"}],
      "paths":{"/user":{"get":{"operationId":"user","responses":{"200":{"description":"OK"}}}},
               "/public":{"get":{"operationId":"public","security":[],"responses":{"200":{"description":"OK"}}}}}
    }"#;
    let document = OpenApiDocument::parse(source).unwrap();
    let review = document.configuration_review().unwrap();
    assert!(review.suggested.authentication.is_none());
    assert_eq!(review.authentication_status, "missing");
    let mut configuration = review.suggested;
    configuration.authentication = Some(AuthenticationConfiguration::Bearer);
    let configured = document.configure(configuration).unwrap();
    assert_eq!(configured.as_str(), source);
    let MaterializationOutcome::Ready(catalog) =
        materialize(DestinationPolicy::default(), &configured.into(), &[])
    else {
        panic!("confirmed configuration must materialize");
    };
    assert_eq!(
        catalog.credential_contract().authentication(),
        &AuthenticationConfiguration::Bearer
    );
    assert_eq!(
        catalog.credential_contract().destinations(),
        &["https://api.example.test/"]
    );
    for tool in catalog.tools() {
        let coral_catalog::ExecutionBinding::Http(binding) = tool.execution_binding() else {
            panic!("HTTP binding");
        };
        assert_eq!(binding.credential_slots().len(), 1);
        assert_eq!(
            binding.credential_slots()[0].contract().authentication(),
            &AuthenticationConfiguration::Bearer
        );
    }
}

#[test]
fn authentication_configuration_cannot_contain_derived_or_nested_contracts() {
    let document = simple_document();
    let mut values = document.configuration_review().unwrap().suggested;
    values.authentication = Some(AuthenticationConfiguration::Bearer);
    let configured = document.configure(values.clone()).unwrap();
    let contract = prepared(&configured).credential_contract();
    assert_eq!(
        contract.authentication(),
        values.authentication.as_ref().unwrap()
    );
    assert_eq!(contract.destinations(), ["https://example.test/v1"]);
    let mut encoded = serde_json::to_value(values).unwrap();
    encoded["authentication"] = serde_json::to_value(&contract).unwrap();
    serde_json::from_value::<coral_catalog::ProviderConfiguration>(encoded).unwrap_err();
    let encoded = serde_json::to_value(&contract).unwrap();
    assert_eq!(encoded["authentication"]["method"], "bearer");
    assert!(encoded["authentication"].get("base_urls").is_none());
    let replacement = contract.at_destinations(vec![
        "https://b.test/".into(),
        "https://a.test/".into(),
        "https://b.test/".into(),
    ]);
    assert_eq!(
        replacement.destinations(),
        ["https://a.test/", "https://b.test/"]
    );
    assert_eq!(
        AuthenticationConfiguration::None.at_destinations(vec!["https://b.test/".into()]),
        coral_catalog::ProviderCredentialContract::NONE
    );
}

#[test]
fn review_and_materialization_share_referenced_declarations_and_tool_keys() {
    let document = OpenApiDocument::parse(
        &serde_json::json!({
            "openapi": "3.1.0", "info": { "title": "References", "version": "1" },
            "security": [], "servers": [{ "url": "https://root.test" }],
            "paths": {
                "/one": { "$ref": "#/components/pathItems/Shared" },
                "/two": { "$ref": "#/components/pathItems/Shared" },
                "x-not-a-path": {}
            },
            "components": { "pathItems": { "Shared": {
                "servers": [{ "url": "https://local.test/v1" }],
                "get": { "responses": { "200": { "description": "OK" } } }
            } } }
        })
        .to_string(),
    )
    .unwrap();
    let review = document.configuration_review().unwrap();
    let local = review
        .servers
        .iter()
        .find(|group| group.declaration == "#/components/pathItems/Shared/servers")
        .unwrap();
    assert_eq!(local.tools.len(), 2);
    let catalog = prepared(&document.configure(review.suggested.clone()).unwrap());
    assert_eq!(
        local.tools,
        catalog
            .tools()
            .iter()
            .map(|tool| tool.key().as_str())
            .collect::<Vec<_>>()
    );
    for tool in catalog.tools() {
        let coral_catalog::ExecutionBinding::Http(binding) = tool.execution_binding() else {
            panic!("HTTP")
        };
        assert_eq!(binding.server_url(), Some("https://local.test/v1"));
    }
}

#[test]
fn declaration_scoped_variables_preserve_root_path_and_tool_destinations() {
    use coral_catalog::ServerSelection;
    let document = OpenApiDocument::parse(r#"{
      "openapi":"3.0.3", "info":{"title":"Datadog","version":"1"}, "security":[],
      "servers":[{"url":"https://api.{site}/v1","variables":{"site":{"default":"us.test","enum":["us.test","eu.test"]}}}],
      "paths":{
        "/main":{"get":{"operationId":"main","responses":{"200":{"description":"OK"}}}},
        "/logs":{"servers":[{"url":"https://logs.{site}","variables":{"site":{"default":"us.logs.test","enum":["us.logs.test","eu.logs.test"]}}}],
          "get":{"operationId":"logs","responses":{"200":{"description":"OK"}}},
          "delete":{"operationId":"remove","responses":{"200":{"description":"OK"}}},
          "post":{"operationId":"send","servers":[{"url":"https://{tenant}.events.test","variables":{"tenant":{"default":"acme"}}}],"responses":{"200":{"description":"OK"}}}}
      }
    }"#).unwrap();
    let review = document.configuration_review().unwrap();
    assert_eq!(review.servers.len(), 3);
    let shared = review
        .servers
        .iter()
        .find(|group| group.declaration == "#/paths/~1logs/servers")
        .unwrap();
    assert_eq!(
        shared
            .tools
            .iter()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["logs", "remove"])
    );
    assert_eq!(review.servers[0].tools, ["main"]);
    let mut configuration = review.suggested;
    let ServerSelection::Declared { variables, .. } =
        configuration.servers.get_mut("#/servers").unwrap()
    else {
        panic!("declared root");
    };
    variables.insert("site".into(), "eu.test".into());
    let configured = document.configure(configuration).unwrap();
    let MaterializationOutcome::Ready(catalog) =
        materialize(DestinationPolicy::default(), &configured.into(), &[])
    else {
        panic!("configured catalog");
    };
    let destinations = catalog
        .tools()
        .iter()
        .map(|tool| {
            let coral_catalog::ExecutionBinding::Http(binding) = tool.execution_binding() else {
                panic!("HTTP");
            };
            (tool.key().as_str(), binding.server_url().unwrap())
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(destinations["main"], "https://api.eu.test/v1");
    assert_eq!(destinations["logs"], "https://logs.us.logs.test/");
    assert_eq!(destinations["remove"], "https://logs.us.logs.test/");
    assert_eq!(destinations["send"], "https://acme.events.test/");
}

#[test]
fn invalid_enum_preserves_configuration_and_typed_variables_use_declared_defaults() {
    use coral_catalog::ServerSelection;
    let document = OpenApiDocument::parse(
        r#"{
        "openapi":"3.0.3","info":{"title":"Variables","version":"1"},"security":[],
        "servers":[{"url":"https://{tenant}.{site}:{port}/v1","variables":{
          "tenant":{"default":"acme"},"port":{"default":"8443"},
          "site":{"default":"us.test","enum":["us.test","eu.test"]}}}],
        "paths":{"/data":{"get":{"responses":{"200":{"description":"OK"}}}}}
    }"#,
    )
    .unwrap();
    let accepted = document
        .configure(document.configuration_review().unwrap().suggested)
        .unwrap();
    let mut changed = accepted.configuration().unwrap().clone();
    let ServerSelection::Declared { variables, .. } = changed.servers.get_mut("#/servers").unwrap()
    else {
        panic!("declared");
    };
    assert_eq!(variables["tenant"], "acme");
    assert_eq!(variables["port"], "8443");
    variables.insert("tenant".into(), "typed-tenant".into());
    variables.insert("port".into(), "9443".into());
    let typed = accepted.configure(changed.clone()).unwrap();
    let catalog = prepared(&typed);
    let coral_catalog::ExecutionBinding::Http(binding) = catalog.tools()[0].execution_binding()
    else {
        panic!("HTTP");
    };
    assert_eq!(
        binding.server_url(),
        Some("https://typed-tenant.us.test:9443/v1")
    );
    let ServerSelection::Declared { variables, .. } = changed.servers.get_mut("#/servers").unwrap()
    else {
        panic!("declared");
    };
    variables.insert("site".into(), "invalid-secret-canary".into());
    let errors = typed.configure(changed).unwrap_err();
    assert_eq!(errors[0].field, "#/servers");
    assert!(
        !serde_json::to_string(&errors)
            .unwrap()
            .contains("invalid-secret-canary")
    );
    assert_eq!(prepared(&typed).fingerprint(), catalog.fingerprint());
    assert_ne!(typed.configuration(), accepted.configuration());
}

#[test]
fn multiple_servers_require_explicit_selection_or_custom_destination() {
    use coral_catalog::ServerSelection;
    use serde_json::json;
    for servers in [
        json!([]),
        json!([{"url":"https://one.test"},{"url":"https://two.test"}]),
    ] {
        let mut source: serde_json::Value =
            serde_json::from_str(simple_document().as_str()).unwrap();
        source["servers"] = servers;
        let document = OpenApiDocument::parse(&source.to_string()).unwrap();
        let configuration = document.configuration_review().unwrap().suggested;
        assert!(configuration.servers.is_empty());
        document.configure(configuration.clone()).unwrap_err();
        let mut choices = vec![ServerSelection::Custom {
            url: "https://self-hosted.test/base".into(),
        }];
        if source["servers"].as_array().unwrap().len() == 2 {
            choices.push(ServerSelection::Declared {
                url: "https://two.test".into(),
                variables: std::collections::BTreeMap::new(),
            });
        }
        for selection in choices {
            let expected = match &selection {
                ServerSelection::Custom { url } | ServerSelection::Declared { url, .. } => {
                    url.clone()
                }
            };
            let mut selected = configuration.clone();
            selected.servers.insert("#/servers".into(), selection);
            let catalog = prepared(&document.configure(selected).unwrap());
            let coral_catalog::ExecutionBinding::Http(binding) =
                catalog.tools()[0].execution_binding()
            else {
                panic!("HTTP");
            };
            assert_eq!(
                binding.server_url(),
                Some(url::Url::parse(&expected).unwrap().as_str())
            );
        }
    }
}

#[test]
fn unresolved_local_servers_do_not_discard_usable_siblings_or_inherit_root_replacement() {
    use coral_catalog::{ServerSelection, ToolAvailability};
    use serde_json::json;
    for servers in [
        json!([{"url":"https://{site}","variables":{"site":{"enum":["one.test"]}}}]),
        json!([{"url":"https://one.test"},{"url":"https://two.test"}]),
        json!([{"url":"https://user:secret-canary@private.test"}]),
        json!("malformed"),
    ] {
        let mut source: serde_json::Value =
            serde_json::from_str(simple_document().as_str()).unwrap();
        source["paths"]["/local"] = json!({"get":{"operationId":"local","servers":servers,"responses":{"200":{"description":"OK"}}}});
        let document = OpenApiDocument::parse(&source.to_string()).unwrap();
        let mut configuration = document.configuration_review().unwrap().suggested;
        configuration.authentication = Some(AuthenticationConfiguration::Bearer);
        configuration.servers.insert(
            "#/servers".into(),
            ServerSelection::Custom {
                url: "https://replacement.test".into(),
            },
        );
        let catalog = prepared(&document.configure(configuration).unwrap());
        assert_eq!(catalog.tools().len(), 2);
        assert_eq!(
            catalog.tools()[0].availability(),
            ToolAvailability::Available
        );
        assert_eq!(
            catalog.tools()[1].availability(),
            ToolAvailability::Unavailable
        );
        assert_eq!(
            catalog.tools()[1].diagnostic().unwrap().code(),
            "unresolved_server_configuration"
        );
        assert_eq!(
            catalog.credential_contract().destinations(),
            &["https://replacement.test/"]
        );
        assert!(
            !serde_json::to_string(catalog.tools()[1].diagnostics())
                .unwrap()
                .contains("secret-canary")
        );
    }
}

#[test]
fn replacing_declared_authentication_removes_old_credential_injection() {
    use serde_json::json;
    let mut source: serde_json::Value = serde_json::from_str(simple_document().as_str()).unwrap();
    source["security"] = json!([{"old":[]}]);
    source["components"] =
        json!({"securitySchemes":{"old":{"type":"apiKey","in":"header","name":"X-Old-Key"}}});
    let document = OpenApiDocument::parse(&source.to_string()).unwrap();
    let review = document.configuration_review().unwrap();
    assert_eq!(review.authentication_status, "supported");
    for (authentication, slots) in [
        (AuthenticationConfiguration::Bearer, &["configured"][..]),
        (AuthenticationConfiguration::None, &[]),
        (one_header_key("X-New-Key"), &["configured"]),
    ] {
        let mut configuration = review.suggested.clone();
        configuration.credential_fields = key_field(&authentication);
        configuration.authentication = Some(authentication);
        let configured = document.configure(configuration).unwrap();
        let catalog = prepared(&configured);
        let coral_catalog::ExecutionBinding::Http(binding) = catalog.tools()[0].execution_binding()
        else {
            panic!("HTTP");
        };
        assert_eq!(
            catalog.credential_contract().authentication(),
            configured
                .configuration()
                .unwrap()
                .authentication
                .as_ref()
                .unwrap()
        );
        assert_eq!(
            binding
                .credential_slots()
                .iter()
                .map(coral_catalog::HttpCredentialSlot::scheme_name)
                .collect::<Vec<_>>(),
            slots,
            "the declared `old` scheme must not keep a slot"
        );
        assert_eq!(
            binding.credential_slots().is_empty(),
            catalog.credential_contract() == coral_catalog::ProviderCredentialContract::NONE
        );
        assert_eq!(configured.as_str(), document.as_str());
    }
}

#[test]
fn authentication_defaults_distinguish_missing_unused_anonymous_and_ambiguous_metadata() {
    use serde_json::json;
    for (security, schemes, expected) in [
        (None, json!({}), "missing"),
        (
            None,
            json!({"unused":{"type":"http","scheme":"bearer"}}),
            "unused",
        ),
        (Some(json!([])), json!({}), "explicit_anonymous"),
        (
            Some(json!([{}, {"token":[]}])),
            json!({"token":{"type":"http","scheme":"bearer"}}),
            "ambiguous",
        ),
        (
            Some(json!([{"token":[]}])),
            json!({"token":{"type":"http","scheme":"digest"}}),
            "unsupported",
        ),
    ] {
        let mut source = json!({"openapi":"3.0.3","info":{"title":"Review","version":"1"},"servers":[{"url":"https://example.test"}],"components":{"securitySchemes":schemes},"paths":{"/data":{"get":{"responses":{"200":{"description":"OK"}}}}}});
        if let Some(security) = security {
            source["security"] = security;
        }
        source["paths"]["/data"]["get"]["parameters"] = json!([{ "name": "X-Api-Key", "in": "header", "required": true, "schema": { "type": "string" } }]);
        let document = OpenApiDocument::parse(&source.to_string()).unwrap();
        let review = document.configuration_review().unwrap();
        assert_eq!(review.authentication_status, expected);
        let mut configuration = review.suggested;
        configuration.authentication = Some(one_header_key("X-Api-Key"));
        configuration.credential_fields = key_field(configuration.authentication.as_ref().unwrap());
        let configured = document.configure(configuration).unwrap();
        let MaterializationOutcome::Ready(catalog) =
            materialize(DestinationPolicy::default(), &configured.into(), &[])
        else {
            panic!("replacement resolves disagreements");
        };
        assert_eq!(
            catalog.credential_contract().authentication(),
            &one_header_key("x-api-key")
        );
        let coral_catalog::ExecutionBinding::Http(binding) = catalog.tools()[0].execution_binding()
        else {
            panic!("HTTP");
        };
        assert!(
            binding.parameters().is_empty(),
            "host credential slots are not Tool arguments"
        );
    }
}

fn oauth(scopes: Vec<String>) -> AuthenticationConfiguration {
    AuthenticationConfiguration::OAuthAuthorizationCode {
        scope_format: coral_catalog::OAuthScopeFormat::Space,
        authorization_url: "https://AUTH.test:443/authorize".into(),
        token_url: "https://auth.test/token".into(),
        scopes,
        resource: None,
        issuer: None,
        client: None,
    }
}

#[test]
fn oauth_configuration_normalizes_exact_scopes_and_rejects_unsafe_or_unbounded_fields() {
    let document = simple_document();
    let mut configuration = document.configuration_review().unwrap().suggested;
    configuration.authentication = Some(oauth(vec!["write".into(), "read".into(), "read".into()]));
    let accepted = document.configure(configuration.clone()).unwrap();
    let AuthenticationConfiguration::OAuthAuthorizationCode {
        scopes,
        authorization_url,
        ..
    } = accepted
        .configuration()
        .unwrap()
        .authentication
        .as_ref()
        .unwrap()
    else {
        panic!("OAuth");
    };
    assert_eq!(scopes, &["read", "write"]);
    assert_eq!(authorization_url, "https://auth.test/authorize");
    let mut invalid = vec![
        oauth(vec![String::new()]),
        oauth(vec!["has space".into()]),
        oauth(vec!["non-ascii-£".into()]),
        oauth(vec!["quote\"".into()]),
        oauth(vec!["slash\\".into()]),
        oauth(vec!["x".repeat(257)]),
        oauth((0..101).map(|n| format!("scope{n}")).collect()),
    ]
    .into_iter()
    .map(|contract| (contract, "authentication.scopes"))
    .collect::<Vec<_>>();
    for endpoint in [
        "ftp://auth.test/token".into(),
        "https://user:secret-canary@auth.test/token".into(),
        "https://auth.test/token#fragment".into(),
        "https://auth.test/token?scope=read".into(),
        "https://auth.test/token?client_secret=secret-canary".into(),
        "https://auth.test/token?ACCESS_TOKEN=secret-canary".into(),
        "https://auth.test/to\nken".into(),
        format!("https://auth.test/{}", "x".repeat(2048)),
    ] {
        for authorization in [true, false] {
            let mut contract = oauth(vec!["read".into()]);
            let AuthenticationConfiguration::OAuthAuthorizationCode {
                authorization_url,
                token_url,
                ..
            } = &mut contract
            else {
                panic!("OAuth");
            };
            if authorization {
                *authorization_url = endpoint.clone();
            } else {
                *token_url = endpoint.clone();
            }
            invalid.push((
                contract,
                if authorization {
                    "authentication.authorization_url"
                } else {
                    "authentication.token_url"
                },
            ));
        }
    }
    for (contract, field) in invalid {
        configuration.authentication = Some(contract);
        let errors = accepted.configure(configuration.clone()).unwrap_err();
        assert_eq!(errors[0].field, field);
        assert!(
            !serde_json::to_string(&errors)
                .unwrap()
                .contains("secret-canary")
        );
        assert_eq!(
            accepted
                .configuration()
                .unwrap()
                .authentication
                .as_ref()
                .unwrap(),
            prepared(&accepted).credential_contract().authentication()
        );
    }
    let mut added_scope = accepted.configuration().unwrap().clone();
    added_scope.authentication = Some(oauth(vec!["read".into(), "write".into(), "admin".into()]));
    assert_ne!(
        prepared(&accepted).credential_contract(),
        prepared(&accepted.configure(added_scope).unwrap()).credential_contract()
    );
}

#[test]
fn configuration_reports_each_invalid_authentication_field_without_rejected_values() {
    let document = simple_document();
    let mut values = document.configuration_review().unwrap().suggested;
    values.authentication = Some(AuthenticationConfiguration::OAuthAuthorizationCode {
        scope_format: coral_catalog::OAuthScopeFormat::Space,
        authorization_url: "https://user:secret-canary@auth.test/authorize".into(),
        token_url: "not-a-url".into(),
        scopes: vec!["invalid scope".into()],
        resource: None,
        issuer: None,
        client: None,
    });
    let errors = document.configure(values.clone()).unwrap_err();
    assert_eq!(
        errors
            .iter()
            .map(|error| error.field.as_str())
            .collect::<Vec<_>>(),
        [
            "authentication.authorization_url",
            "authentication.token_url",
            "authentication.scopes"
        ]
    );
    assert!(
        !serde_json::to_string(&errors)
            .unwrap()
            .contains("secret-canary")
    );
    values.authentication = Some(one_header_key("cookie"));
    values.credential_fields = key_field(values.authentication.as_ref().unwrap());
    assert_eq!(
        document.configure(values).unwrap_err()[0].field,
        "authentication.headers.cookie"
    );
}

#[test]
fn oauth_review_does_not_suggest_unbounded_or_secret_bearing_declarations() {
    use serde_json::json;
    for (endpoint, scopes) in [
        (
            "https://auth.test/authorize?client_secret=secret-canary".to_string(),
            vec!["read".to_string()],
        ),
        (
            format!("https://auth.test/{}", "x".repeat(2048)),
            vec!["read".into()],
        ),
        ("https://auth.test/authorize".into(), vec!["x".repeat(257)]),
        (
            "https://auth.test/authorize".into(),
            (0..101).map(|n| format!("scope{n}")).collect(),
        ),
    ] {
        let mut source: serde_json::Value =
            serde_json::from_str(simple_document().as_str()).unwrap();
        let declared = scopes
            .iter()
            .map(|scope| (scope.clone(), json!("")))
            .collect::<serde_json::Map<_, _>>();
        source["security"] = json!([{"oauth":scopes}]);
        source["components"] = json!({"securitySchemes":{"oauth":{"type":"oauth2","flows":{"authorizationCode":{"authorizationUrl":endpoint,"tokenUrl":"https://auth.test/token","scopes":declared}}}}});
        let review = OpenApiDocument::parse(&source.to_string())
            .unwrap()
            .configuration_review()
            .unwrap();
        assert_eq!(review.authentication_status, "unsupported");
        assert!(review.suggested.authentication.is_none());
        assert!(
            !serde_json::to_string(&review)
                .unwrap()
                .contains("secret-canary")
        );
    }
}

#[test]
fn mixed_tool_security_never_silently_selects_a_method() {
    use serde_json::json;
    for security in [None, Some(json!([])), Some(json!([{"key":[]}]))] {
        let mut source: serde_json::Value =
            serde_json::from_str(simple_document().as_str()).unwrap();
        source.as_object_mut().unwrap().remove("security");
        source["components"] = json!({"securitySchemes":{"bearer":{"type":"http","scheme":"bearer"},"key":{"type":"apiKey","in":"header","name":"X-Key"}}});
        source["paths"]["/secured"] =
            json!({"get":{"security":[{"bearer":[]}],"responses":{"200":{"description":"OK"}}}});
        if let Some(security) = security {
            source["paths"]["/data"]["get"]["security"] = security;
        }
        let document = OpenApiDocument::parse(&source.to_string()).unwrap();
        let review = document.configuration_review().unwrap();
        assert_eq!(review.authentication_status, "ambiguous");
        assert!(review.suggested.authentication.is_none());
        let mut confirmed = review.suggested;
        confirmed.authentication = Some(AuthenticationConfiguration::Bearer);
        assert!(
            prepared(&document.configure(confirmed).unwrap())
                .tools()
                .iter()
                .all(|tool| tool.availability() == coral_catalog::ToolAvailability::Available)
        );
    }
}

#[test]
fn configuration_field_addresses_are_bounded_even_for_large_document_paths() {
    use serde_json::json;
    let mut source: serde_json::Value = serde_json::from_str(simple_document().as_str()).unwrap();
    let path = format!("/{}", "long".repeat(100));
    source["paths"][&path] = json!({"servers":[{"url":"https://local.test"}],"get":{"responses":{"200":{"description":"OK"}}}});
    let document = OpenApiDocument::parse(&source.to_string()).unwrap();
    let mut configuration = document.configuration_review().unwrap().suggested;
    let local = configuration
        .servers
        .keys()
        .find(|key| key.as_str() != "#/servers")
        .unwrap()
        .clone();
    configuration.servers.insert(
        local,
        coral_catalog::ServerSelection::Custom {
            url: "not-a-url-secret-canary".into(),
        },
    );
    let errors = document.configure(configuration).unwrap_err();
    assert!(errors.iter().all(|error| error.field.len() <= 256));
    assert!(
        !serde_json::to_string(&errors)
            .unwrap()
            .contains("secret-canary")
    );
}

#[test]
fn configuration_review_rejects_unsafe_variable_values_and_malformed_enums() {
    use serde_json::json;
    for variable in [
        json!({"default":"user:secret-canary@private.test"}),
        json!({"default":"one.test","enum":["one.test","user:secret-canary@private.test"]}),
        json!({"default":"one.test","enum":["one.test",42]}),
    ] {
        let mut source: serde_json::Value =
            serde_json::from_str(simple_document().as_str()).unwrap();
        source["servers"] = json!([{"url":"https://{site}","variables":{"site":variable}}]);
        let review = OpenApiDocument::parse(&source.to_string())
            .unwrap()
            .configuration_review()
            .unwrap();
        assert!(
            !serde_json::to_string(&review)
                .unwrap()
                .contains("secret-canary")
        );
        assert!(
            !review.suggested.servers.contains_key("#/servers"),
            "invalid declarations cannot preselect a destination"
        );
    }
}

#[test]
fn configuration_review_does_not_echo_credentials_embedded_in_server_urls() {
    let source = r#"{"openapi":"3.0.3","info":{"title":"Unsafe","version":"1"},"servers":[{"url":"https://user:secret-canary@example.test"}],"paths":{"/data":{"get":{"responses":{"200":{"description":"OK"}}}}}}"#;
    let review = OpenApiDocument::parse(source)
        .unwrap()
        .configuration_review()
        .unwrap();
    assert!(
        !serde_json::to_string(&review)
            .unwrap()
            .contains("secret-canary")
    );
}

#[test]
fn destination_binding_waits_for_final_authoring_availability() {
    use serde_json::json;
    let document = simple_document();
    let mut configuration = document.configuration_review().unwrap().suggested;
    configuration.authentication = Some(AuthenticationConfiguration::Bearer);
    let baseline = prepared(&document.configure(configuration).unwrap()).credential_contract();
    let mut source: serde_json::Value = serde_json::from_str(document.as_str()).unwrap();
    let large_enum = (0..1400)
        .map(|n| format!("{n}{}", "x".repeat(200)))
        .collect::<Vec<_>>();
    source["paths"]["/large"] = json!({"get":{"operationId":"large","servers":[{"url":"https://unused.test"}],"responses":{"200":{"description":"OK","content":{"application/json":{"schema":{"type":"string","enum":large_enum}}}}}}});
    let document = OpenApiDocument::parse(&source.to_string()).unwrap();
    let mut configuration = document.configuration_review().unwrap().suggested;
    configuration.authentication = Some(AuthenticationConfiguration::Bearer);
    let catalog = prepared(&document.configure(configuration).unwrap());
    assert_eq!(
        catalog.tools()[1].diagnostic().unwrap().code(),
        "tool_authoring_description_too_large"
    );
    assert_eq!(catalog.credential_contract(), baseline);
}

#[test]
fn only_available_credential_destinations_affect_compatibility() {
    use coral_catalog::{ExecutionBinding, ToolAvailability};
    use serde_json::json;
    let catalog = |extra: Option<(&str, bool)>, authentication: AuthenticationConfiguration| {
        let mut source: serde_json::Value =
            serde_json::from_str(simple_document().as_str()).unwrap();
        if let Some((url, unavailable)) = extra {
            source["paths"]["/other"] = json!({"get":{"operationId":"aaaOther","servers":[{"url":url}],"responses":{"200":{"description":"OK"}}}});
            if unavailable {
                source["paths"]["/other"]["get"]["callbacks"] = json!({});
            }
        }
        let document = OpenApiDocument::parse(&source.to_string()).unwrap();
        let mut configuration = document.configuration_review().unwrap().suggested;
        configuration.authentication = Some(authentication);
        prepared(&document.configure(configuration).unwrap())
    };
    let baseline = catalog(None, AuthenticationConfiguration::Bearer).credential_contract();
    for (url, unavailable, compatible) in [
        ("https://example.test/v1", false, true),
        ("https://other.test/v2", false, false),
        ("https://other.test/v2", true, true),
    ] {
        let catalog = catalog(
            Some((url, unavailable)),
            AuthenticationConfiguration::Bearer,
        );
        assert_eq!(
            catalog.credential_contract() == baseline,
            compatible,
            "{url}, unavailable={unavailable}"
        );
        assert_eq!(
            catalog.tools()[0].availability() == ToolAvailability::Unavailable,
            unavailable
        );
        for tool in catalog.tools() {
            let ExecutionBinding::Http(binding) = tool.execution_binding() else {
                panic!("HTTP");
            };
            assert_eq!(
                binding.credential_slots()[0].contract(),
                &catalog.credential_contract()
            );
        }
    }
    assert_eq!(
        catalog(None, AuthenticationConfiguration::None).credential_contract(),
        catalog(
            Some(("https://anonymous.test/new", false)),
            AuthenticationConfiguration::None
        )
        .credential_contract()
    );
}

#[test]
fn destination_compatibility_uses_normalized_base_paths_not_tool_count() {
    use serde_json::json;
    let contract = |base: &str, extra: bool| {
        let mut source = json!({"openapi":"3.0.3","info":{"title":"Destinations","version":"1"},"servers":[{"url":base}],"paths":{"/data":{"get":{"responses":{"200":{"description":"OK"}}}}}});
        if extra {
            source["paths"]["/other"] = source["paths"]["/data"].clone();
        }
        let document = OpenApiDocument::parse(&source.to_string()).unwrap();
        let mut configuration = document.configuration_review().unwrap().suggested;
        configuration.authentication = Some(AuthenticationConfiguration::Bearer);
        let MaterializationOutcome::Ready(catalog) = materialize(
            DestinationPolicy::default(),
            &document.configure(configuration).unwrap().into(),
            &[],
        ) else {
            panic!("valid configuration");
        };
        catalog.credential_contract()
    };
    assert_eq!(
        contract("HTTPS://EXAMPLE.TEST:443/v1/", false),
        contract("https://example.test/v1", true)
    );
    assert_ne!(
        contract("https://example.test/v1", false),
        contract("https://example.test/v2", false)
    );
}

#[test]
fn a_repeated_server_template_still_resolves_but_wrong_variables_do_not() {
    use coral_catalog::ServerSelection;
    use serde_json::json;
    // Datadog's document declares the same template more than once.
    let template = json!({
        "url": "https://{subdomain}.{site}",
        "variables": {
            "site": {"default": "datadoghq.com", "enum": ["datadoghq.com", "datadoghq.eu"]},
            "subdomain": {"default": "api"},
        },
    });
    let mut source: serde_json::Value =
        serde_json::from_str(simple_document().as_str()).expect("fixture JSON");
    source["servers"] = json!([template, template]);
    let document = OpenApiDocument::parse(&source.to_string()).expect("valid fixture");
    let mut configuration = document.configuration_review().expect("review").suggested;
    let selected = |site: &str| {
        BTreeMap::from([(
            "#/servers".to_string(),
            ServerSelection::Declared {
                url: "https://{subdomain}.{site}".into(),
                variables: BTreeMap::from([
                    ("site".to_string(), site.to_string()),
                    ("subdomain".to_string(), "api".to_string()),
                ]),
            },
        )])
    };

    configuration.authentication = Some(AuthenticationConfiguration::Bearer);
    configuration.servers = selected("datadoghq.eu");
    let configured = document
        .configure(configuration.clone())
        .expect("repeated template resolves");
    assert_eq!(
        prepared(&configured).credential_contract().destinations(),
        &["https://api.datadoghq.eu/"]
    );

    configuration.servers = selected("example.test");
    assert_eq!(
        document.configure(configuration).unwrap_err()[0].field,
        "#/servers",
        "a value outside the declared enum is still refused"
    );
}

fn header_key_document(security: &serde_json::Value) -> OpenApiDocument {
    let mut source: serde_json::Value =
        serde_json::from_str(simple_document().as_str()).expect("fixture JSON");
    source["security"] = security.clone();
    source["components"] = serde_json::json!({"securitySchemes":{
        "apiKeyAuth":{"type":"apiKey","in":"header","name":"DD-API-KEY","description":"Your Datadog API key"},
        "appKeyAuth":{"type":"apiKey","in":"header","name":"DD-APPLICATION-KEY"}
    }});
    OpenApiDocument::parse(&source.to_string()).expect("valid fixture")
}

fn field(name: &str) -> HeaderBinding {
    HeaderBinding::Field {
        field: name.into(),
        prefix: String::new(),
    }
}

#[test]
fn keys_required_together_suggest_one_credential_field_for_each_scheme() {
    use serde_json::json;
    let review = header_key_document(&json!([{"apiKeyAuth":[],"appKeyAuth":[]}]))
        .configuration_review()
        .unwrap();

    assert_eq!(review.authentication_status, "supported");
    assert_eq!(
        review.suggested.authentication,
        Some(AuthenticationConfiguration::HeaderCredentials {
            headers: BTreeMap::from([
                ("dd-api-key".into(), field("api_key_auth")),
                ("dd-application-key".into(), field("app_key_auth")),
            ]),
        })
    );
    assert_eq!(
        review.suggested.credential_fields,
        BTreeMap::from([
            (
                "api_key_auth".into(),
                CredentialField {
                    label: "apiKeyAuth".into(),
                    description: "Your Datadog API key".into(),
                    sensitive: true,
                }
            ),
            (
                "app_key_auth".into(),
                CredentialField {
                    label: "appKeyAuth".into(),
                    description: String::new(),
                    sensitive: true,
                }
            ),
        ])
    );
}

#[test]
fn one_header_key_suggests_one_credential_field_and_hides_its_header_from_programs() {
    use serde_json::json;
    let document = header_key_document(&json!([{"apiKeyAuth":[]}]));
    let review = document.configuration_review().unwrap();
    assert_eq!(
        review.suggested.authentication,
        Some(AuthenticationConfiguration::HeaderCredentials {
            headers: BTreeMap::from([("dd-api-key".into(), field("api_key_auth"))]),
        })
    );
    assert_eq!(
        review
            .suggested
            .credential_fields
            .keys()
            .collect::<Vec<_>>(),
        ["api_key_auth"]
    );
    // Alternatives still need an explicit choice.
    let alternatives = header_key_document(&json!([{"apiKeyAuth":[]}, {"appKeyAuth":[]}]))
        .configuration_review()
        .unwrap();
    assert_eq!(alternatives.authentication_status, "ambiguous");
    assert_eq!(alternatives.suggested.authentication, None);
}

#[test]
fn scheme_names_become_readable_field_names_and_too_many_keys_are_unsupported() {
    use serde_json::json;
    let mut source: serde_json::Value =
        serde_json::from_str(simple_document().as_str()).expect("fixture JSON");
    source["security"] = json!([{"APIKey": [], "OAuthLikeName": []}]);
    source["components"] = json!({"securitySchemes": {
        "APIKey": {"type": "apiKey", "in": "header", "name": "x-api-key"},
        "OAuthLikeName": {"type": "apiKey", "in": "header", "name": "x-other-key"},
    }});
    let review = OpenApiDocument::parse(&source.to_string())
        .expect("valid fixture")
        .configuration_review()
        .expect("review");
    assert_eq!(
        review
            .suggested
            .credential_fields
            .keys()
            .collect::<Vec<_>>(),
        ["api_key", "o_auth_like_name"]
    );

    let mut requirement = serde_json::Map::new();
    let mut schemes = serde_json::Map::new();
    for index in 0..17 {
        requirement.insert(format!("key{index}"), json!([]));
        schemes.insert(
            format!("key{index}"),
            json!({"type": "apiKey", "in": "header", "name": format!("x-key-{index}")}),
        );
    }
    source["security"] = json!([requirement]);
    source["components"] = json!({ "securitySchemes": schemes });
    let review = OpenApiDocument::parse(&source.to_string())
        .expect("valid fixture")
        .configuration_review()
        .expect("review");
    assert_eq!(review.authentication_status, "unsupported");
    assert_eq!(review.suggested.authentication, None);
}

#[test]
fn header_credentials_report_the_header_or_field_at_fault_without_rejected_values() {
    let document = simple_document();
    let base = document.configuration_review().unwrap().suggested;
    let with_prefix = |prefix: &str| HeaderBinding::Field {
        field: "key".into(),
        prefix: prefix.into(),
    };
    for (headers, fields, expected) in [
        (
            vec![("cookie", field("key"))],
            presented(&["key"]),
            "authentication.headers.cookie",
        ),
        (
            vec![("x-key", field("missing"))],
            presented(&["key"]),
            "authentication.headers.x-key",
        ),
        (
            vec![("x-key", field("key"))],
            presented(&["key", "spare"]),
            "authentication.fields.spare",
        ),
        (
            vec![(
                "x-key",
                HeaderBinding::Field {
                    field: "Key".into(),
                    prefix: String::new(),
                },
            )],
            presented(&["Key"]),
            "authentication.fields.Key",
        ),
        (
            vec![("x-key", with_prefix("secret-canary\n"))],
            presented(&["key"]),
            "authentication.headers.x-key",
        ),
        (vec![], presented(&[]), "authentication.headers"),
        // Every typed-in method sends at least one field, so an account has
        // something to enter, and account authorization can tell such a method
        // by the fields it sends.
        (
            vec![(
                "x-client",
                HeaderBinding::Literal {
                    value: "coral".into(),
                },
            )],
            presented(&[]),
            "authentication.fields",
        ),
    ] {
        let mut values = base.clone();
        values.authentication = Some(AuthenticationConfiguration::HeaderCredentials {
            headers: headers
                .into_iter()
                .map(|(name, binding)| (name.to_string(), binding))
                .collect(),
        });
        values.credential_fields = fields;
        let errors = document.configure(values).unwrap_err();
        assert_eq!(errors[0].field, expected);
        assert!(
            !serde_json::to_string(&errors)
                .unwrap()
                .contains("secret-canary")
        );
    }

    let mut valid = base;
    valid.authentication = Some(AuthenticationConfiguration::HeaderCredentials {
        headers: BTreeMap::from([
            ("Authorization".into(), with_prefix("Token token=")),
            (
                "x-client".into(),
                HeaderBinding::Literal {
                    value: "coral".into(),
                },
            ),
        ]),
    });
    valid.credential_fields = presented(&["key"]);
    let configured = document.configure(valid).unwrap();
    assert_eq!(
        configured.configuration().unwrap().authentication,
        Some(AuthenticationConfiguration::HeaderCredentials {
            headers: BTreeMap::from([
                ("authorization".into(), with_prefix("Token token=")),
                (
                    "x-client".into(),
                    HeaderBinding::Literal {
                        value: "coral".into()
                    }
                ),
            ]),
        }),
        "header names are stored in lowercase"
    );
}

#[test]
fn header_text_is_visible_ascii_so_every_call_can_carry_it() {
    let document = simple_document();
    let base = document.configuration_review().unwrap().suggested;
    let key = || {
        BTreeMap::from([(
            "key".to_string(),
            CredentialField {
                label: "Key".into(),
                description: String::new(),
                sensitive: true,
            },
        )])
    };
    for (headers, expected) in [
        (
            vec![(
                "x-key",
                HeaderBinding::Field {
                    field: "key".into(),
                    prefix: "Clé ".into(),
                },
            )],
            "authentication.headers.x-key",
        ),
        (
            vec![
                ("x-key", field("key")),
                (
                    "x-client",
                    HeaderBinding::Literal {
                        value: "café".into(),
                    },
                ),
            ],
            "authentication.headers.x-client",
        ),
    ] {
        let mut values = base.clone();
        values.authentication = Some(AuthenticationConfiguration::HeaderCredentials {
            headers: headers
                .into_iter()
                .map(|(name, binding)| (name.to_string(), binding))
                .collect(),
        });
        values.credential_fields = key();
        assert_eq!(document.configure(values).unwrap_err()[0].field, expected);
    }
}

fn basic(username: CredentialSource, password: CredentialSource) -> AuthenticationConfiguration {
    AuthenticationConfiguration::BasicAuth { username, password }
}

fn from_field(name: &str) -> CredentialSource {
    CredentialSource::Field { field: name.into() }
}

fn literal(value: &str) -> CredentialSource {
    CredentialSource::Literal {
        value: value.into(),
    }
}

fn presented(names: &[&str]) -> BTreeMap<String, CredentialField> {
    names
        .iter()
        .map(|name| {
            (
                (*name).to_string(),
                CredentialField {
                    label: "Key".into(),
                    description: String::new(),
                    sensitive: true,
                },
            )
        })
        .collect()
}

#[test]
fn basic_auth_reports_the_part_or_field_at_fault_without_rejected_values() {
    let document = simple_document();
    let base = document.configuration_review().unwrap().suggested;
    for (authentication, fields, expected) in [
        (
            basic(literal("secret-canary:"), from_field("key")),
            presented(&["key"]),
            "authentication.username",
        ),
        (
            basic(from_field("key"), literal("secret-canary\n")),
            presented(&["key"]),
            "authentication.password",
        ),
        (
            basic(from_field("missing"), from_field("key")),
            presented(&["key"]),
            "authentication.username",
        ),
        (
            basic(from_field("email"), from_field("key")),
            presented(&["email", "key", "spare"]),
            "authentication.fields.spare",
        ),
        (
            basic(literal("api"), literal("")),
            presented(&[]),
            "authentication.fields",
        ),
        // A constant password would keep a secret in stored configuration.
        (
            basic(from_field("email"), literal("secret-canary")),
            presented(&["email"]),
            "authentication.password",
        ),
    ] {
        let mut values = base.clone();
        values.authentication = Some(authentication);
        values.credential_fields = fields;
        let errors = document.configure(values).unwrap_err();
        assert_eq!(errors[0].field, expected);
        assert!(
            !serde_json::to_string(&errors)
                .unwrap()
                .contains("secret-canary")
        );
    }

    // Jira pairs an email with a token; Mailgun's username is the constant `api`.
    for (authentication, fields) in [
        (
            basic(from_field("email"), from_field("api_token")),
            presented(&["email", "api_token"]),
        ),
        (
            basic(literal("api"), from_field("key")),
            presented(&["key"]),
        ),
        (basic(from_field("key"), literal("")), presented(&["key"])),
    ] {
        let mut values = base.clone();
        values.authentication = Some(authentication.clone());
        values.credential_fields = fields;
        let configured = document.configure(values).unwrap();
        assert_eq!(
            configured.configuration().unwrap().authentication,
            Some(authentication)
        );
    }
}

#[test]
fn an_http_basic_scheme_suggests_a_username_and_a_password() {
    let mut source: serde_json::Value =
        serde_json::from_str(simple_document().as_str()).expect("fixture JSON");
    source["security"] = serde_json::json!([{"basicAuth": []}]);
    source["components"] = serde_json::json!({"securitySchemes": {
        "basicAuth": {"type": "http", "scheme": "Basic"}
    }});
    let document = OpenApiDocument::parse(&source.to_string()).expect("valid fixture");
    let review = document.configuration_review().unwrap();

    assert_eq!(review.authentication_status, "supported");
    let expected = basic(from_field("username"), from_field("password"));
    assert_eq!(review.suggested.authentication, Some(expected.clone()));
    assert_eq!(
        review.suggested.credential_fields,
        BTreeMap::from([
            (
                "password".into(),
                CredentialField {
                    label: "Password".into(),
                    description: String::new(),
                    sensitive: true,
                }
            ),
            (
                "username".into(),
                CredentialField {
                    label: "Username".into(),
                    description: String::new(),
                    sensitive: false,
                }
            ),
        ])
    );
    let catalog = prepared(&document.configure(review.suggested).unwrap());
    assert_eq!(catalog.credential_contract().authentication(), &expected);
}

fn query(parameters: &[(&str, CredentialSource)]) -> AuthenticationConfiguration {
    AuthenticationConfiguration::QueryCredentials {
        parameters: parameters
            .iter()
            .map(|(name, source)| ((*name).to_string(), source.clone()))
            .collect(),
    }
}

#[test]
fn query_credentials_report_the_parameter_or_field_at_fault_without_rejected_values() {
    let document = simple_document();
    let base = document.configuration_review().unwrap().suggested;
    for (authentication, fields, expected) in [
        (query(&[]), presented(&[]), "authentication.parameters"),
        (
            query(&[
                ("key", from_field("key")),
                ("token", literal("secret-canary\n")),
            ]),
            presented(&["key"]),
            "authentication.parameters.token",
        ),
        (
            query(&[("", from_field("key"))]),
            presented(&["key"]),
            "authentication.parameters.",
        ),
        (
            query(&[("key", from_field("missing"))]),
            presented(&["key"]),
            "authentication.parameters.key",
        ),
        (
            query(&[("key", from_field("key"))]),
            presented(&["key", "spare"]),
            "authentication.fields.spare",
        ),
        (
            query(&[("client", literal("coral"))]),
            presented(&[]),
            "authentication.fields",
        ),
    ] {
        let mut values = base.clone();
        values.authentication = Some(authentication);
        values.credential_fields = fields;
        let errors = document.configure(values).unwrap_err();
        assert_eq!(errors[0].field, expected);
        assert!(
            !serde_json::to_string(&errors)
                .unwrap()
                .contains("secret-canary")
        );
    }

    // Trello's shape, and a key beside a constant the service requires.
    for (authentication, fields) in [
        (
            query(&[
                ("key", from_field("api_key")),
                ("token", from_field("api_token")),
            ]),
            presented(&["api_key", "api_token"]),
        ),
        (
            query(&[("api_key", from_field("key")), ("format", literal("json"))]),
            presented(&["key"]),
        ),
    ] {
        let mut values = base.clone();
        values.authentication = Some(authentication.clone());
        values.credential_fields = fields;
        let configured = document.configure(values).unwrap();
        assert_eq!(
            configured.configuration().unwrap().authentication,
            Some(authentication)
        );
    }
}

fn query_key_document(security: &serde_json::Value) -> OpenApiDocument {
    let mut source: serde_json::Value =
        serde_json::from_str(simple_document().as_str()).expect("fixture JSON");
    source["security"] = security.clone();
    source["components"] = serde_json::json!({"securitySchemes": {
        "APIKey": {"type": "apiKey", "in": "query", "name": "key", "description": "Your application key"},
        "APIToken": {"type": "apiKey", "in": "query", "name": "token"},
        "HeaderKey": {"type": "apiKey", "in": "header", "name": "x-api-key"}
    }});
    OpenApiDocument::parse(&source.to_string()).expect("valid fixture")
}

#[test]
fn keys_in_the_query_suggest_query_credentials_and_leave_program_arguments() {
    use serde_json::json;
    let document = query_key_document(&json!([{"APIKey": [], "APIToken": []}]));
    let review = document.configuration_review().unwrap();

    assert_eq!(review.authentication_status, "supported");
    let expected = query(&[
        ("key", from_field("api_key")),
        ("token", from_field("api_token")),
    ]);
    assert_eq!(review.suggested.authentication, Some(expected.clone()));
    assert_eq!(
        review.suggested.credential_fields,
        BTreeMap::from([
            (
                "api_key".into(),
                CredentialField {
                    label: "APIKey".into(),
                    description: "Your application key".into(),
                    sensitive: true,
                }
            ),
            (
                "api_token".into(),
                CredentialField {
                    label: "APIToken".into(),
                    description: String::new(),
                    sensitive: true,
                }
            ),
        ])
    );
    let catalog = prepared(&document.configure(review.suggested).unwrap());
    assert_eq!(catalog.credential_contract().authentication(), &expected);

    // A key in a header and a key in the query together are more than one method.
    let mixed = query_key_document(&json!([{"APIKey": [], "HeaderKey": []}]))
        .configuration_review()
        .unwrap();
    assert_eq!(mixed.authentication_status, "unsupported");
}

#[test]
fn a_query_credential_is_never_a_program_argument() {
    let mut source: serde_json::Value =
        serde_json::from_str(simple_document().as_str()).expect("fixture JSON");
    source["security"] = serde_json::json!([]);
    for path in source["paths"].as_object_mut().unwrap().values_mut() {
        for tool in path.as_object_mut().unwrap().values_mut() {
            tool["parameters"] = serde_json::json!([
                {"name": "token", "in": "query", "schema": {"type": "string"}},
                {"name": "limit", "in": "query", "schema": {"type": "integer"}}
            ]);
        }
    }
    let document = OpenApiDocument::parse(&source.to_string()).expect("valid fixture");
    let mut configuration = document.configuration_review().unwrap().suggested;
    configuration.authentication = Some(query(&[("token", from_field("token"))]));
    configuration.credential_fields = presented(&["token"]);
    let catalog = prepared(&document.configure(configuration).unwrap());
    for tool in catalog.tools() {
        let coral_catalog::ExecutionBinding::Http(binding) = tool.execution_binding() else {
            panic!("HTTP binding");
        };
        let names = binding
            .parameters()
            .iter()
            .map(coral_catalog::HttpParameterBinding::wire_name)
            .collect::<Vec<_>>();
        assert_eq!(names, ["limit"]);
    }
}

#[test]
fn comma_scope_format_sorts_tokens_and_refuses_a_token_containing_a_comma() {
    let document = simple_document();
    let configure = |scopes: serde_json::Value, format: &str| {
        let mut configuration = document.configuration_review().unwrap().suggested;
        configuration.authentication = Some(
            serde_json::from_value(serde_json::json!({
                "method": "o_auth_authorization_code",
                "authorization_url": "https://auth.test/authorize",
                "token_url": "https://auth.test/token",
                "scopes": scopes, "scope_format": format,
                "resource": null, "issuer": null, "client": null
            }))
            .unwrap(),
        );
        document.configure(configuration).map(|accepted| {
            serde_json::to_value(accepted.configuration().unwrap()).unwrap()["authentication"]
                .clone()
        })
    };
    let accepted = configure(serde_json::json!(["write", "read"]), "comma").unwrap();
    assert_eq!(accepted["scopes"], serde_json::json!(["read", "write"]));
    assert_eq!(accepted["scope_format"], "comma");

    // A comma inside one token would split into two scopes on the wire.
    let errors = configure(serde_json::json!(["read,write"]), "comma").unwrap_err();
    assert_eq!(
        errors
            .iter()
            .map(|error| error.field.as_str())
            .collect::<Vec<_>>(),
        ["authentication.scopes"]
    );
    let accepted = configure(serde_json::json!(["read,write"]), "space").unwrap();
    assert_eq!(accepted["scopes"], serde_json::json!(["read,write"]));
}

#[test]
fn request_headers_are_normalized_or_refused_at_the_offending_header() {
    let document = simple_document();
    let headers = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    };
    let too_many = (0..65)
        .map(|n| (format!("x-header-{n}"), "value".to_owned()))
        .collect::<BTreeMap<_, _>>();
    let refused = |field: &str| Err(vec![field.to_owned()]);
    for (submitted, mcp, expected) in [
        (
            headers(&[("X-Tenant", "example"), ("accept", "application/json")]),
            false,
            Ok(headers(&[
                ("accept", "application/json"),
                ("x-tenant", "example"),
            ])),
        ),
        (
            headers(&[("Authorization", "Bearer not-a-service-header")]),
            false,
            refused("request_headers.Authorization"),
        ),
        (
            headers(&[("content-type", "text/plain")]),
            false,
            refused("request_headers.content-type"),
        ),
        (
            headers(&[("user-agent", "custom")]),
            false,
            refused("request_headers.user-agent"),
        ),
        (
            headers(&[("mcp-session-id", "forged")]),
            false,
            refused("request_headers.mcp-session-id"),
        ),
        (
            headers(&[("x-tenant", "one\r\nx-other: two")]),
            false,
            refused("request_headers.x-tenant"),
        ),
        (
            headers(&[("X-Tenant", "one"), ("x-tenant", "two")]),
            false,
            refused("request_headers.x-tenant"),
        ),
        (
            headers(&[("x-api-key", "constant")]),
            false,
            refused("request_headers.x-api-key"),
        ),
        (too_many, false, refused("request_headers")),
        (
            headers(&[("accept", "application/json")]),
            true,
            refused("request_headers.accept"),
        ),
    ] {
        let mut configuration = document.configuration_review().unwrap().suggested;
        configuration.authentication = Some(one_header_key("x-api-key"));
        configuration.credential_fields = key_field(configuration.authentication.as_ref().unwrap());
        configuration.request_headers = submitted.clone();
        // MCP owns the Accept header, so only MCP configuration refuses it.
        let outcome = if mcp {
            configuration
                .normalize_request_headers(true)
                .map(|()| configuration.request_headers)
        } else {
            document
                .configure(configuration)
                .map(|configured| configured.configuration().unwrap().request_headers.clone())
        };
        let outcome = outcome.map_err(|errors| {
            errors
                .into_iter()
                .map(|error| error.field)
                .collect::<Vec<_>>()
        });
        assert_eq!(outcome, expected, "{submitted:?} (MCP: {mcp})");
    }
}
