//! Destination policy at the public materialization boundary.
#![expect(
    unused_crate_dependencies,
    reason = "integration targets inherit library dependencies"
)]

use coral_catalog::{MaterializationOutcome, OpenApiDocument, ProviderSetupInput};
use coral_outbound::DestinationPolicy;

#[test]
fn materialization_rejects_non_public_server_literals() {
    for host in [
        "127.0.0.1",
        "169.254.169.254",
        "10.1.2.3",
        "[fd00::1]",
        "[2002:7f00:1::]",
    ] {
        let document = OpenApiDocument::parse(&format!(
            r#"
openapi: 3.1.0
info: {{title: Untrusted Provider, version: "1"}}
servers: [{{url: "https://{host}"}}]
paths:
  /read:
    get:
      operationId: read
      responses:
        "200": {{description: data}}
"#
        ))
        .expect("document");
        let outcome = coral_catalog::materialize(
            DestinationPolicy::default(),
            &ProviderSetupInput::from(document),
            &[],
        );
        let MaterializationOutcome::Rejected(diagnostic) = outcome else {
            panic!("accepted {host}")
        };
        assert_eq!(diagnostic.code(), "outbound_destination_rejected");
        assert!(!diagnostic.message().contains(host));
    }
}

#[test]
fn cached_destination_checks_and_materialization_share_diagnostics() {
    for address in [
        "https://example.test/api",
        "http://localhost:8080",
        "https://169.254.169.254",
    ] {
        let document = OpenApiDocument::parse(&format!(
            r"
openapi: 3.1.0
info: {{title: Destinations, version: '1'}}
security: []
servers: [{{url: '{address}'}}]
paths:
  /read:
    get:
      operationId: read
      responses:
        '200': {{description: data}}
"
        ))
        .unwrap();
        let input = ProviderSetupInput::from(document);
        for policy in [
            DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
            DestinationPolicy::default(),
        ] {
            let checked = coral_catalog::validate_provider_destinations(
                policy,
                [address],
                &coral_catalog::ProviderCredentialContract::NONE,
            );
            match coral_catalog::materialize(policy, &input, &[]) {
                MaterializationOutcome::Ready(_) => checked.unwrap(),
                MaterializationOutcome::Rejected(diagnostic) => {
                    assert_eq!(checked.unwrap_err(), diagnostic);
                }
            }
        }
    }
}

#[test]
fn loopback_materialization_requires_explicit_permission() {
    let document = OpenApiDocument::parse(
        r#"
openapi: 3.1.0
info: {title: Local Provider, version: "1"}
servers: [{url: http://localhost:8080}]
paths:
  /read:
    get:
      operationId: read
      responses:
        "200": {description: data}
"#,
    )
    .expect("document");
    let input = ProviderSetupInput::from(document);
    assert!(matches!(
        coral_catalog::materialize(DestinationPolicy::default(), &input, &[]),
        MaterializationOutcome::Rejected(_)
    ));
    assert!(matches!(
        coral_catalog::materialize(
            DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
            &input,
            &[]
        ),
        MaterializationOutcome::Ready(_)
    ));
}

#[test]
fn private_http_token_endpoints_require_server_egress_permission() {
    use coral_outbound::NetworkAccess::{Deny, HttpAndHttps, Https};
    let document = OpenApiDocument::parse(r#"{
        "openapi":"3.1.0", "info":{"title":"Sidecar","version":"1"},
        "servers":[{"url":"https://api.example.test"}],
        "security":[{"oauth":["read"]}],
        "components":{"securitySchemes":{"oauth":{"type":"oauth2","flows":{"authorizationCode":{
            "authorizationUrl":"http://sidecar:5000/authorize", "tokenUrl":"http://sidecar:5000/token", "scopes":{"read":"Read"}
        }}}}},
        "paths":{"/read":{"get":{"operationId":"read","responses":{"200":{"description":"data"}}}}}
    }"#).unwrap();
    let review = document.configuration_review().unwrap();
    assert!(review.suggested.authentication.is_some());
    let input = document.configure(review.suggested).unwrap().into();
    for access in [Deny, Https, HttpAndHttps] {
        let outcome = coral_catalog::materialize(DestinationPolicy::new(Deny, access), &input, &[]);
        assert_eq!(
            matches!(outcome, MaterializationOutcome::Ready(_)),
            access == HttpAndHttps
        );
    }
}
