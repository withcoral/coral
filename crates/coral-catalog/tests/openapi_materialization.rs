//! OpenAPI Materialized Operation Catalog acceptance at the public Module seam.
#![allow(
    unused_crate_dependencies,
    reason = "integration targets inherit the library's direct dependency set"
)]
#![expect(
    clippy::indexing_slicing,
    reason = "acceptance fixtures assert known JSON, binding, and map members"
)]
#![expect(
    clippy::format_collect,
    reason = "the tiny dynamic path fixture is clearer as one formatted iterator"
)]

use coral_outbound::DestinationPolicy;
use std::collections::BTreeMap;
use std::fmt::Write as _;

use coral_catalog::{
    AuthenticationConfiguration, CatalogDiagnostic, ExecutionBinding, HttpExecutionBinding,
    HttpParameterLocation, HttpParameterStyle, HttpResponseStatus, MaterializationOutcome,
    MaterializedOperation, OpenApiDocument, OperationAvailability, ProviderSetupInput, materialize,
};
use coral_v8::{DiagnosticCategory, ProgramCompiler};
use serde_json::Value;

const HTTP_ARCHITECTURE_CORPUS: &str = include_str!("fixtures/openapi-http-corpus.yaml");

const HTTP_CORPUS_SLICE: &str = r##"
openapi: 3.1.0
info:
  title: HTTP corpus
  version: "1"
servers:
  - url: https://api.example.test/v1
paths:
  /repos/{owner}/{repo}/issues:
    get:
      operationId: issues/list-for-repo
      summary: List repository issues
      parameters:
        - name: owner
          in: path
          required: true
          schema:
            type: string
            minLength: 1
        - name: repo
          in: path
          required: true
          schema:
            type: string
        - name: labels
          in: query
          style: form
          explode: false
          schema:
            type: array
            items:
              type: string
        - name: Accept
          in: header
          schema:
            type: string
      responses:
        "200":
          description: Issues
          headers:
            Link:
              required: false
              schema:
                type: string
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Issue"
        "422":
          $ref: "#/components/responses/ValidationFailure"
  /assets:
    post:
      operationId: repos/upload-release-asset
      summary: Upload an asset
      servers:
        - url: https://uploads.example.test
      requestBody:
        required: true
        content:
          application/octet-stream:
            schema:
              type: string
              format: binary
      responses:
        "201":
          description: Uploaded asset
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Asset"
components:
  schemas:
    Issue:
      type: object
      required: [id, state]
      properties:
        id:
          type: integer
          minimum: 1
        state:
          enum: [open, closed]
        parent:
          anyOf:
            - $ref: "#/components/schemas/Issue"
            - type: "null"
    Asset:
      type: object
      required: [id]
      properties:
        id:
          type: integer
        digest:
          type: [string, "null"]
  responses:
    ValidationFailure:
      description: Invalid request
      content:
        application/problem+json:
          schema:
            type: object
            required: [message]
            properties:
              message:
                type: string
                maxLength: 1024
"##;

fn ready(document: &str) -> coral_catalog::PreparedProviderCatalog {
    let document = OpenApiDocument::parse(document).expect("bounded OpenAPI fixture");
    match materialize(
        DestinationPolicy::default(),
        &ProviderSetupInput::from(document),
        &[],
    ) {
        MaterializationOutcome::Ready(prepared) => prepared,
        MaterializationOutcome::Rejected(diagnostic) => panic!(
            "fixture should materialize: {}: {}",
            diagnostic.code(),
            diagnostic.message()
        ),
    }
}

fn http_binding(operation: &MaterializedOperation) -> &HttpExecutionBinding {
    let ExecutionBinding::Http(binding) = operation.execution_binding() else {
        panic!("OpenAPI materialization must produce an HTTP Execution Binding");
    };
    binding
}

#[test]
fn corpus_slice_materializes_schema_authority_and_typed_http_bindings() {
    let prepared = ready(HTTP_CORPUS_SLICE);
    assert_eq!(
        prepared.schema_graph().semantics(),
        coral_catalog::SchemaSemantics::JsonSchema2020_12
    );

    let issue_schema_id = prepared
        .schema_graph()
        .schema_id_for_source("#/components/schemas/Issue")
        .expect("Issue schema identity");
    let issue_schema = prepared
        .schema_graph()
        .schema(issue_schema_id)
        .expect("authoritative Issue schema");
    assert_eq!(issue_schema["properties"]["id"]["minimum"], 1);
    assert_eq!(
        issue_schema["properties"]["parent"]["anyOf"][0]["$ref"],
        issue_schema_id.as_str()
    );

    let list = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "issues/list-for-repo")
        .expect("list Operation");
    assert_eq!(
        list.availability(),
        OperationAvailability::Available,
        "{:?}",
        list.diagnostics()
    );
    assert!(list.diagnostic().is_none());
    assert!(
        list.diagnostics()
            .iter()
            .all(|diagnostic| diagnostic.severity()
                == coral_catalog::CatalogDiagnosticSeverity::Warning),
        "schema constraints widen only the TypeScript projection"
    );
    let binding = http_binding(list);
    assert_eq!(binding.server_url(), Some("https://api.example.test/v1"));
    assert_eq!(binding.path_template(), "/repos/{owner}/{repo}/issues");
    let labels = binding
        .parameters()
        .iter()
        .find(|parameter| parameter.wire_name() == "labels")
        .expect("labels query parameter");
    assert_eq!(labels.location(), HttpParameterLocation::Query);
    assert_eq!(labels.style(), Some(HttpParameterStyle::Form));
    assert!(!labels.explode());

    let success = binding
        .responses()
        .iter()
        .find(|response| response.status() == HttpResponseStatus::Code(200))
        .expect("200 response");
    assert_eq!(success.headers()[0].wire_name(), "Link");
    assert_eq!(success.content()[0].media_type(), "application/json");
    let validation = binding
        .responses()
        .iter()
        .find(|response| response.status() == HttpResponseStatus::Code(422))
        .expect("referenced 422 response");
    assert_eq!(
        validation.content()[0].media_type(),
        "application/problem+json"
    );
    assert_eq!(
        Some(validation.content()[0].schema()),
        prepared.schema_graph().schema_id_for_source(
            "#/components/responses/ValidationFailure/content/application~1problem+json/schema"
        )
    );

    let upload = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "repos/upload-release-asset")
        .expect("upload Operation");
    assert_eq!(
        http_binding(upload).server_url(),
        Some("https://uploads.example.test/")
    );
    let body = http_binding(upload).request_body().expect("request body");
    assert!(body.required());
    assert_eq!(body.content()[0].media_type(), "application/octet-stream");

    for operation in prepared.operations() {
        let declaration = operation.typescript_declaration();
        assert!(declaration.contains("Promise<"));
        assert!(!declaration.contains("import "));
    }
    assert!(list.typescript_declaration().contains("type Issue"));
    assert!(
        list.typescript_declaration()
            .lines()
            .any(|line| line.starts_with("type Issue = ") && !line.ends_with(" = unknown;")),
        "productive object recursion must remain typed"
    );
    assert_strict_compiling_declaration(list.typescript_declaration());
}

#[test]
fn reserved_openapi_header_parameters_do_not_become_caller_arguments() {
    let prepared = ready(HTTP_CORPUS_SLICE);
    let list = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "issues/list-for-repo")
        .expect("list Operation");

    assert!(
        !http_binding(list)
            .parameters()
            .iter()
            .any(|parameter| parameter.wire_name() == "Accept")
    );
    assert!(!list.typescript_declaration().contains("Accept"));
}

#[test]
fn http_declarations_preserve_response_alternatives_and_unknown_fallback() {
    let prepared = ready(HTTP_CORPUS_SLICE);
    let list = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "issues/list-for-repo")
        .expect("list Operation");
    let declaration = list.typescript_declaration();

    assert!(declaration.contains("HttpResult<Array<Issue>, 200, \"application/json\">"));
    assert!(declaration.contains(", 422, \"application/problem+json\">"));
    assert!(declaration.contains("| HttpUnknownResult>;"));
    assert_strict_compiling_declaration(declaration);
}

/// The HTTP result metadata as each union branch repeated it before the
/// wrapper was declared once per declaration.
const PREVIOUS_HTTP_METADATA: &str = "{ readonly kind: \"http\"; readonly status: number; readonly headers: readonly (readonly [string, string])[]; readonly mediaType: string | null; readonly charset: string | null; readonly finalUrl: string; readonly redirects: readonly { readonly status: number; readonly url: string; readonly crossOrigin: boolean }[]; readonly decodeDiagnostics: readonly { readonly code: string; readonly message: string }[] }";

/// One union branch in its previous form: the full metadata, narrowed.
fn previous_branch(data: &str, narrowing: &str) -> String {
    format!("{{ data: {data}; metadata: {PREVIOUS_HTTP_METADATA} & {{ {narrowing} }} }}")
}

/// A declared, decoded response in its previous form.
fn previous_typed(data: &str, status: &str, media_type: Option<&str>) -> String {
    let media_type = media_type.map_or_else(String::new, |media_type| {
        format!("readonly mediaType: {media_type}; ")
    });
    previous_branch(
        data,
        &format!("readonly status: {status}; {media_type}readonly decodeDiagnostics: readonly []"),
    )
}

#[test]
fn http_result_declarations_are_mutually_assignable_with_their_previous_form() {
    let json = |schema: Value| serde_json::json!({"application/json": {"schema": schema}});
    let reference =
        |name: &str| serde_json::json!({"$ref": format!("#/components/schemas/{name}")});
    let two_hundreds = (200..300)
        .map(|status| status.to_string())
        .collect::<Vec<_>>()
        .join(" | ");
    let application_json = Some("\"application/json\"");
    let cases = [
        (
            "getBranch",
            serde_json::json!({
                "200": {"description": "ok", "content": json(reference("Branch"))},
                "301": {"description": "moved", "content": json(reference("BasicError"))},
                "404": {"description": "missing", "content": json(reference("BasicError"))}
            }),
            vec![
                previous_typed("BasicError", "301", application_json),
                previous_typed("BasicError", "404", application_json),
                previous_typed("Branch", "200", application_json),
            ],
        ),
        (
            "listNames",
            serde_json::json!({"2XX": {"description": "ok",
                "content": json(serde_json::json!({"type": "array", "items": {"type": "string"}}))}}),
            vec![previous_typed(
                "Array<string>",
                &two_hundreds,
                application_json,
            )],
        ),
        (
            "getWithDefault",
            serde_json::json!({
                "200": {"description": "ok", "content": json(reference("Branch"))},
                "default": {"description": "error", "content": json(reference("BasicError"))}
            }),
            vec![
                previous_typed("BasicError", "number", application_json),
                previous_typed("Branch", "200", application_json),
            ],
        ),
        (
            "readText",
            serde_json::json!({"200": {"description": "ok",
                "content": {"text/*": {"schema": {"type": "string"}}}}}),
            vec![previous_typed("string", "200", Some("string"))],
        ),
        (
            "deleteBranch",
            serde_json::json!({"204": {"description": "deleted"}}),
            vec![previous_typed("null", "204", None)],
        ),
    ];
    let paths = cases
        .iter()
        .map(|(operation_id, responses, _)| {
            (
                format!("/{operation_id}"),
                serde_json::json!({"get": {"operationId": operation_id, "responses": responses}}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "HTTP results", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": paths,
        "components": {"schemas": {
            "Branch": {"type": "object", "required": ["name"],
                "properties": {"name": {"type": "string"}}},
            "BasicError": {"type": "object", "properties": {"message": {"type": "string"}}}
        }}
    });
    let prepared = ready(&document.to_string());
    let unknown = previous_branch(
        "unknown",
        "readonly decodeDiagnostics: readonly [{ readonly code: string; readonly message: string }, ...{ readonly code: string; readonly message: string }[]]",
    );

    for (operation_id, _, typed) in cases {
        let declaration = prepared
            .operations()
            .iter()
            .find(|operation| operation.key().as_str() == operation_id)
            .expect("declared Operation")
            .typescript_declaration();
        assert!(
            !declaration.contains(" & { readonly status"),
            "no branch repeats the wrapper: {declaration}"
        );
        let previous = typed
            .iter()
            .chain([&unknown])
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(" | ");
        assert_strict_compiling_declaration(&format!(
            "{declaration}\n\
             type Previous = {previous};\n\
             type Current = Awaited<ReturnType<typeof {operation_id}>>;\n\
             type Assignable<From extends To, To> = From;\n\
             type PreviousIsCurrent = Assignable<Previous, Current>;\n\
             type CurrentIsPrevious = Assignable<Current, Previous>;"
        ));
    }
}

#[test]
fn multi_media_requests_expose_each_explicit_media_selection() {
    let prepared = ready(HTTP_ARCHITECTURE_CORPUS);
    let markdown = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "markdown/render-raw")
        .expect("multi-media request Operation");
    let declaration = markdown.typescript_declaration();

    assert_eq!(
        http_binding(markdown)
            .request_body()
            .expect("request body")
            .content()
            .iter()
            .map(coral_catalog::HttpContentBinding::media_type)
            .collect::<Vec<_>>(),
        ["text/plain", "text/x-markdown"]
    );
    assert!(
        declaration.contains(
            r#""body": ({ "data": PostMarkdownRawBodyInput; "mediaType": "text/plain" } | { "data": PostMarkdownRawBodyInput2; "mediaType": "text/x-markdown" })"#
        ),
        "each media type is one member of the body union: {declaration}"
    );
    assert_strict_compiling_declaration(declaration);
}

#[test]
fn approved_http_corpus_retains_all_transport_and_schema_shapes() {
    let prepared = ready(HTTP_ARCHITECTURE_CORPUS);
    assert_eq!(prepared.operations().len(), 12);
    assert_eq!(
        prepared
            .operations()
            .iter()
            .filter(|operation| operation.availability() == OperationAvailability::Available)
            .count(),
        11
    );
    let create_customer = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "customers/create")
        .expect("form Operation");
    let form = &http_binding(create_customer)
        .request_body()
        .expect("form body")
        .content()[0];
    assert_eq!(form.media_type(), "application/x-www-form-urlencoded");
    assert_eq!(form.encoding()[0].property_name(), "metadata");
    assert!(!create_customer.typescript_declaration().contains("secret"));
    assert!(
        !create_customer
            .typescript_declaration()
            .contains("server_id")
    );
    assert!(
        create_customer
            .typescript_declaration()
            .contains("Record<string, unknown>")
    );
    assert!(
        !create_customer
            .typescript_declaration()
            .contains("write_token"),
        "write-only provider fields do not leak into response authoring types"
    );

    let avatar = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "users/update-avatar")
        .expect("multipart Operation");
    let multipart = &http_binding(avatar)
        .request_body()
        .expect("multipart body")
        .content()[0];
    assert_eq!(multipart.media_type(), "multipart/form-data");
    assert_eq!(
        multipart.encoding()[0].headers()[0].wire_name(),
        "X-Part-Name"
    );

    let content = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "repos/get-content")
        .expect("multi-representation Operation");
    let content_binding = http_binding(content);
    assert_eq!(content_binding.responses().len(), 3);
    assert_eq!(content_binding.responses()[0].content().len(), 3);
    assert!(
        content
            .typescript_declaration()
            .contains("[number, number]")
    );

    let delete = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "drives/delete-item-content")
        .expect("empty response Operation");
    assert!(
        http_binding(delete)
            .responses()
            .iter()
            .any(
                |response| response.status() == HttpResponseStatus::Code(204)
                    && response.content().is_empty()
            )
    );

    let unsupported = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "unsupported/serialization")
        .expect("unsupported Operation remains cataloged");
    assert_eq!(
        unsupported.availability(),
        OperationAvailability::Unavailable
    );
    assert_eq!(
        unsupported.diagnostic().expect("diagnostic").code(),
        "unsupported_parameter_serialization"
    );
}

#[test]
fn approved_http_corpus_retains_every_named_component_schema() {
    let prepared = ready(HTTP_ARCHITECTURE_CORPUS);
    for name in [
        "Issue",
        "User",
        "Customer",
        "CustomerList",
        "CustomerCreate",
        "ContentFile",
        "Asset",
        "Problem",
        "SarifUpload",
        "SarifReceipt",
        "SarifStatus",
    ] {
        let source = format!("#/components/schemas/{name}");
        let id = prepared
            .schema_graph()
            .schema_id_for_source(&source)
            .unwrap_or_else(|| panic!("{source} is retained"));
        assert!(prepared.schema_graph().schema(id).is_some(), "{source}");
    }
}

#[test]
fn unsupported_serialization_or_media_makes_the_operation_unavailable() {
    let object = "{ type: object, properties: { state: { type: string } } }";
    // (method, the Operation's parameters or request body) → the diagnostic
    // code, or `None` when the Operation stays available
    for (method, request, expected) in [
        // deepObject without explode has no defined encoding.
        (
            "get",
            format!("parameters:\n  - {{ name: filter, in: query, style: deepObject, schema: {object} }}"),
            Some("unsupported_parameter_serialization"),
        ),
        (
            "post",
            format!("requestBody:\n  content:\n    application/x-www-form-urlencoded:\n      schema: {{ type: object, properties: {{ filter: {object} }} }}\n      encoding: {{ filter: {{ style: deepObject }} }}"),
            Some("unsupported_parameter_serialization"),
        ),
        (
            "get",
            "parameters:\n  - { name: filter, in: query, content: { application/xml: { schema: { type: object } } } }".to_string(),
            Some("unsupported_parameter_serialization"),
        ),
        (
            "post",
            "requestBody:\n  content:\n    application/x-www-form-urlencoded:\n      schema: { type: object, properties: { payload: { type: object } } }\n      encoding: { payload: { contentType: application/xml } }".to_string(),
            Some("unsupported_http_request_media_type"),
        ),
        (
            "post",
            format!("requestBody:\n  content:\n    text/xml:\n      schema: {object}"),
            Some("unsupported_http_request_media_type"),
        ),
        // A composed schema that is still a string stays available as text.
        (
            "post",
            "requestBody:\n  content:\n    text/plain:\n      schema: { allOf: [{ type: string }, { minLength: 1 }] }".to_string(),
            None,
        ),
    ] {
        let request = request.replace('\n', "\n      ");
        let prepared = ready(&format!(
            r#"openapi: 3.1.0
info: {{ title: Media, version: "1" }}
servers: [{{ url: https://api.example.test }}]
paths:
  /values:
    {method}:
      operationId: values
      {request}
      responses: {{ "200": {{ description: ok }} }}
"#
        ));
        let operation = prepared.operations().first().expect("Operation");
        let availability = if expected.is_some() {
            OperationAvailability::Unavailable
        } else {
            OperationAvailability::Available
        };
        assert_eq!(
            (
                operation.availability(),
                operation
                    .diagnostic()
                    .map(coral_catalog::CatalogDiagnostic::code)
            ),
            (availability, expected),
            "{request}"
        );
    }
}

#[test]
fn structured_enum_values_widen_without_making_operation_unavailable() {
    let prepared = ready(
        r#"openapi: 3.1.0
info:
  title: Structured enum
  version: "1"
servers:
  - url: https://api.example.test
paths:
  /state:
    get:
      operationId: getStructuredState
      responses:
        "200":
          description: state
          content:
            application/json:
              schema:
                enum:
                  - kind: open
"#,
    );
    let operation = prepared.operations().first().expect("Operation");
    assert_eq!(operation.availability(), OperationAvailability::Available);
    assert!(operation.typescript_declaration().contains("data: unknown"));
    assert!(
        operation
            .diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.code() == "typescript_literal_widened")
    );
}

#[test]
fn openapi_30_nullable_enum_includes_null() {
    let prepared = ready(
        r#"openapi: 3.0.3
info:
  title: Nullable enum
  version: "1"
servers:
  - url: https://api.example.test
paths:
  /state:
    get:
      operationId: getState
      responses:
        "200":
          description: state
          content:
            application/json:
              schema:
                type: string
                enum: [open]
                nullable: true
"#,
    );
    assert_eq!(
        prepared.schema_graph().semantics(),
        coral_catalog::SchemaSemantics::OpenApiV3_0
    );
    let operation = &prepared.operations()[0];
    assert!(
        operation
            .typescript_declaration()
            .contains("\"open\" | null")
    );
    let schema = http_binding(operation).responses()[0].content()[0].schema();
    assert!(prepared.schema_graph().accepts(schema, &Value::Null));
    assert!(
        prepared
            .schema_graph()
            .accepts(schema, &Value::from("open"))
    );
    assert!(
        !prepared
            .schema_graph()
            .accepts(schema, &Value::from("closed"))
    );
}

#[test]
fn typed_additional_properties_never_narrow_named_properties() {
    let prepared = ready(
        r#"openapi: 3.1.0
info:
  title: Typed additional properties
  version: "1"
servers:
  - url: https://api.example.test
paths:
  /counts:
    get:
      operationId: getCounts
      responses:
        "200":
          description: counts
          content:
            application/json:
              schema:
                type: object
                required: [count]
                properties:
                  count:
                    type: number
                additionalProperties:
                  type: string
"#,
    );
    let operation = prepared.operations().first().expect("Operation");
    let declaration = operation.typescript_declaration();
    assert!(declaration.contains("\"count\": number"));
    assert!(declaration.contains("Record<string, unknown>"));
    assert!(!declaration.contains("Record<string, string>"));
    assert!(
        operation
            .diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.code() == "typescript_additional_properties_widened")
    );
}

#[test]
fn referenced_path_item_cannot_bypass_credential_contract_validation() {
    let document = OpenApiDocument::parse(
        r##"openapi: 3.1.0
info:
  title: Referenced credentials
  version: "1"
servers:
  - url: https://api.example.test
paths:
  /secret:
    $ref: "#/components/pathItems/Secured"
components:
  securitySchemes:
    bearer:
      type: http
      scheme: digest
  pathItems:
    Secured:
      get:
        operationId: secured
        security:
          - bearer: []
        responses:
          "200":
            description: ok
"##,
    )
    .expect("referenced Path Item fixture");

    let MaterializationOutcome::Rejected(diagnostic) = materialize(
        DestinationPolicy::default(),
        &ProviderSetupInput::from(document),
        &[],
    ) else {
        panic!("referenced Path Item must reject an unsupported credential contract");
    };
    assert_eq!(diagnostic.code(), "credentials_not_supported");
}

/// A document whose `/probe` Operation uses `UserAuth` through the root
/// security and whose `/admin/probe` Operation names `AdminAuth` itself.
fn two_scheme_document(user: (&str, &str), admin: (&str, &str)) -> String {
    let (user_scheme, user_scopes) = user;
    let (admin_scheme, admin_scopes) = admin;
    format!(
        r#"openapi: 3.1.0
info: {{ title: Two credential schemes, version: "1" }}
servers: [{{ url: https://api.example.test }}]
security:
  - UserAuth: [{user_scopes}]
paths:
  /probe:
    get:
      operationId: probe
      responses: {{ "200": {{ description: ok }} }}
  /admin/probe:
    get:
      operationId: adminProbe
      security:
        - AdminAuth: [{admin_scopes}]
      responses: {{ "200": {{ description: ok }} }}
components:
  securitySchemes:
    UserAuth: {user_scheme}
    AdminAuth: {admin_scheme}
"#
    )
}

fn oauth_scopes(prepared: &coral_catalog::PreparedProviderCatalog) -> Vec<String> {
    match prepared.credential_contract().authentication() {
        AuthenticationConfiguration::OAuthAuthorizationCode { scopes, .. } => scopes.clone(),
        _ => Vec::new(),
    }
}

#[test]
fn operations_share_one_oauth_grant_up_to_the_scope_limit() {
    // The scheme declares 101 scopes; each Operation asks for 60 of them.
    let declared = (0..=100)
        .map(|index| format!("s{index}: s{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let oauth = format!(
        r#"{{ type: oauth2, flows: {{ authorizationCode: {{ authorizationUrl: "https://auth.example.test/authorize", tokenUrl: "https://auth.example.test/token", scopes: {{ {declared} }} }} }} }}"#
    );
    let scope_list = |range: std::ops::RangeInclusive<usize>| {
        range
            .map(|index| format!("s{index}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    for (case, admin_scopes, union_size) in [
        ("union within the limit", scope_list(40..=99), Some(100)),
        ("union over the limit", scope_list(41..=100), None),
    ] {
        let document = OpenApiDocument::parse(&two_scheme_document(
            (&oauth, &scope_list(0..=59)),
            (&oauth, &admin_scopes),
        ))
        .expect("overlapping scope fixture");
        match materialize(
            DestinationPolicy::default(),
            &ProviderSetupInput::from(document),
            &[],
        ) {
            MaterializationOutcome::Ready(prepared) => {
                assert_eq!(Some(oauth_scopes(&prepared).len()), union_size, "{case}");
            }
            MaterializationOutcome::Rejected(diagnostic) => {
                assert!(union_size.is_none(), "{case}: {}", diagnostic.message());
                assert_eq!(diagnostic.code(), "invalid_credential_contract", "{case}");
            }
        }
    }
}

#[test]
fn operations_agree_on_credential_contracts_not_security_scheme_names() {
    let bearer = "{ type: http, scheme: bearer }";
    let api_key = "{ type: apiKey, in: header, name: X-Admin-Key }";
    let oauth = r#"{ type: oauth2, flows: { authorizationCode: { authorizationUrl: "https://auth.example.test/authorize", tokenUrl: "https://auth.example.test/token", scopes: { read: r, admin: a } } } }"#;
    let other_oauth = r#"{ type: oauth2, flows: { authorizationCode: { authorizationUrl: "https://other.example.test/authorize", tokenUrl: "https://other.example.test/token", scopes: { read: r, admin: a } } } }"#;
    // The scopes the one shared contract requests; `None` means the
    // Operations need separate credentials.
    let cases = [
        (
            "identical bearer schemes",
            (bearer, ""),
            (bearer, ""),
            Some(&[][..]),
        ),
        ("bearer and API key", (bearer, ""), (api_key, ""), None),
        (
            "one OAuth scheme, different scopes",
            (oauth, "read"),
            (oauth, "admin"),
            Some(&["admin", "read"][..]),
        ),
        (
            "one OAuth scheme, overlapping scopes",
            (oauth, "read, admin"),
            (oauth, "admin"),
            Some(&["admin", "read"][..]),
        ),
        (
            "one OAuth scheme, one Operation without scopes",
            (oauth, "read"),
            (oauth, ""),
            Some(&["read"][..]),
        ),
        (
            "two OAuth authorization servers, same scopes",
            (oauth, "read"),
            (other_oauth, "read"),
            None,
        ),
    ];
    for (case, user, admin, shared) in cases {
        let document =
            OpenApiDocument::parse(&two_scheme_document(user, admin)).expect("credential fixture");
        match materialize(
            DestinationPolicy::default(),
            &ProviderSetupInput::from(document),
            &[],
        ) {
            MaterializationOutcome::Ready(prepared) if shared.is_some() => {
                assert_eq!(
                    oauth_scopes(&prepared),
                    shared.unwrap_or_default(),
                    "{case}"
                );
                for operation in prepared.operations() {
                    assert_eq!(operation.availability(), OperationAvailability::Available);
                    let binding = http_binding(operation);
                    let [slot] = binding.credential_slots() else {
                        panic!("{case}: one credential slot");
                    };
                    assert_eq!(slot.contract(), &prepared.credential_contract(), "{case}");
                    let own_scheme = if binding.path_template() == "/probe" {
                        "UserAuth"
                    } else {
                        "AdminAuth"
                    };
                    assert_eq!(slot.scheme_name(), own_scheme, "{case}");
                }
            }
            MaterializationOutcome::Rejected(diagnostic) if shared.is_none() => {
                assert_eq!(diagnostic.code(), "credentials_not_supported", "{case}");
                assert_eq!(
                    diagnostic.message(),
                    "the Provider's Operations require different credential contracts \
                     at #/paths/~1probe/get: UserAuth differs from AdminAuth",
                    "{case}"
                );
            }
            MaterializationOutcome::Ready(_) => panic!("{case}: must need separate credentials"),
            MaterializationOutcome::Rejected(diagnostic) => {
                panic!(
                    "{case}: must share one credential: {}",
                    diagnostic.message()
                )
            }
        }
    }
}

#[test]
fn operation_local_server_limitation_does_not_block_valid_siblings() {
    let prepared = ready(
        r#"openapi: 3.1.0
info:
  title: Operation server isolation
  version: "1"
servers:
  - url: https://api.example.test
paths:
  /good:
    get:
      operationId: good
      responses:
        "200":
          description: ok
  /unsupported:
    get:
      operationId: unsupportedServers
      servers:
        - url: https://one.example.test
        - url: https://two.example.test
      responses:
        "200":
          description: still cataloged
"#,
    );
    let good = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "good")
        .expect("valid sibling");
    let unsupported = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "unsupportedServers")
        .expect("unsupported server Operation");
    assert_eq!(good.availability(), OperationAvailability::Available);
    assert_eq!(
        unsupported.availability(),
        OperationAvailability::Unavailable
    );
    assert_eq!(http_binding(unsupported).server_url(), None);
    assert_eq!(
        unsupported.diagnostic().expect("server diagnostic").code(),
        "unsupported_server_selection"
    );
}

#[test]
fn operation_server_override_does_not_depend_on_invalid_inherited_servers() {
    for inherited_servers in [
        "servers: []\npaths:\n  /value:\n",
        "paths:\n  /value:\n    servers: []\n",
    ] {
        let document = format!(
            r#"openapi: 3.1.0
info:
  title: Server precedence
  version: "1"
{inherited_servers}    get:
      operationId: value
      servers:
        - url: https://operation.example.test
      responses:
        "200":
          description: ok
"#
        );
        let prepared = ready(&document);
        assert_eq!(
            http_binding(&prepared.operations()[0]).server_url(),
            Some("https://operation.example.test/")
        );
    }
}

#[test]
fn operation_paths_carry_forward_for_active_keys_and_releases_removed_claims() {
    let first = ready(&path_fixture(&["foo-bar", "foo_bar", "stable"]));
    let first_paths = operation_paths(&first);
    assert_eq!(first_paths.len(), 3);
    assert_eq!(
        first_paths
            .values()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        3,
        "normalized collisions receive deterministic suffixes"
    );

    let replacement_document =
        OpenApiDocument::parse(&path_fixture(&["foo_bar", "foo-bar", "stable", "foo bar"]))
            .expect("replacement fixture");
    let replacement = match materialize(
        DestinationPolicy::default(),
        &ProviderSetupInput::from(replacement_document),
        &first.operation_paths(),
    ) {
        MaterializationOutcome::Ready(prepared) => prepared,
        MaterializationOutcome::Rejected(diagnostic) => panic!(
            "replacement should materialize: {}: {}",
            diagnostic.code(),
            diagnostic.message()
        ),
    };
    let replacement_paths = operation_paths(&replacement);
    for key in ["foo-bar", "foo_bar", "stable"] {
        assert_eq!(replacement_paths[key], first_paths[key]);
    }

    let without_first_owner =
        OpenApiDocument::parse(&path_fixture(&["foo_bar", "stable", "foo@bar"]))
            .expect("removal fixture");
    let after_removal = match materialize(
        DestinationPolicy::default(),
        &ProviderSetupInput::from(without_first_owner),
        &replacement.operation_paths(),
    ) {
        MaterializationOutcome::Ready(prepared) => prepared,
        MaterializationOutcome::Rejected(diagnostic) => panic!(
            "removal should materialize: {}: {}",
            diagnostic.code(),
            diagnostic.message()
        ),
    };
    let after_removal_paths = operation_paths(&after_removal);
    assert_eq!(after_removal_paths["foo_bar"], replacement_paths["foo_bar"]);
    assert_eq!(
        after_removal_paths["foo@bar"], first_paths["foo-bar"],
        "a removed key no longer reserves its Operation-relative path"
    );
}

#[test]
fn unsupported_operation_remains_cataloged_without_blocking_valid_operations() {
    let document = HTTP_CORPUS_SLICE.replace(
        "components:\n",
        r#"  /unsupported:
    get:
      operationId: structurallyUnsupported
      parameters:
        - name: canary-secret-parameter
          in: query
          style: simple
          schema:
            type: object
      responses:
        "200":
          description: Unsupported response
          content:
            application/json:
              schema:
                type: object
                required: [value]
                properties:
                  value:
                    type: string
components:
"#,
    );
    let prepared = ready(&document);
    assert_eq!(prepared.operations().len(), 3);

    let unsupported = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "structurallyUnsupported")
        .expect("unsupported Operation remains in Reef catalog material");
    assert_eq!(
        unsupported.availability(),
        OperationAvailability::Unavailable
    );
    let diagnostic = unsupported.diagnostic().expect("safe blocking diagnostic");
    assert_eq!(diagnostic.code(), "unsupported_parameter_serialization");
    assert!(!diagnostic.message().contains("canary"));
    assert!(
        prepared
            .schema_graph()
            .schema_id_for_source("#/paths/~1unsupported/get/parameters/0/schema")
            .is_some(),
        "valid schemas remain in the complete graph even when their Operation binding is unsupported"
    );
    assert!(
        prepared
            .schema_graph()
            .schema_id_for_source(
                "#/paths/~1unsupported/get/responses/200/content/application~1json/schema"
            )
            .is_some(),
        "binding failure does not prevent later response schemas from entering the graph"
    );
    assert!(
        prepared
            .operations()
            .iter()
            .filter(|operation| operation.key().as_str() != "structurallyUnsupported")
            .all(|operation| operation.availability() == OperationAvailability::Available)
    );
}

#[test]
fn unrelated_ref_fields_do_not_hide_schemas_in_concrete_openapi_objects() {
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Concrete object refs", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {"/value": {"get": {
            "$ref": "not-an-operation-reference",
            "operationId": "value",
            "parameters": [{
                "name": "unsupported",
                "in": "query",
                "style": "simple",
                "schema": {"type": "string"}
            }],
            "responses": {"200": {
                "description": "ok",
                "content": {"application/json": {
                    "$ref": "not-a-media-type-reference",
                    "schema": {"type": "integer"}
                }}
            }}
        }}}
    });
    let prepared = ready(&document.to_string());
    assert_eq!(
        prepared.operations()[0].availability(),
        OperationAvailability::Unavailable
    );
    for source in [
        "#/paths/~1value/get/parameters/0/schema",
        "#/paths/~1value/get/responses/200/content/application~1json/schema",
    ] {
        assert!(
            prepared
                .schema_graph()
                .schema_id_for_source(source)
                .is_some(),
            "missing schema at {source}"
        );
    }
}

#[test]
fn malformed_inline_schema_is_unavailable_and_excluded_from_the_coherent_graph() {
    let document = HTTP_CORPUS_SLICE.replace(
        "components:\n",
        r#"  /malformed-schema:
    get:
      operationId: malformedSchema
      responses:
        "200":
          description: malformed
          content:
            application/json:
              schema:
                type: 5
components:
"#,
    );
    let prepared = ready(&document);
    let malformed = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "malformedSchema")
        .expect("malformed Operation remains cataloged");
    assert_eq!(malformed.availability(), OperationAvailability::Unavailable);
    assert_eq!(
        malformed.diagnostic().expect("schema diagnostic").code(),
        "invalid_operation_schema"
    );
    assert!(
        prepared
            .schema_graph()
            .schema_id_for_source(
                "#/paths/~1malformed-schema/get/responses/200/content/application~1json/schema"
            )
            .is_none(),
        "invalid inline roots do not enter the coherent schema graph"
    );
}

#[test]
fn duplicated_required_entries_are_deduplicated_instead_of_rejecting_the_document() {
    let prepared = ready(
        r##"openapi: 3.1.0
info:
  title: Duplicate required
  version: "1"
servers:
  - url: https://api.example.test
paths:
  /probe:
    get:
      operationId: probe
      responses:
        "200":
          description: ok
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Probe"
components:
  schemas:
    Probe:
      type: object
      required: [id, name, id]
      properties:
        id:
          type: string
        name:
          type: string
"##,
    );
    let operation = prepared.operations().first().expect("Operation");
    assert_eq!(operation.availability(), OperationAvailability::Available);
    assert!(
        operation.typescript_declaration().contains(
            r#"type Probe = ({ "id": string; "name": string } & Record<string, unknown>);"#
        )
    );
    let graph = prepared.schema_graph();
    let probe = graph
        .schema_id_for_source("#/components/schemas/Probe")
        .expect("Probe enters the coherent schema graph");
    assert_eq!(
        graph.schema(probe).expect("Probe schema")["required"],
        serde_json::json!(["id", "name"])
    );
    assert!(graph.accepts(probe, &serde_json::json!({ "id": "a", "name": "b" })));
    assert!(!graph.accepts(probe, &serde_json::json!({ "id": "a" })));
}

#[test]
fn equivalent_schema_pointers_share_identity_and_literal_references_stay_literal() {
    let literal_reference = "#/components/schemas/not-semantic";
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Canonical references", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {
            "/value": {
                "get": {
                    "operationId": "getValue",
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {"application/json": {"schema": {
                                "$ref": "#%2Fcomponents%2Fschemas%2FCaf%C3%A9~1Part~0"
                            }}}
                        }
                    }
                }
            }
        },
        "components": {"schemas": {
            "Café/Part~": {"type": "string"},
            "Alias": {"$ref": "#/components/schemas/Caf%C3%A9~1Part~0"},
            "Percent%Value": {"type": "number"},
            "PercentAlias": {"$ref": "#/components/schemas/Percent%25Value"},
            "Literals": {
                "type": "object",
                "example": {"$ref": literal_reference},
                "examples": [{"$ref": literal_reference}],
                "default": {"$ref": literal_reference},
                "enum": [{"$ref": literal_reference}],
                "const": {"$ref": literal_reference},
                "x-literal": {"$ref": literal_reference}
            }
        }}
    });
    let prepared = ready(&document.to_string());
    let target_source = "#/components/schemas/Café~1Part~0";
    let target_id = prepared
        .schema_graph()
        .schema_id_for_source(target_source)
        .expect("canonical target identity");
    let alias_id = prepared
        .schema_graph()
        .schema_id_for_source("#/components/schemas/Alias")
        .expect("alias identity");
    assert_eq!(
        prepared.schema_graph().schema(alias_id).expect("alias")["$ref"],
        target_id.as_str()
    );
    let response_id = http_binding(&prepared.operations()[0]).responses()[0].content()[0].schema();
    assert_eq!(
        prepared
            .schema_graph()
            .schema(response_id)
            .expect("response schema")["$ref"],
        target_id.as_str()
    );
    let percent_id = prepared
        .schema_graph()
        .schema_id_for_source("#/components/schemas/Percent%Value")
        .expect("decoded percent identity");
    let percent_alias_id = prepared
        .schema_graph()
        .schema_id_for_source("#/components/schemas/PercentAlias")
        .expect("percent alias identity");
    assert_eq!(
        prepared
            .schema_graph()
            .schema(percent_alias_id)
            .expect("percent alias")["$ref"],
        percent_id.as_str()
    );

    let literals_id = prepared
        .schema_graph()
        .schema_id_for_source("#/components/schemas/Literals")
        .expect("literal schema identity");
    let literals = prepared
        .schema_graph()
        .schema(literals_id)
        .expect("literal schema");
    for pointer in [
        "/example/$ref",
        "/examples/0/$ref",
        "/default/$ref",
        "/enum/0/$ref",
        "/const/$ref",
        "/x-literal/$ref",
    ] {
        assert_eq!(
            literals
                .pointer(pointer)
                .and_then(serde_json::Value::as_str),
            Some(literal_reference)
        );
    }
}

#[test]
fn invalid_schema_reference_forms_are_safe_operation_local_failures() {
    let cases = [
        (
            "schema_000000000000000000000000",
            "invalid_schema_reference",
        ),
        (
            "other.yaml#/components/schemas/Value",
            "unsupported_external_schema_reference",
        ),
        ("#named-anchor", "unsupported_schema_reference_scope"),
        (
            "#/components/schemas/Holder~2bad",
            "invalid_schema_reference",
        ),
        (
            "#/components/schemas/Holder/example",
            "invalid_schema_reference",
        ),
        (
            "#/components/schemas/Holder/default",
            "invalid_schema_reference",
        ),
        (
            "#/components/schemas/Holder/examples/0",
            "invalid_schema_reference",
        ),
        (
            "#/components/schemas/Holder/enum/0",
            "invalid_schema_reference",
        ),
        (
            "#/components/schemas/Holder/const",
            "invalid_schema_reference",
        ),
        (
            "#/components/schemas/Holder/x-data",
            "invalid_schema_reference",
        ),
        ("#/info", "invalid_schema_reference"),
        ("#/components/schemas/Missing", "invalid_schema_reference"),
    ];
    for (reference, expected_code) in cases {
        let document = reference_fixture("3.1.0", reference);
        let prepared = ready(&document);
        let operation = &prepared.operations()[0];
        assert_eq!(
            operation.availability(),
            OperationAvailability::Unavailable,
            "{reference}"
        );
        assert_eq!(
            operation.diagnostic().expect("safe diagnostic").code(),
            expected_code,
            "{reference}"
        );
        assert!(
            prepared
                .schema_graph()
                .schema_id_for_source(
                    "#/paths/~1value/get/responses/200/content/application~1json/schema"
                )
                .is_none(),
            "an invalid provider schema must not enter the package"
        );
    }
}

#[test]
fn openapi_versions_apply_their_ref_sibling_semantics() {
    let openapi_30 = ready(&reference_fixture_with_sibling(
        "3.0.3",
        serde_json::json!(5),
    ));
    let operation_30 = &openapi_30.operations()[0];
    assert_eq!(
        operation_30.availability(),
        OperationAvailability::Available
    );
    let schema_id_30 = http_binding(operation_30).responses()[0].content()[0].schema();
    let schema_30 = openapi_30
        .schema_graph()
        .schema(schema_id_30)
        .expect("normalized OpenAPI 3.0 reference");
    assert_eq!(schema_30.as_object().map(serde_json::Map::len), Some(1));
    let holder = openapi_30
        .schema_graph()
        .schema_id_for_source("#/components/schemas/Holder")
        .expect("reference target");
    assert_eq!(schema_30["$ref"], holder.as_str());
    assert_eq!(
        openapi_30.schema_graph().schema(holder).expect("target")["type"],
        "object"
    );

    let openapi_31 = ready(&reference_fixture_with_sibling(
        "3.1.0",
        serde_json::json!(5),
    ));
    assert_eq!(
        openapi_31.operations()[0].availability(),
        OperationAvailability::Unavailable
    );
    assert_eq!(
        openapi_31.operations()[0]
            .diagnostic()
            .expect("3.1 sibling diagnostic")
            .code(),
        "invalid_operation_schema"
    );
}

#[test]
fn schema_dialect_and_scope_failures_use_stable_diagnostics() {
    for dialect in [
        "https://spec.openapis.org/oas/3.1/dialect/base#",
        "https://json-schema.org/draft/2020-12/schema#",
    ] {
        let mut document: serde_json::Value =
            serde_json::from_str(&reference_fixture("3.1.0", "#/components/schemas/Holder"))
                .expect("fixture JSON");
        document["jsonSchemaDialect"] = serde_json::json!(dialect);
        let prepared = ready(&document.to_string());
        assert_eq!(
            prepared.operations()[0].availability(),
            OperationAvailability::Available
        );
    }

    let mut unsupported_root: serde_json::Value =
        serde_json::from_str(&reference_fixture("3.1.0", "#/components/schemas/Holder"))
            .expect("fixture JSON");
    unsupported_root["jsonSchemaDialect"] = serde_json::json!("https://example.test/dialect");
    let document = OpenApiDocument::parse(&unsupported_root.to_string()).expect("bounded fixture");
    let MaterializationOutcome::Rejected(diagnostic) = materialize(
        DestinationPolicy::default(),
        &ProviderSetupInput::from(document),
        &[],
    ) else {
        panic!("unsupported root dialect must reject publication");
    };
    assert_eq!(diagnostic.code(), "unsupported_schema_dialect");

    for (keyword, value) in [
        ("$id", serde_json::json!("https://example.test/schema")),
        ("$anchor", serde_json::json!("value")),
        ("$dynamicRef", serde_json::json!("#value")),
        ("$dynamicAnchor", serde_json::json!("value")),
        ("$recursiveRef", serde_json::json!("#")),
        ("$recursiveAnchor", serde_json::json!(true)),
    ] {
        let prepared = ready(&schema_keyword_fixture("3.1.0", keyword, value));
        let diagnostic = prepared.operations()[0]
            .diagnostic()
            .expect("scope diagnostic");
        assert_eq!(
            diagnostic.code(),
            "unsupported_schema_reference_scope",
            "{keyword}"
        );
        assert!(
            diagnostic.message().contains(&format!("`{keyword}`")),
            "diagnostic must name the keyword: {}",
            diagnostic.message()
        );
    }
    let anchor_reference = ready(&reference_fixture("3.1.0", "#named-anchor"));
    let diagnostic = anchor_reference.operations()[0]
        .diagnostic()
        .expect("anchor reference diagnostic");
    assert!(
        diagnostic.message().contains("anchor"),
        "diagnostic must name the anchor reference: {}",
        diagnostic.message()
    );
    let openapi_30 = ready(&schema_keyword_fixture(
        "3.0.3",
        "$schema",
        serde_json::json!("https://json-schema.org/draft/2020-12/schema"),
    ));
    assert_eq!(
        openapi_30.operations()[0]
            .diagnostic()
            .expect("OpenAPI 3.0 dialect diagnostic")
            .code(),
        "unsupported_schema_dialect"
    );
    let vocabulary = ready(&schema_keyword_fixture(
        "3.1.0",
        "$vocabulary",
        serde_json::json!({"https://example.test/vocabulary": false}),
    ));
    assert_eq!(
        vocabulary.operations()[0].availability(),
        OperationAvailability::Available
    );
}

#[test]
fn shared_non_schema_component_failure_is_applied_to_each_operation_owner() {
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Shared malformed parameter", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {
            "/one": {"get": {
                "operationId": "one",
                "parameters": [{"$ref": "#/components/parameters/Bad"}],
                "responses": {"200": {"description": "ok"}}
            }},
            "/two": {"get": {
                "operationId": "two",
                "parameters": [{"$ref": "#/components/parameters/Bad"}],
                "responses": {"200": {"description": "ok"}}
            }}
        },
        "components": {"parameters": {"Bad": {
            "name": "value", "in": "query", "schema": {"type": 5}
        }}}
    });
    let prepared = ready(&document.to_string());
    assert!(prepared.operations().iter().all(|operation| {
        operation.availability() == OperationAvailability::Unavailable
            && operation
                .diagnostic()
                .is_some_and(|diagnostic| diagnostic.code() == "invalid_operation_schema")
    }));
    assert!(
        prepared
            .schema_graph()
            .schema_id_for_source("#/components/parameters/Bad/schema")
            .is_none()
    );
}

#[test]
fn local_schema_failures_do_not_hide_reference_edges_from_scc_analysis() {
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Invalid reference cycle", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {
            "/a": {"get": {
                "operationId": "a",
                "parameters": [{"$ref": "#/components/parameters/A"}],
                "responses": {"200": {"description": "ok"}}
            }},
            "/b": {"get": {
                "operationId": "b",
                "parameters": [{"$ref": "#/components/parameters/B"}],
                "responses": {"200": {"description": "ok"}}
            }}
        },
        "components": {"parameters": {
            "A": {"name": "a", "in": "query", "schema": {
                "$id": "https://example.test/a",
                "$ref": "#/components/parameters/B/schema"
            }},
            "B": {"name": "b", "in": "query", "schema": {
                "$schema": "https://example.test/unsupported-dialect",
                "$ref": "#/components/parameters/A/schema"
            }}
        }}
    });
    let prepared = ready(&document.to_string());
    let diagnostics = prepared
        .operations()
        .iter()
        .map(|operation| operation.diagnostic().expect("cycle failure").code())
        .collect::<Vec<_>>();
    assert_eq!(
        diagnostics,
        vec![
            "unsupported_schema_reference_scope",
            "unsupported_schema_reference_scope"
        ]
    );
}

#[test]
fn schema_cycles_propagate_failures_and_closed_cycles_remain_package_local() {
    let invalid = ready(&cycle_fixture(true));
    assert!(invalid.operations().iter().all(|operation| {
        operation.availability() == OperationAvailability::Unavailable
            && operation
                .diagnostic()
                .is_some_and(|diagnostic| diagnostic.code() == "invalid_operation_schema")
    }));

    let valid = ready(&cycle_fixture(false));
    assert!(
        valid
            .operations()
            .iter()
            .all(|operation| operation.availability() == OperationAvailability::Available)
    );
    for source in [
        "#/components/parameters/A/schema",
        "#/components/parameters/B/schema",
    ] {
        let id = valid
            .schema_graph()
            .schema_id_for_source(source)
            .expect("cycle member identity");
        assert_package_local_schema_references(
            valid
                .schema_graph()
                .schema(id)
                .expect("cycle member schema"),
        );
    }
    for operation in valid.operations() {
        assert!(
            direct_type_aliases_are_acyclic(operation.typescript_declaration()),
            "declaration contains a circular direct alias:\n{}",
            operation.typescript_declaration()
        );
        assert_strict_compiling_declaration(operation.typescript_declaration());
    }
}

#[test]
fn unreferenced_failing_component_schema_does_not_block_other_operations() {
    let prepared = ready(&failing_component_fixture(false));

    assert_eq!(prepared.operations().len(), 2);
    assert!(
        prepared
            .operations()
            .iter()
            .all(|operation| operation.availability() == OperationAvailability::Available)
    );
    assert!(
        prepared
            .schema_graph()
            .schema_id_for_source("#/components/schemas/CompoundFilter")
            .is_none(),
        "a failing component schema must not enter the package"
    );
}

#[test]
fn failing_component_schema_blocks_only_operations_that_reach_it() {
    let prepared = ready(&failing_component_fixture(true));

    let availability = prepared
        .operations()
        .iter()
        .map(|operation| (operation.key().as_str(), operation.availability()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        availability,
        BTreeMap::from([
            ("health", OperationAvailability::Available),
            ("listFiles", OperationAvailability::Available),
            ("search", OperationAvailability::Unavailable),
        ])
    );
    let search = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "search")
        .expect("search Operation");
    let diagnostic = search.diagnostic().expect("scope diagnostic");
    assert_eq!(diagnostic.code(), "unsupported_schema_reference_scope");
    assert!(
        diagnostic
            .message()
            .contains("#/components/schemas/CompoundFilter"),
        "diagnostic must name the failing schema: {}",
        diagnostic.message()
    );
}

#[test]
fn component_schema_faults_are_local_to_the_operations_that_reach_them() {
    let cases = [
        (
            "component schema",
            serde_json::json!({"Broken": {"type": 5}}),
            "invalid_operation_schema",
        ),
        (
            "schema graph",
            serde_json::json!({
                "Broken": {"$ref": "#/components/schemas/Target/properties"},
                "Target": {"type": "object", "properties": {"value": {"type": "string"}}}
            }),
            "invalid_schema_reference",
        ),
    ];
    for (name, schemas, expected_code) in cases {
        let document = serde_json::json!({
            "openapi": "3.1.0",
            "info": {"title": "Broken component", "version": "1"},
            "servers": [{"url": "https://api.example.test"}],
            "paths": {
                "/broken": {"get": {
                    "operationId": "broken",
                    "responses": {"200": {
                        "description": "ok",
                        "content": {"application/json": {
                            "schema": {"$ref": "#/components/schemas/Broken"}
                        }}
                    }}
                }},
                "/value": {"get": {
                    "operationId": "value",
                    "responses": {"200": {"description": "ok"}}
                }}
            },
            "components": {"schemas": schemas}
        });
        let prepared = ready(&document.to_string());
        let outcomes = prepared
            .operations()
            .iter()
            .map(|operation| {
                (
                    operation.key().as_str(),
                    operation.diagnostic().map(CatalogDiagnostic::code),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            outcomes,
            BTreeMap::from([("broken", Some(expected_code)), ("value", None)]),
            "{name}"
        );
    }
}

#[test]
fn productive_array_recursion_remains_strict_compiling_and_typed() {
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Recursive array", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {"/value": {"get": {
            "operationId": "value",
            "responses": {"200": {
                "description": "ok",
                "content": {"application/json": {"schema": {
                    "$ref": "#/components/schemas/RecursiveArray"
                }}}
            }}
        }}},
        "components": {"schemas": {"RecursiveArray": {
            "type": "array",
            "items": {"$ref": "#/components/schemas/RecursiveArray"}
        }}}
    });
    let prepared = ready(&document.to_string());
    let operation = &prepared.operations()[0];
    assert_eq!(operation.availability(), OperationAvailability::Available);
    assert!(
        operation
            .typescript_declaration()
            .contains("type RecursiveArray = Array<RecursiveArray>;")
    );
    assert_strict_compiling_declaration(operation.typescript_declaration());
}

#[test]
fn productive_map_recursion_remains_strict_compiling_and_typed() {
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Recursive map", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {"/value": {"get": {
            "operationId": "value",
            "responses": {"200": {
                "description": "ok",
                "content": {"application/json": {"schema": {
                    "$ref": "#/components/schemas/RecursiveMap"
                }}}
            }}
        }}},
        "components": {"schemas": {"RecursiveMap": {
            "type": "object",
            "additionalProperties": {"$ref": "#/components/schemas/RecursiveMap"}
        }}}
    });
    let prepared = ready(&document.to_string());
    let operation = &prepared.operations()[0];
    assert_eq!(operation.availability(), OperationAvailability::Available);
    assert!(
        operation
            .typescript_declaration()
            .contains("type RecursiveMap = { [key: string]: RecursiveMap };")
    );
    assert!(
        !operation
            .diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.code() == "typescript_schema_widened")
    );
    assert_strict_compiling_declaration(operation.typescript_declaration());
}

#[test]
fn composed_reference_cycles_widen_to_strict_compiling_types() {
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Composed reference cycle", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {"/value": {"get": {
            "operationId": "value",
            "parameters": [{"$ref": "#/components/parameters/A"}],
            "responses": {"200": {"description": "ok"}}
        }}},
        "components": {"parameters": {
            "A": {"name": "value", "in": "query", "schema": {
                "allOf": [{"$ref": "#/components/parameters/B/schema"}]
            }},
            "B": {"name": "other", "in": "query", "schema": {
                "allOf": [{"$ref": "#/components/parameters/A/schema"}]
            }}
        }}
    });
    let prepared = ready(&document.to_string());
    let operation = &prepared.operations()[0];
    assert_eq!(operation.availability(), OperationAvailability::Available);
    let aliases = operation
        .typescript_declaration()
        .lines()
        .filter(|line| {
            line.starts_with("type ")
                && !line.starts_with("type HttpResult<")
                && !line.starts_with("type HttpUnknownResult ")
        })
        .collect::<Vec<_>>();
    assert!(!aliases.is_empty());
    assert!(
        aliases.iter().all(|alias| alias.ends_with(" = unknown;")),
        "reference-only cycle must widen instead of producing circular aliases: {aliases:?}"
    );
    assert!(
        operation
            .diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.code() == "typescript_schema_widened")
    );
    assert_strict_compiling_declaration(operation.typescript_declaration());
}

#[test]
fn openapi_map_extensions_remain_literal_during_discovery_and_binding() {
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Literal map extensions", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {
            "/value": {"get": {
                "operationId": "value",
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {"application/json": {"schema": {"type": "string"}}}
                    },
                    "x-response-extension": {
                        "content": {"application/json": {"schema": {
                            "$ref": "#/not/a/schema/location"
                        }}}
                    }
                }
            }},
            "x-path-extension": {"get": {
                "security": [{"literal-extension-data": []}],
                "responses": {"200": {"content": {"application/json": {"schema": {
                    "$ref": "#/not/a/schema/location"
                }}}}}
            }}
        }
    });
    let prepared = ready(&document.to_string());
    assert_eq!(prepared.operations().len(), 1);
    assert_eq!(
        prepared.operations()[0].availability(),
        OperationAvailability::Available
    );
    for source in [
        "#/paths/x-path-extension/get/responses/200/content/application~1json/schema",
        "#/paths/~1value/get/responses/x-response-extension/content/application~1json/schema",
    ] {
        assert!(
            prepared
                .schema_graph()
                .schema_id_for_source(source)
                .is_none()
        );
    }
}

#[test]
fn discovery_only_failures_are_ignored_and_callback_and_webhook_schemas_are_retained() {
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Best effort discovery", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {"/value": {"get": {
            "operationId": "value",
            "responses": {"200": {"description": "ok"}}
        }}},
        "webhooks": {"incoming": {"post": {
            "requestBody": {"content": {"application/json": {"schema": {"type": "string"}}}},
            "responses": {"200": {"description": "ok"}}
        }}},
        "components": {
            "responses": {"UnusedBad": {
                "description": "bad", "content": {"application/json": {"schema": {"type": 5}}}
            }},
            "callbacks": {"UnusedCallback": {
                "{$request.body#/url}": {"post": {
                    "requestBody": {"content": {"application/json": {"schema": {"type": "integer"}}}},
                    "responses": {"200": {"description": "ok"}}
                }}
            }}
        }
    });
    let prepared = ready(&document.to_string());
    assert_eq!(
        prepared.operations()[0].availability(),
        OperationAvailability::Available
    );
    assert!(
        prepared
            .schema_graph()
            .schema_id_for_source(
                "#/components/responses/UnusedBad/content/application~1json/schema"
            )
            .is_none()
    );
    for (source, retained) in [
        (
            "#/webhooks/incoming/post/requestBody/content/application~1json/schema",
            "string",
        ),
        (
            "#/components/callbacks/UnusedCallback/{$request.body#~1url}/post/requestBody/content/application~1json/schema",
            "integer",
        ),
    ] {
        let graph = prepared.schema_graph();
        let id = graph
            .schema_id_for_source(source)
            .unwrap_or_else(|| panic!("{source} is retained"));
        assert_eq!(graph.schema(id).expect(source)["type"], retained);
    }
}

#[test]
fn nested_locations_are_indexed_without_becoming_roots_until_referenced() {
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Nested targets", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {"/value": {"get": {
            "operationId": "value",
            "responses": {"200": {"description": "ok"}}
        }}},
        "components": {"schemas": {
            "Root": {
                "type": "object",
                "properties": {
                    "unreferenced": {"type": "string"},
                    "target": {"type": "integer"}
                }
            },
            "Alias": {"$ref": "#/components/schemas/Root/properties/target"}
        }}
    });
    let prepared = ready(&document.to_string());
    assert!(
        prepared
            .schema_graph()
            .schema_id_for_source("#/components/schemas/Root/properties/unreferenced")
            .is_none()
    );
    let target_id = prepared
        .schema_graph()
        .schema_id_for_source("#/components/schemas/Root/properties/target")
        .expect("referenced nested target");
    let alias_id = prepared
        .schema_graph()
        .schema_id_for_source("#/components/schemas/Alias")
        .expect("component alias");
    assert_eq!(
        prepared.schema_graph().schema(alias_id).expect("alias")["$ref"],
        target_id.as_str()
    );
}

#[test]
fn openapi_30_does_not_discover_openapi_31_only_object_locations() {
    let document = serde_json::json!({
        "openapi": "3.0.3",
        "info": {"title": "Versioned discovery", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {"/value": {"get": {
            "operationId": "value",
            "responses": {"200": {"description": "ok"}}
        }}},
        "webhooks": {"not-an-openapi-30-field": {"post": {
            "requestBody": {"content": {"application/json": {"schema": {"type": "string"}}}}
        }}},
        "components": {"pathItems": {"NotAnOpenApi30Component": {"post": {
            "requestBody": {"content": {"application/json": {"schema": {"type": "integer"}}}}
        }}}}
    });
    let prepared = ready(&document.to_string());
    assert_eq!(
        prepared.operations()[0].availability(),
        OperationAvailability::Available
    );
    assert!(prepared
        .schema_graph()
        .schema_id_for_source("#/webhooks/not-an-openapi-30-field/post/requestBody/content/application~1json/schema")
        .is_none());
    assert!(prepared
        .schema_graph()
        .schema_id_for_source("#/components/pathItems/NotAnOpenApi30Component/post/requestBody/content/application~1json/schema")
        .is_none());
}

#[test]
fn references_to_malformed_indexed_schemas_propagate_the_target_failure() {
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Malformed target", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {
            "/target": {"get": {"operationId": "target", "responses": {"200": {
                "description": "malformed",
                "content": {"application/json": {"schema": 5}}
            }}}},
            "/dependent": {"get": {"operationId": "dependent", "responses": {"200": {
                "description": "dependent",
                "content": {"application/json": {"schema": {
                    "$ref": "#/paths/~1target/get/responses/200/content/application~1json/schema"
                }}}
            }}}}
        }
    });
    let prepared = ready(&document.to_string());
    assert!(prepared.operations().iter().all(|operation| {
        operation.availability() == OperationAvailability::Unavailable
            && operation
                .diagnostic()
                .is_some_and(|diagnostic| diagnostic.code() == "invalid_operation_schema")
    }));
}

#[test]
fn malformed_schema_child_containers_expose_no_reference_targets() {
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Malformed children", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {
            "/owner": {"get": {"operationId": "owner", "responses": {"200": {
                "description": "bad",
                "content": {"application/json": {"schema": {"properties": [{"type": "string"}]}}}
            }}}},
            "/reference": {"get": {"operationId": "reference", "responses": {"200": {
                "description": "bad target",
                "content": {"application/json": {"schema": {
                    "$ref": "#/paths/~1owner/get/responses/200/content/application~1json/schema/properties/0"
                }}}
            }}}}
        }
    });
    let prepared = ready(&document.to_string());
    let owner = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "owner")
        .expect("owner Operation");
    let reference = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "reference")
        .expect("reference Operation");
    assert_eq!(
        owner.diagnostic().expect("owner diagnostic").code(),
        "invalid_operation_schema"
    );
    assert_eq!(
        reference.diagnostic().expect("reference diagnostic").code(),
        "invalid_schema_reference"
    );
    assert!(
        prepared
            .schema_graph()
            .schema_id_for_source(
                "#/paths/~1owner/get/responses/200/content/application~1json/schema/properties/0"
            )
            .is_none()
    );
}

#[test]
fn reusable_parameter_schema_keeps_its_provider_pointer_identity() {
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Shared parameter", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {
            "/one": {"get": {
                "operationId": "one",
                "parameters": [{"$ref": "#/components/parameters/Shared"}],
                "responses": {"200": {"description": "ok"}}
            }},
            "/two": {"get": {
                "operationId": "two",
                "parameters": [{"$ref": "#/components/parameters/Shared"}],
                "responses": {"200": {"description": "ok"}}
            }}
        },
        "components": {"parameters": {"Shared": {
            "name": "value", "in": "query", "schema": {"type": "string"}
        }}}
    });
    let prepared = ready(&document.to_string());
    let expected = prepared
        .schema_graph()
        .schema_id_for_source("#/components/parameters/Shared/schema")
        .expect("shared provider identity");
    let binding_ids = prepared
        .operations()
        .iter()
        .map(|operation| http_binding(operation).parameters()[0].schema())
        .collect::<Vec<_>>();
    assert_eq!(binding_ids, vec![expected, expected]);
}

#[test]
fn root_identity_credential_contract_and_index_failures_reject_the_package() {
    let cases = [
        (
            "root",
            "openapi: 3.1.0\ninfo: []\npaths: {}\n",
            "invalid_provider_metadata",
        ),
        (
            "identity",
            r#"openapi: 3.1.0
info:
  title: "\u0001unsafe"
  version: "1"
paths: {}
"#,
            "invalid_provider_metadata",
        ),
        (
            "credential contract",
            r#"openapi: 3.1.0
info:
  title: Credentials
  version: "1"
servers:
  - url: https://api.example.test
paths:
  /value:
    get:
      operationId: value
      responses:
        "200":
          description: ok
components:
  securitySchemes:
    broken:
      description: missing type
"#,
            "invalid_credential_contract",
        ),
        (
            "credential contract reference",
            r##"openapi: 3.1.0
info:
  title: Credentials
  version: "1"
servers:
  - url: https://api.example.test
paths:
  /value:
    get:
      operationId: value
      responses:
        "200":
          description: ok
components:
  securitySchemes:
    alias:
      $ref: "#/components/securitySchemes/Missing"
"##,
            "invalid_credential_contract",
        ),
        (
            "index",
            r#"openapi: 3.1.0
info:
  title: Duplicate keys
  version: "1"
servers:
  - url: https://api.example.test
paths:
  /one:
    get:
      operationId: duplicate
      responses:
        "200":
          description: ok
  /two:
    post:
      operationId: duplicate
      responses:
        "200":
          description: ok
"#,
            "duplicate_operation_key",
        ),
    ];

    for (name, document, expected_code) in cases {
        let document = OpenApiDocument::parse(document).expect("bounded failure fixture");
        let MaterializationOutcome::Rejected(diagnostic) = materialize(
            DestinationPolicy::default(),
            &ProviderSetupInput::from(document),
            &[],
        ) else {
            panic!("{name} failure must reject the complete package");
        };
        assert_eq!(diagnostic.code(), expected_code, "{name}");
    }
}

#[test]
fn unparseable_document_rejection_names_the_location_without_the_value() {
    let cases = [
        (
            "integer outside i64",
            r"openapi: 3.1.0
info: { title: Out-of-range bound, version: '1' }
servers: [{ url: https://api.example.test }]
paths:
  /probe:
    get:
      operationId: probe
      responses:
        '200':
          description: ok
          content:
            application/json:
              schema:
                type: integer
                minimum: -9223372036854776000
",
            "the pinned input is not a valid OpenAPI YAML or JSON document at \
             line 15, column 26 \
             (#/paths/~1probe/get/responses/200/content/application~1json/schema/minimum)",
        ),
        (
            "unclosed flow sequence",
            "openapi: 3.1.0\ninfo: [unclosed\n",
            "the pinned input is not a valid OpenAPI YAML or JSON document at \
             line 3, column 1 (#/info)",
        ),
        (
            "fault under a YAML tag",
            "openapi: 3.1.0\nx-tagged: !custom\n  minimum: -9223372036854776000\n",
            "the pinned input is not a valid OpenAPI YAML or JSON document at \
             line 3, column 12 (#/x-tagged/!custom/minimum)",
        ),
        (
            "sequence entry in the root mapping",
            "openapi: 3.1.0\n- item\n",
            "the pinned input is not a valid OpenAPI YAML or JSON document at \
             line 2, column 1",
        ),
    ];

    for (name, document, expected_message) in cases {
        let document = OpenApiDocument::parse(document).expect("bounded failure fixture");
        let MaterializationOutcome::Rejected(diagnostic) = materialize(
            DestinationPolicy::default(),
            &ProviderSetupInput::from(document),
            &[],
        ) else {
            panic!("{name} must reject the complete package");
        };
        assert_eq!(diagnostic.code(), "invalid_openapi_document", "{name}");
        assert_eq!(diagnostic.message(), expected_message, "{name}");
    }
}

#[test]
fn layered_diamond_schema_graph_materializes_as_one_deterministic_local_package() {
    let document = layered_diamond_fixture(28);
    let first = ready(&document);
    let second = ready(&document);

    assert_eq!(first.fingerprint(), second.fingerprint());
    assert_eq!(first.operation_paths(), second.operation_paths());
    assert_eq!(first.operations().len(), 1);
    assert_eq!(
        first.operations()[0].availability(),
        OperationAvailability::Available
    );

    for layer in 0..=28 {
        for side in ['A', 'B'] {
            let source = format!("#/components/schemas/{side}{layer}");
            let schema_id = first
                .schema_graph()
                .schema_id_for_source(&source)
                .unwrap_or_else(|| panic!("missing retained {source}"));
            let schema = first
                .schema_graph()
                .schema(schema_id)
                .unwrap_or_else(|| panic!("missing package schema for {source}"));
            assert_package_local_schema_references(schema);
        }
    }
}

fn assert_package_local_schema_references(schema: &serde_json::Value) {
    let mut pending = vec![schema];
    while let Some(value) = pending.pop() {
        match value {
            serde_json::Value::Array(values) => pending.extend(values),
            serde_json::Value::Object(object) => {
                if let Some(reference) = object.get("$ref") {
                    assert!(
                        reference
                            .as_str()
                            .is_some_and(|reference| reference.starts_with("schema_")),
                        "semantic references must be package-local: {reference}"
                    );
                }
                pending.extend(object.values());
            }
            _ => {}
        }
    }
}

fn layered_diamond_fixture(layers: usize) -> String {
    let mut schemas = String::from("    A0:\n      type: string\n    B0:\n      type: integer\n");
    for layer in 1..=layers {
        let previous = layer - 1;
        for side in ['A', 'B'] {
            write!(
                schemas,
                "    {side}{layer}:\n      allOf:\n        - $ref: \"#/components/schemas/A{previous}\"\n        - $ref: \"#/components/schemas/B{previous}\"\n"
            )
            .expect("writing to a String cannot fail");
        }
    }
    format!(
        r##"openapi: 3.1.0
info:
  title: Layered diamond
  version: "1"
servers:
  - url: https://api.example.test
paths:
  /value:
    get:
      operationId: getValue
      responses:
        "200":
          description: ok
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/A{layers}"
components:
  schemas:
{schemas}"##
    )
}

fn assert_strict_compiling_declaration(declaration: &str) {
    let source = format!(
        "declare namespace Materialized {{\n{declaration}\n}}\nexport default function compileCheck(): void {{}}"
    );
    let compiler = ProgramCompiler::new(coral_v8::IsolateLimits::default())
        .expect("pinned TypeScript compiler");
    let compilation = compiler
        .prepare_saved_function(
            std::time::Instant::now() + coral_v8::COMPILATION_TIMEOUT,
            &source,
        )
        .expect("materialized declaration must be valid TypeScript");
    let errors = compilation
        .diagnostics()
        .iter()
        .filter(|diagnostic| diagnostic.category() == DiagnosticCategory::Error)
        .map(|diagnostic| (diagnostic.code(), diagnostic.message()))
        .collect::<Vec<_>>();
    assert!(
        errors.is_empty(),
        "strict TypeScript diagnostics: {errors:?}"
    );
}

fn direct_type_aliases_are_acyclic(declaration: &str) -> bool {
    let aliases = declaration
        .lines()
        .filter_map(|line| {
            let (name, target) = line.strip_prefix("type ")?.split_once(" = ")?;
            let target = target.strip_suffix(';')?;
            target
                .chars()
                .all(|character| {
                    character.is_ascii_alphanumeric() || matches!(character, '_' | '$')
                })
                .then(|| (name.to_string(), target.to_string()))
        })
        .collect::<BTreeMap<_, _>>();
    for source in aliases.keys() {
        let mut seen = std::collections::BTreeSet::new();
        let mut current = source.as_str();
        while let Some(target) = aliases.get(current) {
            if !seen.insert(current.to_string()) {
                return false;
            }
            current = target;
        }
    }
    true
}

/// Mirrors the `OpenAI` document's `CompoundFilter`, which recurses through draft
/// 2019-09 keywords that Coral does not support.
fn failing_component_fixture(with_dependent_operation: bool) -> String {
    let mut document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Failing component", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {
            "/files": {"get": {
                "operationId": "listFiles",
                "responses": {"200": {
                    "description": "ok",
                    "content": {"application/json": {
                        "schema": {"$ref": "#/components/schemas/File"}
                    }}
                }}
            }},
            "/health": {"get": {
                "operationId": "health",
                "responses": {"204": {"description": "ok"}}
            }}
        },
        "components": {"schemas": {
            "File": {
                "type": "object",
                "properties": {"id": {"type": "string"}}
            },
            "ComparisonFilter": {
                "type": "object",
                "properties": {"key": {"type": "string"}}
            },
            "CompoundFilter": {
                "$recursiveAnchor": true,
                "type": "object",
                "properties": {"filters": {
                    "type": "array",
                    "items": {"oneOf": [
                        {"$ref": "#/components/schemas/ComparisonFilter"},
                        {"$recursiveRef": "#"}
                    ]}
                }}
            },
            "SearchRequest": {
                "type": "object",
                "properties": {"filter": {"$ref": "#/components/schemas/CompoundFilter"}}
            }
        }}
    });
    if with_dependent_operation {
        document["paths"]["/search"] = serde_json::json!({"post": {
            "operationId": "search",
            "requestBody": {"content": {"application/json": {
                "schema": {"$ref": "#/components/schemas/SearchRequest"}
            }}},
            "responses": {"200": {"description": "ok"}}
        }});
    }
    document.to_string()
}

fn reference_fixture(version: &str, reference: &str) -> String {
    serde_json::json!({
        "openapi": version,
        "info": {"title": "Reference fixture", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {"/value": {"get": {
            "operationId": "value",
            "responses": {"200": {
                "description": "ok",
                "content": {"application/json": {"schema": {"$ref": reference}}}
            }}
        }}},
        "components": {"schemas": {"Holder": {
            "type": "object",
            "example": {"value": 1},
            "examples": [{"value": 1}],
            "default": {"value": 1},
            "enum": [{"value": 1}],
            "const": {"value": 1},
            "x-data": {"value": 1}
        }}}
    })
    .to_string()
}

fn reference_fixture_with_sibling(version: &str, sibling: serde_json::Value) -> String {
    let mut document: serde_json::Value =
        serde_json::from_str(&reference_fixture(version, "#/components/schemas/Holder"))
            .expect("fixture JSON");
    fixture_response_schema(&mut document)["type"] = sibling;
    document.to_string()
}

fn schema_keyword_fixture(version: &str, keyword: &str, value: serde_json::Value) -> String {
    let mut document: serde_json::Value =
        serde_json::from_str(&reference_fixture(version, "#/components/schemas/Holder"))
            .expect("fixture JSON");
    *fixture_response_schema(&mut document) =
        serde_json::Value::Object(serde_json::Map::from_iter([(keyword.to_string(), value)]));
    document.to_string()
}

fn fixture_response_schema(document: &mut serde_json::Value) -> &mut serde_json::Value {
    &mut document["paths"]["/value"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
}

fn cycle_fixture(with_invalid_dependency: bool) -> String {
    let mut second = serde_json::json!({"$ref": "#/components/parameters/A/schema"});
    let mut parameters = serde_json::json!({
        "A": {
            "name": "a", "in": "query",
            "schema": {"$ref": "#/components/parameters/B/schema"}
        },
        "B": {"name": "b", "in": "query", "schema": second.clone()}
    });
    if with_invalid_dependency {
        second["allOf"] = serde_json::json!([
            {"$ref": "#/components/parameters/Invalid/schema"}
        ]);
        parameters["B"]["schema"] = second;
        parameters["Invalid"] = serde_json::json!({
            "name": "invalid", "in": "query", "schema": {"type": 5}
        });
    }
    serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Cycle fixture", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {
            "/a": {"get": {
                "operationId": "a",
                "parameters": [{"$ref": "#/components/parameters/A"}],
                "responses": {"200": {"description": "ok"}}
            }},
            "/b": {"get": {
                "operationId": "b",
                "parameters": [{"$ref": "#/components/parameters/B"}],
                "responses": {"200": {"description": "ok"}}
            }}
        },
        "components": {"parameters": parameters}
    })
    .to_string()
}

fn operation_paths(prepared: &coral_catalog::PreparedProviderCatalog) -> BTreeMap<String, String> {
    prepared
        .operations()
        .iter()
        .map(|operation| {
            (
                operation.key().as_str().to_string(),
                operation.relative_path().to_string(),
            )
        })
        .collect()
}

#[test]
fn operation_count_does_not_reject_an_openapi_catalog() {
    let operation_ids: Vec<String> = (0..100).map(|index| format!("operation{index}")).collect();
    let ids: Vec<&str> = operation_ids.iter().map(String::as_str).collect();
    let prepared = ready(&path_fixture(&ids));

    assert_eq!(prepared.operations().len(), operation_ids.len());
    for operation_id in &operation_ids {
        let operation = prepared
            .operations()
            .iter()
            .find(|operation| operation.key().as_str() == operation_id)
            .expect("every Operation is retained above the former 64-Operation cap");
        assert_eq!(operation.availability(), OperationAvailability::Available);
        assert_eq!(http_binding(operation).responses().len(), 1);
    }
}

fn path_fixture(operation_ids: &[&str]) -> String {
    let paths = operation_ids
        .iter()
        .enumerate()
        .map(|(index, operation_id)| {
            format!(
                r#"  /operation-{index}:
    get:
      operationId: {operation_id}
      responses:
        "200":
          description: ok
"#
            )
        })
        .collect::<String>();
    format!(
        r#"openapi: 3.1.0
info:
  title: Path fixture
  version: "1"
servers:
  - url: https://api.example.test
paths:
{paths}"#
    )
}

#[test]
fn summaries_bearing_nonwhitespace_control_characters_fall_back_to_the_method_and_path() {
    let prepared = ready(
        "openapi: 3.1.0\n\
         info: { title: Control text, version: \"1\" }\n\
         servers: [{ url: https://api.example.test }]\n\
         paths:\n  \
         /values:\n    \
         get:\n      \
         operationId: listValues\n      \
         summary: \"Lists\\u0000 values\"\n      \
         responses:\n        \
         \"200\": { description: ok }\n",
    );
    let operation = prepared.operations().first().expect("Operation");

    assert_eq!(operation.summary(), "GET /values");
    assert_eq!(operation.presentation().description(), "GET /values");
    assert_eq!(operation.presentation().title(), None);
}

#[test]
fn oversized_search_material_warns_without_blocking_the_catalog() {
    let properties = (0..8_300)
        .map(|index| {
            (
                format!("f{index:04}"),
                serde_json::json!({"type": "boolean"}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let ok = |schema: Value| {
        serde_json::json!({"200": {
            "description": "ok",
            "content": {"application/json": {"schema": schema}},
        }})
    };
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Oversized search material", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "security": [],
        "paths": {
            "/wide": {"get": {
                "operationId": "wide",
                "responses": ok(serde_json::json!({"type": "object", "properties": properties})),
            }},
            "/small": {"get": {
                "operationId": "small",
                "responses": ok(serde_json::json!({"type": "string"})),
            }},
        },
    })
    .to_string();

    let prepared = ready(&document);

    assert_eq!(prepared.operations().len(), 2);
    assert!(
        prepared
            .operations()
            .iter()
            .all(|operation| operation.availability() == OperationAvailability::Available)
    );
    let wide = prepared
        .operations()
        .iter()
        .find(|operation| operation.key().as_str() == "wide")
        .expect("oversized Operation");
    assert_eq!(
        wide.diagnostics()
            .iter()
            .map(coral_catalog::CatalogDiagnostic::code)
            .collect::<Vec<_>>(),
        ["operation_search_material_truncated"]
    );
}

#[test]
fn openapi_input_is_refused_over_the_byte_budget() {
    // A comment keeps the padded document valid YAML.
    let padded = |length: usize| format!("#{}", " ".repeat(length - 1));
    assert!(matches!(
        OpenApiDocument::parse(&padded(coral_catalog::OPENAPI_DOCUMENT_MAX_BYTES + 1)),
        Err(coral_catalog::Error::OpenApiDocumentTooLarge)
    ));
    OpenApiDocument::parse(&padded(coral_catalog::OPENAPI_DOCUMENT_MAX_BYTES))
        .expect("the exact budget fits");
}

/// Materializes a small valid document that also carries `extension` as `x-extension`.
fn with_extension(extension: &str) -> MaterializationOutcome {
    let document = OpenApiDocument::parse(&format!(
        r#"openapi: 3.1.0
info: {{ title: Bounded parsing, version: "1" }}
servers: [{{ url: https://api.example.test }}]
paths:
  /value:
    get:
      operationId: value
      responses: {{ "200": {{ description: ok }} }}
x-extension: {extension}
"#
    ))
    .expect("bounded input");
    materialize(
        DestinationPolicy::default(),
        &ProviderSetupInput::from(document),
        &[],
    )
}

#[test]
fn alias_expansion_and_deep_nesting_are_refused_before_materialization() {
    // Nine levels of ten aliases each would expand to a billion entries.
    let mut laughs = String::from("\n  a0: &a0 [x, x, x, x, x, x, x, x, x, x]");
    for level in 1..9 {
        let previous = format!("*a{}", level - 1);
        write!(
            laughs,
            "\n  a{level}: &a{level} [{}]",
            [previous.as_str(); 10].join(", ")
        )
        .expect("write fixture");
    }
    let deep = |depth: usize| format!("{}{}", "[".repeat(depth), "]".repeat(depth));
    for (extension, refused) in [
        (laughs.as_str(), true),
        (deep(200).as_str(), true),
        // Aliases and ordinary nesting are accepted within the bounds.
        ("\n  a: &a [x]\n  b: *a", false),
        (deep(32).as_str(), false),
    ] {
        match with_extension(extension) {
            MaterializationOutcome::Rejected(diagnostic) => {
                assert!(refused, "{extension}: {}", diagnostic.code());
                assert_eq!(diagnostic.code(), "invalid_openapi_document");
            }
            MaterializationOutcome::Ready(_) => assert!(!refused, "{extension}"),
        }
    }
}

/// One credential-free Operation whose single response the tests below edit.
const WEATHER_OPENAPI: &str = r#"openapi: 3.1.0
info:
  title: Weather
  version: 1.0.0
servers:
  - url: https://weather.example
security: []
paths:
  /forecast:
    get:
      operationId: getForecast
      summary: Get forecast
      responses:
        "200":
          description: Forecast
"#;

fn weather_with_response(response: &str) -> String {
    WEATHER_OPENAPI.replace("          description: Forecast", response)
}

/// Materializes `document` with its suggested configuration, as a published
/// Setup does.
fn configured(document: &str) -> MaterializationOutcome {
    let document = OpenApiDocument::parse(document).expect("OpenAPI input");
    let configuration = document
        .configuration_review()
        .expect("configuration review")
        .suggested;
    let configured = document
        .configure(configuration)
        .expect("the suggested configuration applies");
    materialize(
        DestinationPolicy::default(),
        &ProviderSetupInput::from(configured),
        &[],
    )
}

fn configured_ready(document: &str) -> coral_catalog::PreparedProviderCatalog {
    let MaterializationOutcome::Ready(prepared) = configured(document) else {
        panic!("the configured fixture materializes");
    };
    prepared
}

#[test]
fn operation_ids_must_be_keys_and_reserved_words_are_escaped() {
    let invalid = WEATHER_OPENAPI.replace("operationId: getForecast", "operationId: 7");
    let MaterializationOutcome::Rejected(diagnostic) = materialize(
        DestinationPolicy::default(),
        &ProviderSetupInput::from(OpenApiDocument::parse(&invalid).expect("OpenAPI input")),
        &[],
    ) else {
        panic!("a numeric operationId rejects the package");
    };
    assert_eq!(diagnostic.code(), "invalid_operation_key");

    let reserved = configured_ready(
        &WEATHER_OPENAPI.replace("operationId: getForecast", "operationId: interface"),
    );
    let [operation] = reserved.operations() else {
        panic!("one Operation");
    };
    assert_eq!(operation.relative_path(), "_interface");
    let declaration = operation.typescript_declaration();
    assert_eq!(
        declaration.lines().last(),
        Some(
            "function _interface(): Promise<HttpResult<null, 200, string | null> | HttpUnknownResult>;"
        )
    );
}

#[test]
fn response_schema_drives_the_typescript_declaration() {
    let document = format!(
        "{}components:\n  schemas:\n    Forecast:\n      type: object\n      required: [temperature]\n      properties:\n        temperature:\n          type: number\n",
        weather_with_response(
            "          description: Forecast\n          content:\n            application/json:\n              schema:\n                $ref: '#/components/schemas/Forecast'"
        )
    );
    let prepared = configured_ready(&document);
    let declaration = prepared.operations()[0].typescript_declaration();
    assert!(
        declaration.starts_with(
            "type Forecast = ({ \"temperature\": number } & Record<string, unknown>);\n"
        )
    );
    assert!(declaration.ends_with(
        "Promise<HttpResult<Forecast, 200, \"application/json\"> | HttpUnknownResult>;"
    ));
}

#[test]
fn malformed_responses_and_servers_make_only_their_operation_unavailable() {
    for (case, document, expected) in [
        (
            "status key",
            WEATHER_OPENAPI.replace("\"200\":", "bogus:"),
            "invalid_openapi_operation",
        ),
        (
            "malformed reference",
            weather_with_response("          description: Forecast\n          $ref: 7"),
            "invalid_openapi_operation",
        ),
        (
            "unresolved reference",
            weather_with_response("          $ref: '#/components/responses/Missing'"),
            "invalid_openapi_operation",
        ),
        (
            "external reference with a malformed sibling",
            weather_with_response(
                "          $ref: 'https://example.com/responses.yaml#/Weather'\n          summary: 7",
            ),
            "invalid_openapi_operation",
        ),
        (
            "malformed schema",
            weather_with_response(
                "          description: Forecast\n          content:\n            application/json:\n              schema: 7",
            ),
            "invalid_operation_schema",
        ),
        (
            "explicit empty servers",
            WEATHER_OPENAPI.replace(
                "operationId: getForecast\n",
                "operationId: getForecast\n      servers: []\n",
            ),
            "unresolved_server_configuration",
        ),
    ] {
        let prepared = configured_ready(&document);
        let [operation] = prepared.operations() else {
            panic!("{case}: one Operation");
        };
        assert_eq!(
            operation.availability(),
            OperationAvailability::Unavailable,
            "{case}"
        );
        assert_eq!(
            operation
                .diagnostic()
                .map(coral_catalog::CatalogDiagnostic::code),
            Some(expected),
            "{case}"
        );
    }
}

#[test]
fn a_valid_local_response_reference_is_available() {
    let document = format!(
        "{}components:\n  responses:\n    Weather:\n      description: Forecast\n",
        weather_with_response(
            "          $ref: '#/components/responses/Weather'\n          summary: Weather response"
        )
    );
    assert_eq!(
        configured_ready(&document).operations()[0].availability(),
        OperationAvailability::Available
    );
}

#[test]
fn an_anonymous_security_alternative_is_credential_free() {
    let prepared = configured_ready(&WEATHER_OPENAPI.replace("security: []", "security: [{}]"));
    assert_eq!(
        prepared.credential_contract().authentication(),
        &coral_catalog::AuthenticationConfiguration::None
    );
}

#[test]
fn server_user_information_is_rejected_without_echoing_it() {
    let document = OpenApiDocument::parse(&WEATHER_OPENAPI.replace(
        "https://weather.example",
        "https://canary-user:canary-password@weather.example",
    ))
    .expect("OpenAPI input");
    let mut configuration = document
        .configuration_review()
        .expect("configuration review")
        .suggested;
    configuration.authentication = Some(coral_catalog::AuthenticationConfiguration::None);
    let Err(errors) = document.configure(configuration) else {
        panic!("a server URL with user information is not a destination");
    };
    assert!(!errors.is_empty());
    assert!(errors.iter().all(|error| !error.message.contains("canary")));
}

/// Concatenates every Operation's declaration into one scope, as a Program
/// that calls several Operations of one Provider sees them. Identical lines
/// declare one shared type once.
fn shared_scope(prepared: &coral_catalog::PreparedProviderCatalog) -> String {
    prepared
        .operations()
        .iter()
        .flat_map(|operation| operation.typescript_declaration().lines())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn inline_schema_type_names_say_where_the_schema_sits() {
    let marker = |value: &str| serde_json::json!({"const": value});
    let json_content =
        |value: &str| serde_json::json!({"application/json": {"schema": marker(value)}});
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Names", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {
            "/models/{model}": {
                "parameters": [{"name": "model", "in": "path", "required": true,
                    "schema": marker("path-level parameter")}],
                "get": {
                    "operationId": "retrieveModel",
                    "parameters": [
                        {"name": "view", "in": "query", "schema": marker("parameter")},
                        {"$ref": "#/components/parameters/Limit"}
                    ],
                    "responses": {"200": {"description": "ok"}}
                }
            },
            "/admin": {"patch": {
                "operationId": "updateAdmin",
                "requestBody": {"content": json_content("body")},
                "responses": {"204": {"description": "ok"}}
            }},
            "/messages": {"post": {
                "operationId": "postMessage",
                "requestBody": {"content": {"application/json": {"schema": {
                    "$ref": "#/components/schemas/Message/properties/content"
                }}}},
                "responses": {"204": {"description": "ok"}}
            }},
            "/settings": {"put": {
                "operationId": "putSettings",
                "requestBody": {"$ref": "#/components/requestBodies/Settings"},
                "responses": {"200": {"description": "ok", "content": {"application/json": {
                    "schema": {"$ref": "#/components/schemas/Model"}
                }}}}
            }}
        },
        "components": {
            "schemas": {
                "Model": marker("component"),
                "Message": {"type": "object", "properties": {"content": marker("property")}}
            },
            "parameters": {"Limit": {"name": "limit", "in": "query",
                "schema": marker("component parameter")}},
            "requestBodies": {"Settings": {"content": json_content("component body")}}
        }
    });
    let scope = shared_scope(&ready(&document.to_string()));

    for expected in [
        r#"type ModelsModelParameter0Input = "path-level parameter";"#,
        r#"type GetModelsModelParameter0Input = "parameter";"#,
        r#"type PatchAdminBodyInput = "body";"#,
        r#"type Model = "component";"#,
        r#"type LimitParameterInput = "component parameter";"#,
        r#"type SettingsBodyInput = "component body";"#,
        r#"type MessageContentInput = "property";"#,
    ] {
        assert!(
            scope.lines().any(|line| line == expected),
            "missing `{expected}` in:\n{scope}"
        );
    }
}

#[test]
fn declarations_of_every_operation_compile_together_in_one_scope() {
    let reference =
        |name: &str| serde_json::json!({"$ref": format!("#/components/schemas/{name}")});
    let ok_with = |schema: Value| {
        serde_json::json!({"200": {"description": "ok",
        "content": {"application/json": {"schema": {
            "type": "object", "properties": {"value": schema}
        }}}}})
    };
    let query =
        |value: &str| serde_json::json!([{"name": "q", "in": "query", "schema": {"const": value}}]);
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Shared scope", "version": "1"},
        "servers": [{"url": "https://api.example.test"}],
        "paths": {
            "/one": {"get": {
                "operationId": "one",
                "responses": ok_with(reference("FooBar"))
            }},
            "/two": {"get": {
                "operationId": "two",
                "responses": ok_with(serde_json::json!({
                    "anyOf": [reference("Foo-Bar"), reference("FooBar")]
                }))
            }},
            "/items/{id}": {
                "parameters": [{"name": "id", "in": "path", "required": true,
                    "schema": {"type": "string"}}],
                "get": {
                    "operationId": "byTemplate",
                    "parameters": query("template"),
                    "responses": {"200": {"description": "ok"}}
                }
            },
            "/items/id": {"get": {
                "operationId": "byLiteral",
                "parameters": query("literal"),
                "responses": {"200": {"description": "ok"}}
            }},
            "/settings": {"put": {
                "operationId": "putSettings",
                "requestBody": {"content": {"application/json": {"schema": {
                    "type": "object",
                    "properties": {"settings": reference("Settings")}
                }}}},
                "responses": ok_with(reference("SettingsInput"))
            }},
            "/results": {"get": {
                "operationId": "results",
                "responses": ok_with(serde_json::json!({
                    "anyOf": [
                        reference("HttpResult"),
                        reference("HttpUnknownResult"),
                        reference("BinaryValue")
                    ]
                }))
            }}
        },
        "components": {"schemas": {
            "Foo-Bar": {"const": "hyphenated"},
            "FooBar": {"const": "joined"},
            "Settings": {"const": "settings"},
            "SettingsInput": {"const": "component named SettingsInput"},
            "HttpResult": {"const": "component named HttpResult"},
            "HttpUnknownResult": {"const": "component named HttpUnknownResult"},
            "BinaryValue": {"const": "component named BinaryValue"}
        }}
    });
    let prepared = ready(&document.to_string());
    assert!(
        prepared
            .operations()
            .iter()
            .all(|operation| operation.availability() == OperationAvailability::Available)
    );
    let scope = shared_scope(&prepared);

    assert_strict_compiling_declaration(&scope);
    for expected in [
        r#"type FooBar = "joined";"#,
        r#"type FooBar2 = "hyphenated";"#,
        r#"type GetItemsIdParameter0Input = "literal";"#,
        r#"type GetItemsIdParameter0Input2 = "template";"#,
        r#"type SettingsInput = "component named SettingsInput";"#,
        r#"type SettingsInput2 = "settings";"#,
        r#"type HttpResult2 = "component named HttpResult";"#,
        r#"type HttpUnknownResult2 = "component named HttpUnknownResult";"#,
        r#"type BinaryValue2 = "component named BinaryValue";"#,
    ] {
        assert!(
            scope.lines().any(|line| line == expected),
            "missing `{expected}` in:\n{scope}"
        );
    }
}

#[test]
fn blank_response_descriptions_are_accepted_and_missing_ones_are_not() {
    let cases = [
        ("description: \"\"", OperationAvailability::Available),
        ("description: \"   \"", OperationAvailability::Available),
        ("x-generated: true", OperationAvailability::Unavailable),
    ];
    for (description_line, expected) in cases {
        let document = HTTP_CORPUS_SLICE.replace(
            "components:\n",
            &format!(
                r#"  /generated:
    get:
      operationId: generatedResponse
      responses:
        "200":
          {description_line}
          content:
            application/json:
              schema:
                type: string
components:
"#
            ),
        );
        let prepared = ready(&document);
        let operation = prepared
            .operations()
            .iter()
            .find(|operation| operation.key().as_str() == "generatedResponse")
            .expect("generated Operation remains cataloged");
        assert_eq!(operation.availability(), expected, "{description_line}");
        if expected == OperationAvailability::Unavailable {
            let diagnostic = operation.diagnostic().expect("blocking diagnostic");
            assert_eq!(diagnostic.code(), "invalid_openapi_operation");
            assert_eq!(
                diagnostic.message(),
                "an OpenAPI Response Object has no description"
            );
        } else {
            assert!(operation.diagnostic().is_none(), "{description_line}");
        }
    }
}
