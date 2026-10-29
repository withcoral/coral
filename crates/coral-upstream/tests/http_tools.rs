//! HTTP planning and outcome preservation at the upstream Module seam.
#![allow(
    unused_crate_dependencies,
    reason = "integration targets inherit the library's direct dependency set"
)]

use coral_catalog::{
    AuthenticationConfiguration, CredentialField, CredentialSource, HeaderBinding,
    MaterializationOutcome, OpenApiDocument, PreparedProviderCatalog, ProviderSetupInput,
};
use coral_outbound::DestinationPolicy;
use coral_upstream::{HttpOutcome, HttpUpstream, OutcomeClass, ReadyHttpDispatch};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn limits(upstream: usize) -> coral_value::ValueLimits {
    coral_value::ValueLimits::new(8 << 20, 16 << 20, upstream, 64 << 20).expect("limits")
}

/// Every fixture server listens on loopback.
fn loopback() -> DestinationPolicy {
    DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps)
}

fn ready_input(input: &ProviderSetupInput) -> PreparedProviderCatalog {
    match coral_catalog::materialize(loopback(), input, &[]) {
        MaterializationOutcome::Ready(prepared) => prepared,
        MaterializationOutcome::Rejected(diagnostic) => {
            panic!("fixture should materialize: {}", diagnostic.message())
        }
    }
}

fn ready(document: &str) -> PreparedProviderCatalog {
    ready_input(&ProviderSetupInput::from(
        OpenApiDocument::parse(document).expect("OpenAPI fixture"),
    ))
}

/// Plans the catalog's only Tool.
fn try_plan(
    prepared: &PreparedProviderCatalog,
    arguments: &serde_json::Value,
) -> Result<ReadyHttpDispatch, coral_upstream::Error> {
    ReadyHttpDispatch::plan(
        prepared.tools().first().expect("Tool"),
        prepared.schema_graph(),
        arguments,
        limits(4096),
    )
}

fn plan(prepared: &PreparedProviderCatalog, arguments: &serde_json::Value) -> ReadyHttpDispatch {
    try_plan(prepared, arguments).expect("plan HTTP request")
}

async fn send(dispatch: ReadyHttpDispatch) -> Result<HttpOutcome, coral_upstream::Error> {
    HttpUpstream::new(loopback())
        .expect("HTTP Adapter")
        .send(dispatch)
        .await
}

/// Tools under `paths` behind `server`, sent with `authentication`. Each
/// Credential Field the method reads is presented under its own name.
fn configured(
    server: &str,
    paths: &str,
    authentication: AuthenticationConfiguration,
) -> PreparedProviderCatalog {
    let document = OpenApiDocument::parse(&format!(
        r#"
openapi: 3.1.0
info: {{title: Credentials, version: "1"}}
servers: [{{url: {server}}}]
security: []
paths:
{paths}"#
    ))
    .expect("fixture");
    let mut configuration = document.configuration_review().expect("fixture").suggested;
    configuration.credential_fields = authentication
        .credential_fields()
        .into_iter()
        .map(|field| {
            (
                field.to_string(),
                CredentialField {
                    label: field.to_string(),
                    description: String::new(),
                    sensitive: true,
                },
            )
        })
        .collect();
    configuration.authentication = Some(authentication);
    ready_input(
        &document
            .configure(configuration)
            .expect("valid credentials")
            .into(),
    )
}

/// `source` answers `method` with a 307 to `sink`, on another origin, which answers 204.
async fn cross_origin_pair(method_name: &str) -> (MockServer, MockServer) {
    let source = MockServer::start().await;
    let sink = MockServer::start().await;
    Mock::given(method(method_name))
        .respond_with(
            ResponseTemplate::new(307)
                .insert_header("location", format!("{}/elsewhere", sink.uri())),
        )
        .mount(&source)
        .await;
    Mock::given(method(method_name))
        .respond_with(ResponseTemplate::new(204))
        .mount(&sink)
        .await;
    (source, sink)
}

#[tokio::test]
async fn tools_send_a_default_user_agent_and_preserve_an_explicit_header() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(204))
        .expect(2)
        .mount(&server)
        .await;
    let prepared = ready(&format!(
        r#"
openapi: 3.1.0
info: {{title: User agent, version: "1"}}
servers: [{{url: {}}}]
paths:
  /:
    get:
      operationId: fetch
      parameters:
        - {{name: User-Agent, in: header, schema: {{type: string}}}}
      responses:
        "204": {{description: done}}
"#,
        server.uri()
    ));
    for arguments in [
        serde_json::json!({}),
        serde_json::json!({"User-Agent": "ConfiguredClient/1.0"}),
    ] {
        send(plan(&prepared, &arguments)).await.expect("response");
    }
    let requests = server.received_requests().await.expect("requests");
    for (request, expected) in requests.iter().zip(["Coral", "ConfiguredClient/1.0"]) {
        assert_eq!(
            request
                .headers
                .get_all("user-agent")
                .iter()
                .collect::<Vec<_>>(),
            vec![expected],
            "each request must send exactly the selected User-Agent"
        );
    }
}

#[tokio::test]
async fn multipart_media_list_selects_one_permitted_binary_type() {
    let server = MockServer::start().await;
    let document = include_str!("../../coral-catalog/tests/fixtures/openapi-http-corpus.yaml");
    let prepared = ready(&document.replace("https://api.example.test/v1", &server.uri()));
    let tool = prepared
        .tools()
        .iter()
        .find(|tool| tool.key().as_str() == "users/update-avatar")
        .expect("avatar Tool");
    Mock::given(method("PUT"))
        .and(path("/user/avatar"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(2)
        .mount(&server)
        .await;
    for mime in ["image/png", "image/jpeg"] {
        let arguments = serde_json::json!({"body":{"avatar":{"_tag":"BinaryValue", "encoding":"base64", "data":"AP8=", "mimeType":mime, "byteLength":2}}});
        let dispatch =
            ReadyHttpDispatch::plan(tool, prepared.schema_graph(), &arguments, limits(4096))
                .expect("permitted multipart type");
        send(dispatch).await.expect("uploaded");
    }
    let requests = server.received_requests().await.expect("requests");
    for (request, mime) in requests.iter().zip(["image/png", "image/jpeg"]) {
        let media = request
            .headers
            .get("content-type")
            .expect("multipart type")
            .to_str()
            .expect("header");
        let boundary = media
            .strip_prefix("multipart/form-data; boundary=")
            .expect("boundary");
        let mut expected = format!("--{boundary}\r\nContent-Disposition: form-data; name=\"avatar\"\r\nContent-Type: {mime}\r\n\r\n").into_bytes();
        expected.extend_from_slice(&[0, 255]);
        expected.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        assert_eq!(request.body, expected);
    }
    let invalid = serde_json::json!({"body":{"avatar":{"_tag":"BinaryValue", "encoding":"base64", "data":"AP8=", "mimeType":"application/pdf", "byteLength":2}}});
    assert!(matches!(
        ReadyHttpDispatch::plan(tool, prepared.schema_graph(), &invalid, limits(4096)),
        Err(coral_upstream::Error::InvalidArguments(_))
    ));
    server.verify().await;
}

#[tokio::test]
async fn multipart_upload_uses_binding_media_and_preserves_binary_parts() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/upload"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    let prepared = ready(&format!(
        r#"
openapi: 3.1.0
info: {{title: Multipart, version: "1"}}
servers: [{{url: {}}}]
paths:
  /upload:
    post:
      operationId: upload
      requestBody:
        content:
          multipart/form-data:
            schema:
              type: object
              properties:
                file: {{type: string, format: binary}}
                label: {{type: string}}
            encoding:
              file:
                contentType: application/pdf
                headers:
                  X-Part: {{schema: {{type: string, const: fixed}}}}
      responses:
        "204": {{description: uploaded}}
"#,
        server.uri()
    ));
    let args = serde_json::json!({"body":{"file":{"_tag":"BinaryValue", "encoding":"base64", "data":"AP+ADQo=", "mimeType":"image/png", "byteLength":5, "name":"report.pdf"}, "label":"café"}});
    send(plan(&prepared, &args)).await.expect("sent");
    let requests = server.received_requests().await.expect("requests");
    let request = requests.first().expect("one request");
    let media = request
        .headers
        .get("content-type")
        .expect("multipart type")
        .to_str()
        .expect("header");
    let boundary = media
        .strip_prefix("multipart/form-data; boundary=")
        .expect("boundary");
    let mut expected = format!("--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"report.pdf\"\r\nContent-Type: application/pdf\r\nX-Part: fixed\r\n\r\n").into_bytes();
    expected.extend_from_slice(&[0, 255, 128, 13, 10]);
    expected.extend_from_slice(format!("\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"label\"\r\nContent-Type: text/plain\r\n\r\ncafé\r\n--{boundary}--\r\n").as_bytes());
    assert_eq!(request.body, expected);
}

#[tokio::test]
async fn composed_binary_schemas_preserve_raw_bytes() {
    for (schema, binary) in [
        ("{allOf: [{type: string, format: binary}]}", true),
        (
            "{anyOf: [{$ref: '#/components/schemas/Bytes'}, {$ref: '#/components/schemas/Bytes'}]}",
            true,
        ),
        (
            "{oneOf: [{allOf: [{$ref: '#/components/schemas/Bytes'}]}]}",
            true,
        ),
        (
            "{anyOf: [{$ref: '#/components/schemas/Bytes'}, {type: string}]}",
            false,
        ),
    ] {
        let bytes = if binary {
            vec![255]
        } else {
            b"unchanged".to_vec()
        };
        let value = if binary {
            serde_json::json!({"_tag":"BinaryValue", "encoding":"base64", "data":"/w==", "mimeType":"text/plain", "byteLength":1})
        } else {
            serde_json::json!("unchanged")
        };
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/binary"))
            .and(wiremock::matchers::body_bytes(bytes.clone()))
            .respond_with(ResponseTemplate::new(200).set_body_raw(bytes, "text/plain"))
            .expect(1)
            .mount(&server)
            .await;
        let prepared = ready(&format!(
            r#"
openapi: 3.1.0
info: {{title: Composed binary, version: "1"}}
servers: [{{url: {}}}]
components:
  schemas:
    Bytes: {{type: string, format: binary}}
paths:
  /binary:
    post:
      operationId: binary
      requestBody:
        content:
          text/plain:
            schema: {schema}
      responses:
        "200":
          description: bytes
          content:
            text/plain:
              schema: {schema}
"#,
            server.uri()
        ));
        let dispatch = plan(&prepared, &serde_json::json!({"body":value}));
        let result = send(dispatch).await.expect("response");
        assert_eq!(result.data(), &value, "{schema}");
        assert!(result.decode_diagnostics().is_empty());
    }
}

#[tokio::test]
async fn binary_download_and_raw_upload_preserve_exact_bytes() {
    for media in ["application/pdf", "text/plain", "application/json"] {
        for (bytes, encoded, length) in
            [(vec![0, 255, 128, 13, 10], "AP+ADQo=", 5), (vec![], "", 0)]
        {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/binary"))
                .and(wiremock::matchers::body_bytes(bytes.clone()))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_bytes(bytes)
                        .insert_header("content-type", media),
                )
                .expect(1)
                .mount(&server)
                .await;
            let prepared = ready(&format!(
                r#"
openapi: 3.1.0
info: {{title: Binary, version: "1"}}
servers: [{{url: {}}}]
paths:
  /binary:
    post:
      operationId: binary
      requestBody:
        content:
          {media}:
            schema: {{type: string, format: binary}}
      responses:
        "200":
          description: bytes
          content:
            {media}:
              schema: {{type: string, format: binary}}
"#,
                server.uri()
            ));
            let binary = serde_json::json!({"_tag":"BinaryValue", "encoding":"base64", "data":encoded, "mimeType":media, "byteLength":length});
            let dispatch = plan(&prepared, &serde_json::json!({"body":binary}));
            let result = send(dispatch).await.expect("binary response");
            assert_eq!(result.data(), &binary);
            assert!(result.decode_diagnostics().is_empty());
        }
    }
}

const SERIALIZATION_FIXTURE: &str = r#"
openapi: 3.1.0
info:
  title: HTTP serialization
  version: "1"
servers:
  - url: https://api.example.test/v1
paths:
  /widgets/{coordinates}:
    post:
      operationId: createWidget
      parameters:
        - name: coordinates
          in: path
          required: true
          style: matrix
          explode: true
          schema:
            type: object
            required: [x, y]
            properties:
              x: { type: integer }
              y: { type: integer }
        - name: empty
          in: query
          allowEmptyValue: true
          schema: { type: string }
        - name: filter
          in: query
          style: deepObject
          explode: true
          schema:
            type: object
            properties:
              state: { type: string }
        - name: q
          in: query
          allowReserved: true
          schema: { type: string }
        - name: tags
          in: query
          style: pipeDelimited
          explode: false
          schema:
            type: array
            items: { type: string }
        - name: X-Mode
          in: header
          style: simple
          explode: false
          schema:
            type: array
            items: { type: string }
        - name: facets
          in: cookie
          style: form
          explode: true
          schema:
            type: object
            properties:
              color: { type: string }
              size: { type: string }
        - name: session
          in: cookie
          schema: { type: string }
      requestBody:
        required: true
        content:
          application/json:
            schema: { type: string }
          application/x-www-form-urlencoded:
            schema:
              type: object
              required: [name]
              properties:
                choices:
                  type: array
                  items: { type: string }
                metadata:
                  type: object
                  additionalProperties: { type: string }
                name: { type: string }
            encoding:
              choices:
                style: pipeDelimited
                explode: false
              metadata:
                style: deepObject
                explode: true
      responses:
        "201":
          description: created
          content:
            application/json:
              schema: { type: object }
"#;

#[test]
fn plans_parameters_and_request_encoding_from_logical_arguments() {
    let prepared = ready(SERIALIZATION_FIXTURE);
    let arguments = serde_json::json!({
        "coordinates": {"x": 1, "y": 2},
        "empty": "",
        "filter": {"state": "open"},
        "q": "a/b?c",
        "tags": ["red", "blue"],
        "X-Mode": ["fast", "safe"],
        "facets": {"color": "red", "size": "large"},
        "session": "abc",
        "body": {
            "mediaType": "application/x-www-form-urlencoded",
            "data": {
                "choices": ["one", "two"],
                "metadata": {"priority": "high"},
                "name": "demo value"
            }
        }
    });

    let dispatch = plan(&prepared, &arguments);

    assert_eq!(dispatch.method(), reqwest::Method::POST);
    assert_eq!(
        dispatch.url().as_str(),
        "https://api.example.test/v1/widgets/;x=1;y=2?empty=&filter%5Bstate%5D=open&q=a/b?c&tags=red%7Cblue"
    );
    assert_eq!(
        dispatch.headers(),
        &[
            (
                "content-type".to_string(),
                "application/x-www-form-urlencoded".to_string()
            ),
            (
                "cookie".to_string(),
                "color=red&size=large; session=abc".to_string()
            ),
            ("x-mode".to_string(), "fast,safe".to_string())
        ]
    );
    assert_eq!(
        dispatch.body(),
        Some(b"choices=one%7Ctwo&metadata%5Bpriority%5D=high&name=demo+value".as_slice())
    );
    assert_eq!(
        dispatch.request_media_type(),
        Some("application/x-www-form-urlencoded")
    );
    assert!(dispatch.credential_slots().is_empty());
}

#[test]
fn text_request_bodies_use_the_declared_charset_or_are_refused() {
    for (media, text, expected) in [
        (
            "text/plain; charset=windows-1252",
            "café",
            Some(b"caf\xe9".as_slice()),
        ),
        (
            "text/plain; charset=utf-16le",
            "A🦀",
            Some([0x41, 0x00, 0x3e, 0xd8, 0x80, 0xdd].as_slice()),
        ),
        // The label maps to windows-1252, which would mislabel the bytes sent.
        ("text/plain; charset=iso-8859-1", "€", None),
    ] {
        let prepared = ready(&format!(
            r#"openapi: 3.1.0
info: {{ title: Text request, version: "1" }}
servers: [{{ url: https://api.example.test }}]
paths:
  /message:
    post:
      operationId: sendMessage
      requestBody:
        required: true
        content:
          "{media}":
            schema: {{ type: string }}
      responses:
        "204": {{ description: accepted }}
"#
        ));
        let result = try_plan(&prepared, &serde_json::json!({"body": text}));
        match expected {
            Some(bytes) => {
                let dispatch = result.expect(media);
                assert_eq!(dispatch.body(), Some(bytes), "{media}");
                assert_eq!(dispatch.request_media_type(), Some(media));
            }
            None => assert!(
                matches!(result, Err(coral_upstream::Error::InvalidArguments(_))),
                "{media}"
            ),
        }
    }
}

#[test]
fn a_fixed_path_query_precedes_query_arguments() {
    let prepared = ready(
        r#"openapi: 3.1.0
info: { title: Fixed query, version: "1" }
servers: [{ url: "https://api.example.test/v1" }]
paths:
  "/models/{id}?beta=true":
    get:
      operationId: read
      parameters:
        - { name: id, in: path, required: true, schema: { type: string } }
        - { name: limit, in: query, schema: { type: integer } }
      responses:
        "204": { description: done }
"#,
    );
    for (arguments, expected) in [
        (
            serde_json::json!({"id": "m"}),
            "https://api.example.test/v1/models/m?beta=true",
        ),
        (
            serde_json::json!({"id": "m", "limit": 2}),
            "https://api.example.test/v1/models/m?beta=true&limit=2",
        ),
    ] {
        assert_eq!(plan(&prepared, &arguments).url().as_str(), expected);
    }
}

/// One GET Tool at `{server}/v1` + `template`, whose path parameters
/// `a` and `b` use `parameters` as their serialization fields.
fn path_tool(server: &str, template: &str, parameters: &str) -> PreparedProviderCatalog {
    ready(&format!(
        r#"openapi: 3.1.0
info: {{ title: Path arguments, version: "1" }}
servers: [{{ url: "{server}/v1" }}]
paths:
  {template}:
    get:
      operationId: read
      parameters:
        - {{ name: a, in: path, required: true, {parameters} }}
        - {{ name: b, in: path, required: true, {parameters} }}
      responses:
        "204": {{ description: done }}
"#
    ))
}

#[test]
fn a_path_argument_cannot_become_a_dot_segment() {
    const SIMPLE: &str = "schema: { type: string }";
    const LABEL: &str = "style: label, schema: { type: string }";
    const MATRIX: &str = "style: matrix, schema: { type: string }";
    const TEXT: &str = r#"content: { "text/plain": { schema: { type: string } } }"#;
    const JSON: &str = r#"content: { "application/json": { schema: { type: string } } }"#;
    const SPLIT: &str = "/files/{a}/{b}";
    const JOINED: &str = "/files/{a}{b}";
    // Java servlet containers drop a segment's `;` parameters before they
    // resolve dot segments.
    const PARAMETERIZED: &str = "/files/{a};rev/{b}";
    // `None` marks a refused value; otherwise the path the request reaches
    // below `/v1/files/`.
    for (parameters, template, a, b, expected) in [
        (SIMPLE, SPLIT, "..", "x", None),
        (SIMPLE, SPLIT, "x", ".", None),
        (SIMPLE, JOINED, ".", ".", None),
        (SIMPLE, JOINED, ".", "x", Some(".x")),
        (SIMPLE, SPLIT, "%2e%2E", "%2e", Some("%252e%252E/%252e")),
        // An upstream that decodes `%2F` before routing would see these.
        (SIMPLE, SPLIT, "../admin", "x", None),
        (SIMPLE, SPLIT, "x\\..", "x", None),
        (MATRIX, SPLIT, "x/..", "x", None),
        (
            SIMPLE,
            SPLIT,
            "group/project",
            "x",
            Some("group%2Fproject/x"),
        ),
        (SIMPLE, PARAMETERIZED, "..", "x", None),
        (SIMPLE, PARAMETERIZED, "x", "x", Some("x;rev/x")),
        // A proxy that decodes `%3B` hands a servlet container `..;x`.
        (SIMPLE, SPLIT, "..;x", "x", None),
        (SIMPLE, SPLIT, "x;y", "x", Some("x%3By/x")),
        (SIMPLE, SPLIT, "report.v1", "...", Some("report.v1/...")),
        (LABEL, SPLIT, ".", "x", None),
        (LABEL, SPLIT, "", "x", None),
        (LABEL, SPLIT, "x", "x", Some(".x/.x")),
        (MATRIX, SPLIT, "..", ".", Some(";a=../;b=.")),
        (TEXT, SPLIT, "..", "x", None),
        (TEXT, SPLIT, "x", "x", Some("x/x")),
        (JSON, SPLIT, "..", ".", Some("%22..%22/%22.%22")),
    ] {
        let expected = expected.map(|path| format!("/v1/files/{path}"));
        let prepared = path_tool("https://api.example.test", template, parameters);
        let result = try_plan(&prepared, &serde_json::json!({"a": a, "b": b}));
        let case = format!("{parameters} {template} a={a:?} b={b:?}");
        match expected {
            Some(path) => assert_eq!(result.expect(&case).url().path(), path, "{case}"),
            None => assert!(
                matches!(result, Err(coral_upstream::Error::InvalidArguments(_))),
                "{case}"
            ),
        }
    }
    // The Program learns which argument to correct.
    let prepared = path_tool("https://api.example.test", SPLIT, SIMPLE);
    assert!(
        matches!(
            try_plan(&prepared, &serde_json::json!({"a": "x", "b": "../admin"})),
            Err(coral_upstream::Error::InvalidArguments(reason))
                if reason == "Tool argument `b` cannot contain a `.` or `..` path segment"
        ),
        "the reason names the argument"
    );
}

#[tokio::test]
async fn a_traversal_argument_never_reaches_an_undeclared_endpoint() {
    let server = MockServer::start().await;
    Mock::given(path("/v1/files/report/content"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/v1/secrets"))
        .respond_with(ResponseTemplate::new(204))
        .expect(0)
        .mount(&server)
        .await;
    let schema = "schema: { type: string }";
    let prepared = path_tool(&server.uri(), "/files/{a}/{b}", schema);
    let arguments = serde_json::json!({"a": "report", "b": "content"});
    let dispatch = plan(&prepared, &arguments);
    assert_eq!(dispatch.url().path(), "/v1/files/report/content");
    send(dispatch).await.expect("declared endpoint");
    // Unchecked, `/v1/files/../secrets` would normalize to `/v1/secrets`.
    let refused = try_plan(&prepared, &serde_json::json!({"a": "..", "b": "secrets"}));
    assert!(matches!(
        refused,
        Err(coral_upstream::Error::InvalidArguments(_))
    ));
    server.verify().await;
}

#[tokio::test]
async fn complete_redirected_non_success_response_preserves_http_evidence() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/start"))
        .respond_with(
            ResponseTemplate::new(302).insert_header("Location", "/final?temporary_token=secret"),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/final"))
        .respond_with(
            ResponseTemplate::new(422)
                .insert_header("Content-Type", "application/problem+json; charset=utf-8")
                .append_header("X-Quota", "first")
                .append_header("X-Quota", "second")
                .insert_header("Link", "</page-2>; rel=next")
                .set_body_raw(
                    r#"{"unexpected":true}"#,
                    "application/problem+json; charset=utf-8",
                ),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/page-2"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;

    let prepared = ready(&format!(
        r#"openapi: 3.1.0
info:
  title: HTTP outcomes
  version: "1"
servers:
  - url: {server}
paths:
  /start:
    get:
      operationId: startJob
      responses:
        "302":
          description: continue
          headers:
            Location:
              schema: {{ type: string }}
        "422":
          description: rejected
          headers:
            X-Quota:
              schema: {{ type: string }}
            Link:
              schema: {{ type: string }}
          content:
            "*/*":
              schema: {{ type: object }}
            application/problem+json:
              schema:
                type: object
                required: [message]
                properties:
                  message: {{ type: string }}
"#,
        server = server.uri()
    ));
    let dispatch = plan(&prepared, &serde_json::json!({}));

    let outcome = send(dispatch)
        .await
        .expect("a complete response is an outcome");

    assert_eq!(outcome.status(), 422);
    assert_eq!(
        outcome.data(),
        &serde_json::json!({"unexpected": true}),
        "{outcome:?}"
    );
    assert_eq!(outcome.media_type(), Some("application/problem+json"));
    assert_eq!(outcome.charset(), Some("utf-8"));
    assert!(outcome.final_url().ends_with("/final"));
    assert!(!outcome.final_url().contains("temporary_token"));
    let [redirect] = outcome.redirects() else {
        panic!("one redirect should be retained");
    };
    assert_eq!(redirect.status(), 302);
    assert!(redirect.url().ends_with("/final"));
    assert!(!redirect.url().contains("temporary_token"));
    assert!(!redirect.cross_origin());
    assert_eq!(
        outcome
            .headers()
            .iter()
            .filter(|(name, _value)| name == "x-quota")
            .map(|(_name, value)| value.as_str())
            .collect::<Vec<_>>(),
        vec!["first", "second"]
    );
    let [diagnostic] = outcome.decode_diagnostics() else {
        panic!("schema mismatch should retain one diagnostic");
    };
    assert_eq!(diagnostic.code(), "schema_incompatible");
}

/// A response a fixture server sends, and what decoding it must report.
struct DecodeCase<'a> {
    status: u16,
    content_type: Option<&'static str>,
    body: &'static [u8],
    /// The 200 response's declared `content`, as YAML.
    declared: &'a str,
    data: serde_json::Value,
    media_type: Option<&'static str>,
    charset: Option<&'static str>,
    diagnostics: &'static [&'static str],
}

async fn decode(case: &DecodeCase<'_>) -> HttpOutcome {
    let server = MockServer::start().await;
    let mut response = ResponseTemplate::new(case.status).set_body_bytes(case.body);
    if let Some(content_type) = case.content_type {
        response = response.insert_header("content-type", content_type);
    }
    Mock::given(method("GET"))
        .respond_with(response)
        .mount(&server)
        .await;
    let declared = case.declared.replace('\n', "\n            ");
    let prepared = ready(&format!(
        r#"openapi: 3.1.0
info: {{ title: Decoding, version: "1" }}
servers: [{{ url: {} }}]
paths:
  /value:
    get:
      operationId: getValue
      responses:
        "200":
          description: value
          content:
            {declared}
"#,
        server.uri()
    ));
    send(plan(&prepared, &serde_json::json!({})))
        .await
        .expect("a complete response is an outcome")
}

#[tokio::test]
async fn complete_responses_decode_by_their_declared_media_and_schema() {
    let object = "application/json:\n  schema: { type: object }";
    let with_id = "{ type: object, required: [id], properties: { id: { type: string } } }";
    for case in [
        // An undeclared status and a malformed body still resolve as text.
        DecodeCase {
            status: 418,
            content_type: Some("application/json; charset=utf-8"),
            body: b"{broken",
            declared: object,
            data: serde_json::json!("{broken"),
            media_type: Some("application/json"),
            charset: Some("utf-8"),
            diagnostics: &[
                "undeclared_status",
                "undeclared_media_type",
                "malformed_body",
            ],
        },
        // A malformed Content-Type is not replaced by the declared alternative.
        DecodeCase {
            status: 200,
            content_type: Some("not-a-media-type"),
            body: br#"{"id":"one"}"#,
            declared: object,
            data: serde_json::json!({"_tag":"BinaryValue", "encoding":"base64", "data":"eyJpZCI6Im9uZSJ9", "mimeType":"application/octet-stream", "byteLength":12}),
            media_type: None,
            charset: None,
            diagnostics: &["malformed_content_type", "undeclared_media_type"],
        },
        // An empty body that the schema needs is reported, not refused.
        DecodeCase {
            status: 200,
            content_type: None,
            body: b"",
            declared: &format!("application/json:\n  schema: {with_id}"),
            data: serde_json::Value::Null,
            media_type: Some("application/json"),
            charset: None,
            diagnostics: &["schema_incompatible"],
        },
        // OpenAPI 3.1 ignores `nullable`, so null keeps its data but is reported.
        DecodeCase {
            status: 200,
            content_type: Some("application/json"),
            body: b"null",
            declared: "application/json:\n  schema: { type: string, nullable: true }",
            data: serde_json::Value::Null,
            media_type: Some("application/json"),
            charset: Some("utf-8"),
            diagnostics: &["schema_incompatible"],
        },
        DecodeCase {
            status: 200,
            content_type: Some("application/json; charset=windows-1252"),
            body: b"{\"message\":\"caf\xe9\"}",
            declared: object,
            data: serde_json::json!({"message": "café"}),
            media_type: Some("application/json"),
            charset: Some("windows-1252"),
            diagnostics: &[],
        },
        // Media type parameters select between alternatives.
        DecodeCase {
            status: 200,
            content_type: Some("application/json; profile=two; charset=UTF-8"),
            body: br#"{"id":"one"}"#,
            declared: &format!(
                "application/json; profile=one:\n  schema: {{ type: string }}\n\"application/json; profile=two; charset=utf-8\":\n  schema: {with_id}"
            ),
            data: serde_json::json!({"id": "one"}),
            media_type: Some("application/json"),
            charset: Some("utf-8"),
            diagnostics: &[],
        },
    ] {
        let outcome = decode(&case).await;
        assert_eq!(outcome.status(), case.status);
        assert_eq!(
            (
                outcome.data(),
                outcome.media_type(),
                outcome.charset(),
                outcome
                    .decode_diagnostics()
                    .iter()
                    .map(coral_upstream::HttpDecodeDiagnostic::code)
                    .collect::<Vec<_>>()
                    .as_slice(),
            ),
            (&case.data, case.media_type, case.charset, case.diagnostics),
            "{:?} {}",
            case.content_type,
            case.declared
        );
    }
}

/// A response to a Tool declaring `responses`, and its class.
struct ClassCase {
    name: &'static str,
    /// The Tool's declared `responses`, as YAML.
    responses: &'static str,
    /// The first request answers `302` to the response below.
    redirected: bool,
    status: u16,
    content_type: Option<&'static str>,
    body: &'static [u8],
    class: OutcomeClass,
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table holds every class a response can take"
)]
async fn each_outcome_is_a_declared_success_or_names_why_it_is_not() {
    const JSON_200: &str = r#"
        "200":
          description: ok
          content:
            application/json:
              schema: { type: object, required: [id], properties: { id: { type: string } } }"#;
    const JSON_200_AND_404: &str = r#"
        "200":
          description: ok
          content:
            application/json:
              schema: { type: object }
        "404":
          description: missing
          content:
            application/json:
              schema: { type: object }"#;
    const DEFAULT_ONLY: &str = r"
        default:
          description: any
          content:
            application/json:
              schema: { type: object }";
    const OK_AND_DEFAULT: &str = r#"
        "200": { description: ok }
        default:
          description: error
          content:
            application/json:
              schema: { type: object }"#;
    const RANGE_2XX: &str = r#"
        "2XX": { description: any success }"#;
    const NO_CONTENT_200: &str = r#"
        "200": { description: ok }"#;
    const REDIRECT_ONLY: &str = r#"
        "302": { description: to the file }"#;
    const TWO_MEDIA_200: &str = r#"
        "200":
          description: ok
          content:
            application/json:
              schema: { type: object }
            text/csv:
              schema: { type: string }"#;
    let json = Some("application/json");
    for case in [
        ClassCase {
            name: "a declared 200 that matches its schema",
            responses: JSON_200,
            redirected: false,
            status: 200,
            content_type: json,
            body: br#"{"id":"one"}"#,
            class: OutcomeClass::Success,
        },
        ClassCase {
            name: "a declared 200 that misses its schema is still a success",
            responses: JSON_200,
            redirected: false,
            status: 200,
            content_type: json,
            body: br#"{"id":1}"#,
            class: OutcomeClass::Success,
        },
        ClassCase {
            name: "a declared 404",
            responses: JSON_200_AND_404,
            redirected: false,
            status: 404,
            content_type: json,
            body: br#"{"message":"missing"}"#,
            class: OutcomeClass::ErrorResponse,
        },
        ClassCase {
            name: "an undeclared 500",
            responses: JSON_200,
            redirected: false,
            status: 500,
            content_type: None,
            body: b"",
            class: OutcomeClass::ErrorResponse,
        },
        ClassCase {
            name: "a 2xx the Tool does not declare",
            responses: JSON_200,
            redirected: false,
            status: 201,
            content_type: json,
            body: br#"{"id":"one"}"#,
            class: OutcomeClass::UndeclaredResponse,
        },
        ClassCase {
            name: "a 2xx that only `default` matches beside a declared 200",
            responses: OK_AND_DEFAULT,
            redirected: false,
            status: 201,
            content_type: json,
            body: b"{}",
            class: OutcomeClass::UndeclaredResponse,
        },
        ClassCase {
            name: "a 2xx when `default` is the only declared response",
            responses: DEFAULT_ONLY,
            redirected: false,
            status: 200,
            content_type: json,
            body: b"{}",
            class: OutcomeClass::Success,
        },
        ClassCase {
            name: "a 4xx when `default` is the only declared response",
            responses: DEFAULT_ONLY,
            redirected: false,
            status: 400,
            content_type: json,
            body: b"{}",
            class: OutcomeClass::ErrorResponse,
        },
        ClassCase {
            name: "a status in a declared 2XX range",
            responses: RANGE_2XX,
            redirected: false,
            status: 204,
            content_type: None,
            body: b"",
            class: OutcomeClass::Success,
        },
        ClassCase {
            name: "a body for a success that declares no content",
            responses: NO_CONTENT_200,
            redirected: false,
            status: 200,
            content_type: json,
            body: br#"{"anything":true}"#,
            class: OutcomeClass::Success,
        },
        ClassCase {
            name: "a media type the declared success does not list",
            responses: JSON_200,
            redirected: false,
            status: 200,
            content_type: Some("text/html"),
            body: b"<html></html>",
            class: OutcomeClass::UndeclaredResponse,
        },
        ClassCase {
            name: "a body that is not valid JSON",
            responses: JSON_200,
            redirected: false,
            status: 200,
            content_type: json,
            body: b"{broken",
            class: OutcomeClass::UndecodableResponse,
        },
        ClassCase {
            name: "a malformed Content-Type",
            responses: JSON_200,
            redirected: false,
            status: 200,
            content_type: Some("not-a-media-type"),
            body: br#"{"id":"one"}"#,
            class: OutcomeClass::UndecodableResponse,
        },
        ClassCase {
            name: "an unsupported charset",
            responses: JSON_200,
            redirected: false,
            status: 200,
            content_type: Some("application/json; charset=x-unknown"),
            body: br#"{"id":"one"}"#,
            class: OutcomeClass::UndecodableResponse,
        },
        ClassCase {
            name: "an empty body for a success that declares no content, under a malformed Content-Type",
            responses: NO_CONTENT_200,
            redirected: false,
            status: 200,
            content_type: Some("not-a-media-type"),
            body: b"",
            class: OutcomeClass::Success,
        },
        ClassCase {
            name: "an empty body without a Content-Type for a success with two media types",
            responses: TWO_MEDIA_200,
            redirected: false,
            status: 200,
            content_type: None,
            body: b"",
            class: OutcomeClass::Success,
        },
        ClassCase {
            name: "an undeclared 2xx is undeclared even when its body is malformed",
            responses: JSON_200,
            redirected: false,
            status: 201,
            content_type: json,
            body: b"{broken",
            class: OutcomeClass::UndeclaredResponse,
        },
        ClassCase {
            name: "the final 2xx after a declared redirect",
            responses: REDIRECT_ONLY,
            redirected: true,
            status: 200,
            content_type: Some("application/octet-stream"),
            body: b"file",
            class: OutcomeClass::Success,
        },
        ClassCase {
            name: "a final 2xx after a declared redirect whose body does not decode",
            responses: REDIRECT_ONLY,
            redirected: true,
            status: 200,
            content_type: json,
            body: b"{broken",
            class: OutcomeClass::UndecodableResponse,
        },
        ClassCase {
            name: "a final 4xx after a declared redirect",
            responses: REDIRECT_ONLY,
            redirected: true,
            status: 404,
            content_type: None,
            body: b"",
            class: OutcomeClass::ErrorResponse,
        },
    ] {
        let server = MockServer::start().await;
        let mut response = ResponseTemplate::new(case.status).set_body_bytes(case.body);
        if let Some(content_type) = case.content_type {
            response = response.insert_header("content-type", content_type);
        }
        if case.redirected {
            Mock::given(path("/value"))
                .respond_with(ResponseTemplate::new(302).insert_header("location", "/final"))
                .mount(&server)
                .await;
            Mock::given(path("/final"))
                .respond_with(response)
                .mount(&server)
                .await;
        } else {
            Mock::given(path("/value"))
                .respond_with(response)
                .mount(&server)
                .await;
        }
        let prepared = ready(&format!(
            r#"openapi: 3.1.0
info: {{ title: Classes, version: "1" }}
servers: [{{ url: {} }}]
paths:
  /value:
    get:
      operationId: getValue
      responses:{}
"#,
            server.uri(),
            case.responses
        ));
        let outcome = send(plan(&prepared, &serde_json::json!({})))
            .await
            .expect("a complete response is an outcome");
        assert_eq!(outcome.class(), case.class, "{}", case.name);
    }
}

#[tokio::test]
async fn openapi_direction_annotations_do_not_create_false_incompatibility() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "name": "example"
        })))
        .mount(&server)
        .await;
    let prepared = ready(&format!(
        r#"openapi: 3.1.0
info: {{ title: Directional schemas, version: "1" }}
servers: [{{ url: {} }}]
paths:
  /items:
    post:
      operationId: createItem
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required: [name, server_id]
              properties:
                name: {{ type: string }}
                server_id: {{ type: string, readOnly: true }}
      responses:
        "200":
          description: item
          content:
            application/json:
              schema:
                allOf:
                  - type: object
                    properties:
                      secret: {{ type: string, writeOnly: true }}
                  - type: object
                    required: [name, secret]
                    properties:
                      name: {{ type: string }}
"#,
        server.uri()
    ));
    let dispatch = plan(&prepared, &serde_json::json!({"body": {"name": "example"}}));

    let outcome = send(dispatch)
        .await
        .expect("write-only response property does not become required");

    assert_eq!(outcome.data(), &serde_json::json!({"name": "example"}));
    assert!(outcome.decode_diagnostics().is_empty());
}

#[tokio::test]
async fn see_other_discards_a_get_request_body() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/start"))
        .respond_with(ResponseTemplate::new(303).insert_header("Location", "/final"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/final"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::Value::Null))
        .mount(&server)
        .await;
    let prepared = ready(&format!(
        r#"openapi: 3.1.0
info: {{ title: See Other, version: "1" }}
servers: [{{ url: {} }}]
paths:
  /start:
    get:
      operationId: getRedirected
      parameters:
        - name: Content-Language
          in: header
          schema: {{ type: string }}
      requestBody:
        required: true
        content:
          application/json:
            schema: {{ type: object }}
      responses:
        "200":
          description: final
          content:
            application/json:
              schema: {{ type: "null" }}
        "303":
          description: redirect
"#,
        server.uri()
    ));
    let dispatch = plan(
        &prepared,
        &serde_json::json!({
            "Content-Language": "en",
            "body": {"query": "one"}
        }),
    );

    send(dispatch).await.expect("follow See Other");

    let requests = server.received_requests().await.expect("recorded requests");
    let final_request = requests
        .iter()
        .find(|request| request.url.path() == "/final")
        .expect("redirected request");
    assert!(final_request.body.is_empty());
    assert!(!final_request.headers.contains_key("content-type"));
    assert!(!final_request.headers.contains_key("content-language"));
}

#[tokio::test]
async fn cross_origin_redirect_drops_all_caller_headers_and_cookies() {
    let (source, sink) = cross_origin_pair("POST").await;
    let prepared = ready(&format!(
        r#"
openapi: 3.1.0
info: {{title: Redirect secrets, version: "1"}}
servers: [{{url: {}}}]
components:
  securitySchemes:
    bearer: {{type: http, scheme: bearer}}
security: [{{bearer: []}}]
paths:
  /:
    post:
      operationId: send
      parameters:
        - {{name: X-Secret, in: header, schema: {{type: string}}}}
        - {{name: Content-Language, in: header, schema: {{type: string}}}}
        - {{name: session, in: cookie, schema: {{type: string}}}}
      requestBody:
        content:
          application/json:
            schema: {{type: object}}
      responses:
        "204": {{description: done}}
"#,
        source.uri()
    ));
    let mut dispatch = plan(
        &prepared,
        &serde_json::json!({"X-Secret":"caller-canary", "Content-Language":"header-canary", "session":"cookie-canary", "body":{"keep":true}}),
    );
    dispatch
        .authorize(&coral_credentials::CredentialValues::token(
            coral_credentials::Secret::new("host-canary".into()).unwrap(),
        ))
        .unwrap();
    send(dispatch).await.unwrap();
    let requests = sink.received_requests().await.unwrap();
    let request = requests.first().unwrap();
    for name in ["authorization", "x-secret", "content-language", "cookie"] {
        assert!(!request.headers.contains_key(name), "leaked {name}");
    }
    assert_eq!(
        request.headers.get("content-type").unwrap(),
        "application/json"
    );
    assert_eq!(request.body, br#"{"keep":true}"#);
    let requests = source.received_requests().await.unwrap();
    assert_eq!(
        requests.first().unwrap().headers.get("x-secret").unwrap(),
        "caller-canary"
    );
    assert_eq!(
        requests
            .first()
            .unwrap()
            .headers
            .get("authorization")
            .unwrap(),
        "Bearer host-canary"
    );
}

#[tokio::test]
async fn public_deployment_blocks_local_dispatch_and_private_redirects() {
    let source = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("location", "https://169.254.169.254/latest/meta-data"),
        )
        .expect(1)
        .mount(&source)
        .await;
    let prepared = ready(&format!(
        r#"
openapi: 3.1.0
info: {{title: Destination policy, version: "1"}}
servers: [{{url: {}}}]
paths:
  /:
    get:
      operationId: fetch
      responses:
        "200": {{description: data}}
"#,
        source.uri()
    ));
    let dispatch = || plan(&prepared, &serde_json::json!({}));
    assert!(matches!(
        HttpUpstream::new(DestinationPolicy::default())
            .unwrap()
            .send(dispatch())
            .await,
        Err(coral_upstream::Error::DestinationRejected)
    ));
    assert!(source.received_requests().await.unwrap().is_empty());
    assert!(matches!(
        send(dispatch()).await,
        Err(coral_upstream::Error::DestinationRejected)
    ));
    source.verify().await;
}

/// One POST Tool at `/` that takes a JSON body.
const POST_JSON: &str = r#"
  /:
    post:
      operationId: send
      requestBody:
        content:
          application/json:
            schema: {type: object}
      responses:
        "204": {description: done}
"#;

/// One GET Tool at `/`.
const GET_ROOT: &str = r#"
  /:
    get:
      operationId: read
      responses:
        "204": {description: done}
"#;

/// One GET Tool at `/boards` with an ordinary `limit` argument.
const GET_BOARDS: &str = r#"
  /boards:
    get:
      operationId: boards
      parameters:
        - {name: limit, in: query, schema: {type: integer}}
      responses:
        "204": {description: done}
"#;

fn header_credentials(bindings: &[(&str, HeaderBinding)]) -> AuthenticationConfiguration {
    AuthenticationConfiguration::HeaderCredentials {
        headers: bindings
            .iter()
            .map(|(name, binding)| ((*name).to_string(), binding.clone()))
            .collect(),
    }
}

fn field_header(field: &str, prefix: &str) -> HeaderBinding {
    HeaderBinding::Field {
        field: field.into(),
        prefix: prefix.into(),
    }
}

fn field(name: &str) -> CredentialSource {
    CredentialSource::Field { field: name.into() }
}

fn basic_auth(
    username: CredentialSource,
    password: CredentialSource,
) -> AuthenticationConfiguration {
    AuthenticationConfiguration::BasicAuth { username, password }
}

/// Trello's pair of query credentials: `key` and `token`.
fn trello() -> AuthenticationConfiguration {
    AuthenticationConfiguration::QueryCredentials {
        parameters: [("key", field("api_key")), ("token", field("api_token"))]
            .into_iter()
            .map(|(name, source)| (name.to_string(), source))
            .collect(),
    }
}

fn values(pairs: &[(&str, &str)]) -> coral_credentials::CredentialValues {
    coral_credentials::CredentialValues::new(
        pairs
            .iter()
            .map(|(name, value)| {
                (
                    (*name).to_string(),
                    coral_credentials::Secret::new((*value).to_string()).expect("fixture"),
                )
            })
            .collect(),
    )
    .expect("fixture")
}

#[tokio::test]
async fn header_credentials_send_each_bound_header_and_none_after_a_cross_origin_redirect() {
    let (source, sink) = cross_origin_pair("POST").await;
    let prepared = configured(
        &source.uri(),
        POST_JSON,
        header_credentials(&[
            ("authorization", field_header("api_key", "Token token=")),
            ("dd-application-key", field_header("application_key", "")),
            (
                "x-client",
                HeaderBinding::Literal {
                    value: "coral".into(),
                },
            ),
        ]),
    );
    let mut dispatch = plan(&prepared, &serde_json::json!({"body":{}}));
    dispatch
        .authorize(&values(&[
            ("api_key", "api-canary"),
            ("application_key", "app-canary"),
        ]))
        .unwrap();
    send(dispatch).await.unwrap();

    let sent = source.received_requests().await.unwrap();
    let headers = &sent.first().unwrap().headers;
    assert_eq!(
        headers.get("authorization").unwrap(),
        "Token token=api-canary"
    );
    assert_eq!(headers.get("dd-application-key").unwrap(), "app-canary");
    assert_eq!(headers.get("x-client").unwrap(), "coral");
    let redirected = sink.received_requests().await.unwrap();
    let headers = &redirected.first().unwrap().headers;
    for name in ["authorization", "dd-application-key", "x-client"] {
        assert!(!headers.contains_key(name), "leaked {name}");
    }
}

#[tokio::test]
async fn each_method_requires_every_field() {
    for (paths, authentication, arguments, missing_one, every) in [
        (
            POST_JSON,
            header_credentials(&[("x-api-key", field_header("api_key", ""))]),
            serde_json::json!({"body": {}}),
            values(&[("other", "value")]),
            values(&[("api_key", "value")]),
        ),
        (
            GET_ROOT,
            basic_auth(field("email"), field("api_token")),
            serde_json::json!({}),
            values(&[("api_token", "token")]),
            values(&[("api_token", "token"), ("email", "person@example.test")]),
        ),
        (
            GET_BOARDS,
            trello(),
            serde_json::json!({}),
            values(&[("api_key", "key-canary")]),
            trello_values(),
        ),
    ] {
        let prepared = configured("http://127.0.0.1:9", paths, authentication);
        let mut dispatch = plan(&prepared, &arguments);
        assert!(matches!(
            dispatch.authorize(&missing_one),
            Err(coral_upstream::Error::CredentialRequired)
        ));
        // The refusal attached nothing, so the complete set is still accepted.
        dispatch
            .authorize(&every)
            .expect("a refused dispatch stays unauthorized");
    }
}

#[tokio::test]
async fn a_header_credential_cannot_replace_a_planned_header() {
    let prepared = configured(
        "http://127.0.0.1:9",
        POST_JSON,
        header_credentials(&[("content-type", field_header("api_key", ""))]),
    );
    let mut dispatch = plan(&prepared, &serde_json::json!({"body":{}}));
    assert!(matches!(
        dispatch.authorize(&values(&[("api_key", "value")])),
        Err(coral_upstream::Error::CredentialRequired)
    ));
}

#[tokio::test]
async fn credential_user_agent_overrides_the_default_only_on_its_own_origin() {
    let (source, sink) = cross_origin_pair("POST").await;
    let prepared = configured(
        &source.uri(),
        POST_JSON,
        header_credentials(&[("user-agent", field_header("agent", ""))]),
    );
    let mut dispatch = plan(&prepared, &serde_json::json!({"body": {}}));
    dispatch
        .authorize(&values(&[("agent", "private-agent-canary")]))
        .expect("credential may override a transport default");
    send(dispatch).await.expect("response");
    for (server, expected) in [(&source, "private-agent-canary"), (&sink, "Coral")] {
        let requests = server.received_requests().await.expect("requests");
        let request = requests.first().expect("request");
        assert_eq!(
            request
                .headers
                .get_all("user-agent")
                .iter()
                .collect::<Vec<_>>(),
            vec![expected],
            "the private User-Agent must not cross the origin boundary"
        );
    }
}

#[tokio::test]
async fn basic_auth_encodes_the_username_and_password_itself() {
    let upstream = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&upstream)
        .await;
    for (username, password, values, expected) in [
        (
            field("email"),
            field("api_token"),
            values(&[
                ("api_token", "token-canary"),
                ("email", "person@example.test"),
            ]),
            "Basic cGVyc29uQGV4YW1wbGUudGVzdDp0b2tlbi1jYW5hcnk=",
        ),
        (
            CredentialSource::Literal {
                value: "api".into(),
            },
            field("key"),
            values(&[("key", "key-canary")]),
            "Basic YXBpOmtleS1jYW5hcnk=",
        ),
    ] {
        let prepared = configured(&upstream.uri(), GET_ROOT, basic_auth(username, password));
        let mut dispatch = plan(&prepared, &serde_json::json!({}));
        dispatch.authorize(&values).unwrap();
        send(dispatch).await.unwrap();
        let sent = upstream.received_requests().await.unwrap();
        assert_eq!(
            sent.last().unwrap().headers.get("authorization").unwrap(),
            expected
        );
    }
}

#[tokio::test]
async fn basic_auth_refuses_a_colon_in_the_username() {
    let prepared = configured(
        "http://127.0.0.1:9",
        GET_ROOT,
        basic_auth(field("email"), field("api_token")),
    );
    // Basic splits at the first colon, so the service would read a different username.
    assert!(matches!(
        plan(&prepared, &serde_json::json!({})).authorize(&values(&[
            ("api_token", "token"),
            ("email", "person:other")
        ])),
        Err(coral_upstream::Error::CredentialUnusable)
    ));
}

fn trello_values() -> coral_credentials::CredentialValues {
    values(&[("api_key", "key-canary"), ("api_token", "token canary&=")])
}

fn query_pairs(request: &wiremock::Request) -> Vec<(String, String)> {
    request
        .url
        .query_pairs()
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect()
}

#[tokio::test]
async fn query_credentials_follow_a_same_origin_redirect_but_never_reach_evidence() {
    let upstream = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/boards"))
        .respond_with(ResponseTemplate::new(307).insert_header("location", "/moved"))
        .mount(&upstream)
        .await;
    Mock::given(method("GET"))
        .and(path("/moved"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&upstream)
        .await;
    let prepared = configured(&upstream.uri(), GET_BOARDS, trello());
    let mut dispatch = plan(&prepared, &serde_json::json!({"limit": 5}));
    dispatch.authorize(&trello_values()).unwrap();
    assert!(
        !dispatch.url().as_str().contains("canary"),
        "the planned URL stays secret-free"
    );
    let outcome = send(dispatch).await.unwrap();

    let sent = upstream.received_requests().await.unwrap();
    let [first, moved] = sent.as_slice() else {
        panic!("two requests");
    };
    let expected = [
        ("limit", "5"),
        ("key", "key-canary"),
        ("token", "token canary&="),
    ]
    .map(|(name, value)| (name.to_string(), value.to_string()));
    assert_eq!(query_pairs(first), expected);
    // The redirect target is resolved from the secret-free URL, so nothing repeats.
    assert_eq!(
        query_pairs(moved)
            .into_iter()
            .filter(|(name, _)| name != "limit")
            .collect::<Vec<_>>(),
        expected.get(1..).unwrap()
    );
    let evidence = format!(
        "{} {:?} {}",
        outcome.final_url(),
        outcome.redirects(),
        outcome.tool_result()
    );
    assert!(!evidence.contains("canary"), "{evidence}");
}

#[tokio::test]
async fn a_redirect_target_cannot_shadow_a_credential_parameter() {
    let upstream = MockServer::start().await;
    // The service sends back a parameter Coral carries, so the call cannot say
    // which value it means.
    Mock::given(method("GET"))
        .and(path("/boards"))
        .respond_with(ResponseTemplate::new(307).insert_header("location", "/moved?key=theirs"))
        .mount(&upstream)
        .await;
    let prepared = configured(&upstream.uri(), GET_BOARDS, trello());
    let mut dispatch = plan(&prepared, &serde_json::json!({"limit": 5}));
    dispatch.authorize(&trello_values()).unwrap();
    let error = send(dispatch).await.unwrap_err();
    assert!(matches!(error, coral_upstream::Error::CredentialUnusable));
    let text = format!("{error} {error:?}");
    assert!(!text.contains("canary"), "{text}");
}

#[test]
fn a_query_credential_is_counted_as_the_url_carries_it() {
    let prepared = configured("http://127.0.0.1:9", GET_BOARDS, trello());
    let dispatch = |budget: usize| {
        ReadyHttpDispatch::plan(
            prepared.tools().first().expect("Tool"),
            prepared.schema_graph(),
            &serde_json::json!({}),
            limits(budget),
        )
        .expect("the secret-free request fits")
    };
    // `key=key-canary` and `token=token+canary%26%3D`, each with its separator.
    // `token canary&=` costs 18 bytes encoded, not 14: the budget counts what
    // the URL carries.
    let exact = dispatch(4096).url().as_str().len() + 15 + 25;
    assert!(matches!(
        dispatch(exact - 1).authorize(&trello_values()),
        Err(coral_upstream::Error::InputTooLarge)
    ));
    dispatch(exact)
        .authorize(&trello_values())
        .expect("the exact size fits");
}

#[tokio::test]
async fn query_credentials_are_dropped_after_a_cross_origin_redirect() {
    let (source, sink) = cross_origin_pair("GET").await;
    let prepared = configured(&source.uri(), GET_BOARDS, trello());
    let mut dispatch = plan(&prepared, &serde_json::json!({}));
    dispatch.authorize(&trello_values()).unwrap();
    send(dispatch).await.unwrap();

    let redirected = sink.received_requests().await.unwrap();
    let redirected = redirected.first().unwrap();
    assert!(redirected.url.query().is_none(), "{}", redirected.url);
}

#[tokio::test]
async fn query_credentials_stay_out_of_send_errors() {
    // Nothing listens on port 9, so the send fails after the URL is built.
    let prepared = configured("http://127.0.0.1:9", GET_BOARDS, trello());
    let mut dispatch = plan(&prepared, &serde_json::json!({}));
    dispatch.authorize(&trello_values()).unwrap();
    // A dispatch is printed in diagnostics, so its query credentials stay redacted.
    let printed = format!("{dispatch:?}");
    assert!(!printed.contains("canary"), "{printed}");
    let error = send(dispatch).await.unwrap_err();
    let text = format!("{error} {error:?}");
    assert!(!text.contains("canary"), "{text}");
}

#[tokio::test]
async fn configured_service_headers_are_sent_without_credentials_and_dropped_across_origins() {
    let (source, sink) = cross_origin_pair("GET").await;
    let document = OpenApiDocument::parse(&serde_json::json!({
        "openapi": "3.1.0", "info": { "title": "Headers", "version": "1" },
        "servers": [{ "url": source.uri() }], "security": [],
        "paths": {"/": {"get": {"operationId": "fetch", "responses": {"204": {"description": "Done"}}}}}
    }).to_string()).unwrap();
    let mut configuration = document.configuration_review().unwrap().suggested;
    configuration
        .request_headers
        .insert("x-service-version".into(), "2022-11-28".into());
    let prepared = ready_input(&document.configure(configuration).unwrap().into());
    send(plan(&prepared, &serde_json::json!({}))).await.unwrap();
    let requests = source.received_requests().await.unwrap();
    assert_eq!(
        requests
            .first()
            .unwrap()
            .headers
            .get("x-service-version")
            .unwrap(),
        "2022-11-28"
    );
    assert!(
        !sink
            .received_requests()
            .await
            .unwrap()
            .first()
            .unwrap()
            .headers
            .contains_key("x-service-version")
    );
}

#[tokio::test]
async fn a_redirect_loop_stops_at_the_redirect_limit() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(302).insert_header("location", "/"))
        .mount(&server)
        .await;
    let prepared = configured(&server.uri(), GET_ROOT, AuthenticationConfiguration::None);
    let outcome = send(plan(&prepared, &serde_json::json!({})))
        .await
        .expect("the last redirect is the outcome");
    assert_eq!(outcome.status(), 302);
    assert_eq!(outcome.redirects().len(), 10);
    assert_eq!(server.received_requests().await.unwrap().len(), 11);
}

#[test]
fn a_planned_request_over_the_budget_is_refused() {
    let prepared = ready(
        r#"openapi: 3.1.0
info: { title: Request size, version: "1" }
servers: [{ url: https://api.example.test }]
paths:
  /:
    get:
      operationId: fetch
      parameters:
        - { name: X-Note, in: header, schema: { type: string } }
      responses:
        "204": { description: done }
"#,
    );
    let arguments = serde_json::json!({"X-Note": "n".repeat(100)});
    let dispatch = |budget: usize| {
        ReadyHttpDispatch::plan(
            prepared.tools().first().expect("Tool"),
            prepared.schema_graph(),
            &arguments,
            limits(budget),
        )
    };
    // The URL, then the header's name and value.
    let exact = "https://api.example.test/".len() + "x-note".len() + 100;
    assert!(matches!(
        dispatch(exact - 1),
        Err(coral_upstream::Error::InputTooLarge)
    ));
    dispatch(exact).expect("the exact size fits");
}

#[tokio::test]
async fn a_response_over_the_budget_is_refused() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(format!("\"{}\"", "x".repeat(200)), "application/json"),
        )
        .mount(&server)
        .await;
    let prepared = configured(&server.uri(), GET_ROOT, AuthenticationConfiguration::None);
    let dispatch = |budget: usize| {
        ReadyHttpDispatch::plan(
            prepared.tools().first().expect("Tool"),
            prepared.schema_graph(),
            &serde_json::json!({}),
            limits(budget),
        )
        .expect("plan HTTP request")
    };
    // The request fits, but the 202-byte body alone is over the budget.
    assert!(matches!(
        send(dispatch(201)).await,
        Err(coral_upstream::Error::ResultTooLarge)
    ));
    // The body fits, but the result also carries its HTTP evidence.
    let result = send(dispatch(4096))
        .await
        .expect("response")
        .tool_result()
        .to_string()
        .len();
    assert!(matches!(
        send(dispatch(result - 1)).await,
        Err(coral_upstream::Error::ResultTooLarge)
    ));
    send(dispatch(result)).await.expect("the exact size fits");
}

/// Sends `headers` from a fixture service as one complete 201 response, with
/// `authentication` presented using `credentials`, and returns the pairs Coral
/// retains under the sent names, in response order.
async fn retained_headers(
    headers: &[(&str, &str)],
    authentication: AuthenticationConfiguration,
    credentials: Option<&coral_credentials::CredentialValues>,
) -> Vec<(String, String)> {
    let upstream = MockServer::start().await;
    let response = headers
        .iter()
        .fold(ResponseTemplate::new(201), |response, (name, value)| {
            response.append_header(*name, *value)
        });
    Mock::given(method("GET"))
        .respond_with(response)
        .mount(&upstream)
        .await;
    let prepared = configured(&upstream.uri(), GET_BOARDS, authentication);
    let mut dispatch = plan(&prepared, &serde_json::json!({}));
    if let Some(credentials) = credentials {
        dispatch
            .authorize(credentials)
            .expect("fixture credentials");
    }
    let outcome = send(dispatch)
        .await
        .expect("a complete response is an outcome");
    let result = outcome.tool_result().to_string();
    assert!(!result.contains("canary"), "{result}");
    outcome
        .headers()
        .iter()
        .filter(|(name, _value)| {
            headers
                .iter()
                .any(|(sent, _value)| sent.eq_ignore_ascii_case(name))
        })
        .cloned()
        .collect()
}

/// (name, value the service sends, value Coral retains)
const SENSITIVE_HEADER_CASES: &[(&str, &str, &str)] = &[
    ("Set-Cookie", "session=canary-one; HttpOnly", "[redacted]"),
    ("Set-Cookie", "refresh=canary-two", "[redacted]"),
    ("Authorization", "Bearer canary-authorization", "[redacted]"),
    ("Proxy-Authorization", "Basic canary-proxy", "[redacted]"),
    (
        "WWW-Authenticate",
        r#"Bearer realm="canary-realm""#,
        "[redacted]",
    ),
    ("X-API-Key", "canary-api-key", "[redacted]"),
    (
        "Location",
        "https://person:canary-password@example.test/items/7?page=2&access_token=canary-location#canary-fragment",
        "https://example.test/items/7?page=2&access_token=[redacted]",
    ),
    (
        "Location",
        "//person:canary-password@example.test/items/7",
        "//example.test/items/7",
    ),
    (
        "Content-Location",
        "/items/7?SIG=canary-sig&view=full",
        "/items/7?SIG=[redacted]&view=full",
    ),
    (
        "Content-Location",
        "/items?page=2;sig=canary-semicolon",
        "/items?page=2;sig=[redacted]",
    ),
    (
        "Content-Location",
        "/items?%74oken=canary-encoded&page=2",
        "/items?%74oken=[redacted]&page=2",
    ),
    (
        "Link",
        r#"<https://example.test/items?page=3&X-Amz-Signature=canary-link>; rel="next", </items?page=9>; rel="last""#,
        r#"<https://example.test/items?page=3&X-Amz-Signature=[redacted]>; rel="next", </items?page=9>; rel="last""#,
    ),
    (
        "Link",
        r#"</items?page=2>; rel=next; anchor="https://person:canary@example.test/x?token=canary-anchor#f""#,
        r#"</items?page=2>; rel=next; anchor="https://example.test/x?token=[redacted]""#,
    ),
    (
        "Link",
        "</items?token=canary-open",
        "</items?token=[redacted]",
    ),
    (
        "Link",
        r#"</x>; anchor="/items?page=2;token=canary-quoted", </y>"#,
        r#"</x>; anchor="/items?page=2;token=[redacted]", </y>"#,
    ),
    (
        "Link",
        "</x>; anchor=/items?page=2;token=canary-unquoted",
        "</x>; anchor=/items?page=2;token=[redacted]",
    ),
    (
        "Link",
        r#"</x>; anchor="/y?a=\"&token=canary-escaped""#,
        r#"</x>; anchor="/y?a=\"&token=[redacted]""#,
    ),
    (
        "Link",
        r#"</a?page=1>; title="x; <y>, z"; rel=next"#,
        r#"</a?page=1>; title="x; <y>, z"; rel=next"#,
    ),
    (
        "Refresh",
        "0; url=/login?token=canary-refresh",
        "0; url=/login?token=[redacted]",
    ),
    (
        "Refresh",
        "5 ; URL = '/x?token=canary-spaced'",
        "5 ; URL = '/x?token=[redacted]'",
    ),
    ("Refresh", "0; /x?code=canary-bare", "0; /x?code=[redacted]"),
    ("Refresh", "30", "30"),
    (
        "Refresh",
        "0, url=/x?token=canary-comma",
        "0, url=/x?token=[redacted]",
    ),
    (
        "Refresh",
        "/x?token=canary-unlabelled#canary-fragment",
        "/x?token=[redacted]",
    ),
    ("X-Quota", "first", "first"),
    ("X-Quota", "second", "second"),
    ("X-Request-Id", "request-7", "request-7"),
];

#[tokio::test]
async fn sensitive_response_header_values_never_reach_the_result() {
    let sent = SENSITIVE_HEADER_CASES
        .iter()
        .map(|(name, value, _retained)| (*name, *value))
        .collect::<Vec<_>>();

    let retained = retained_headers(&sent, AuthenticationConfiguration::None, None).await;

    let expected = SENSITIVE_HEADER_CASES
        .iter()
        .map(|(name, _value, retained)| (name.to_ascii_lowercase(), (*retained).to_string()))
        .collect::<Vec<_>>();
    assert_eq!(retained, expected);
}

#[tokio::test]
async fn echoed_credentials_never_reach_the_result() {
    // A service may repeat what Coral sent it, under the same names, which no
    // fixed list of sensitive names covers.
    let member = AuthenticationConfiguration::QueryCredentials {
        parameters: [("member".to_string(), field("api_token"))]
            .into_iter()
            .collect(),
    };
    let token = values(&[("api_token", "token-canary")]);
    for (authentication, sent, expected) in [
        (
            member,
            ("Link", "</boards?page=2&member=token-canary>; rel=next"),
            "</boards?page=2&member=[redacted]>; rel=next",
        ),
        (
            header_credentials(&[("x-trello-member", field_header("api_token", ""))]),
            ("X-Trello-Member", "token-canary"),
            "[redacted]",
        ),
    ] {
        let retained = retained_headers(&[sent], authentication, Some(&token)).await;
        assert_eq!(
            retained,
            [(sent.0.to_ascii_lowercase(), expected.to_string())]
        );
    }
}

/// An HTTP dispatch is one client span of the Invocation that sends it, and
/// the upstream receives that span's `traceparent` and nothing else of the
/// trace context: not one a Program argument names, nor `baggage` or
/// `tracestate`. The span holds the route template, never the filled path,
/// and an error status marks only the span.
#[tokio::test]
async fn an_http_dispatch_carries_only_its_client_span_trace_context() {
    use coral_telemetry::testing::attribute;
    use opentelemetry::trace::{SpanKind, Status};
    use tracing::Instrument as _;

    let capture = capture();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;
    let prepared = ready(&format!(
        r#"
openapi: 3.1.0
info: {{title: Trace context, version: "1"}}
servers: [{{url: {}}}]
paths:
  /items/{{id}}:
    get:
      operationId: read
      parameters:
        - {{name: id, in: path, required: true, schema: {{type: string}}}}
        - {{name: traceparent, in: header, schema: {{type: string}}}}
        - {{name: baggage, in: header, schema: {{type: string}}}}
      responses:
        "200": {{description: done}}
"#,
        server.uri()
    ));
    let invocation = tracing::info_span!("coral.invocation");
    let trace = {
        use opentelemetry::trace::TraceContextExt as _;
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;
        invocation.context().span().span_context().trace_id()
    };
    let outcome = send(plan(
        &prepared,
        &serde_json::json!({
            "id": "private-42",
            "traceparent": "00-ffffffffffffffffffffffffffffffff-ffffffffffffffff-01",
            "baggage": "user=private",
        }),
    ))
    .instrument(invocation.clone())
    .await
    .expect("a complete response");
    assert_eq!(outcome.status(), 404);
    drop(invocation);

    let spans = capture.finished();
    let parent = spans
        .iter()
        .find(|span| span.span_context.trace_id() == trace && span.name == "coral.invocation")
        .expect("Invocation span");
    let client = spans
        .iter()
        .find(|span| span.span_context.trace_id() == trace && span.span_kind == SpanKind::Client)
        .expect("HTTP client span");
    assert_eq!(client.name, "GET /items/{id}");
    assert_eq!(client.parent_span_id, parent.span_context.span_id());
    for (key, value) in [
        ("http.request.method", "GET"),
        ("url.scheme", "http"),
        ("server.address", "127.0.0.1"),
        ("url.template", "/items/{id}"),
        ("http.response.status_code", "404"),
        ("error.type", "404"),
    ] {
        assert_eq!(attribute(client, key).as_deref(), Some(value), "{key}");
    }
    assert!(attribute(client, "server.port").is_some());
    assert!(
        client
            .attributes
            .iter()
            .all(|kv| !kv.value.to_string().contains("private")),
        "no filled path or argument reaches the span"
    );
    assert!(matches!(client.status, Status::Error { .. }));

    let requests = server.received_requests().await.expect("requests");
    let [request] = requests.as_slice() else {
        panic!("one request: {requests:?}");
    };
    let headers = &request.headers;
    assert_eq!(
        headers
            .get_all("traceparent")
            .iter()
            .map(|value| value.to_str().expect("ASCII").to_string())
            .collect::<Vec<_>>(),
        vec![format!(
            "00-{}-{}-01",
            client.span_context.trace_id(),
            client.span_context.span_id()
        )]
    );
    assert!(headers.get("tracestate").is_none());
    assert!(headers.get("baggage").is_none());
}

/// The process-wide span capture: a subscriber set for one test thread
/// misses callsites another test's thread registered first.
fn capture() -> &'static coral_telemetry::testing::SpanCapture {
    static CAPTURE: std::sync::OnceLock<coral_telemetry::testing::SpanCapture> =
        std::sync::OnceLock::new();
    CAPTURE.get_or_init(|| {
        let capture = coral_telemetry::testing::SpanCapture::default();
        tracing::subscriber::set_global_default(capture.subscriber())
            .expect("install the span capture");
        capture
    })
}

/// An HTTP dispatch records one exchange by how it ended: a complete
/// response, whatever its status, with its request and response body
/// sizes, or a failure without a response.
#[tokio::test]
async fn an_http_dispatch_records_its_exchange() {
    // Installed before the scoped capture, so no thread keeps none.
    capture();
    let closed = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("address");
    let server = MockServer::start().await;
    let response = br#"{"ok": false}"#;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_raw(response, "application/json"))
        .mount(&server)
        .await;
    let response_size = f64::from(u32::try_from(response.len()).expect("small"));
    for (uri, outcome, response_bytes) in [
        (server.uri(), "response", Some(response_size)),
        (format!("http://{closed}"), "failed", None),
    ] {
        let metrics = coral_telemetry::testing::MetricCapture::default();
        let _metrics = tracing::subscriber::set_default(metrics.subscriber());
        let prepared = ready(&format!(
            r#"
openapi: 3.1.0
info: {{title: Exchange, version: "1"}}
servers: [{{url: {uri}}}]
paths:
  /items:
    post:
      operationId: create
      requestBody:
        required: true
        content: {{application/json: {{schema: {{type: object}}}}}}
      responses:
        "200": {{description: done}}
"#
        ));
        let dispatch = plan(
            &prepared,
            &serde_json::json!({"body": {"mediaType": "application/json", "data": {"name": "x"}}}),
        );
        let sent = send(dispatch).await;
        assert_eq!(sent.is_ok(), outcome == "response", "{uri}");
        let durations = metrics.named("coral.upstream.duration");
        let [duration] = durations.as_slice() else {
            panic!("{uri}: one exchange: {durations:?}");
        };
        assert_eq!(
            duration.attributes.get("outcome").map(String::as_str),
            Some(outcome),
            "{uri}"
        );
        assert_eq!(
            duration
                .attributes
                .get("provider.protocol")
                .map(String::as_str),
            Some("http")
        );
        let sizes = |name: &str| {
            metrics
                .named(name)
                .into_iter()
                .map(|point| point.value)
                .collect::<Vec<_>>()
        };
        // The body the server received; the failed dispatch planned the same.
        let received = server.received_requests().await.expect("requests");
        let [request] = received.as_slice() else {
            panic!("one request reached the server");
        };
        assert_eq!(
            sizes("coral.upstream.request.size"),
            [f64::from(u32::try_from(request.body.len()).expect("small"))],
            "{uri}"
        );
        assert_eq!(
            sizes("coral.upstream.response.size"),
            response_bytes.into_iter().collect::<Vec<_>>(),
            "{uri}"
        );
    }
}
