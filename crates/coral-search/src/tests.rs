mod builtin;
mod describe;
mod describe_operations;
mod index;
mod retrieval;
mod storage;

use coral_outbound::DestinationPolicy;
use coral_test_support::{change, trace};
use std::fmt::Write as _;

use coral_catalog::{OpenApiDocument, OperationHints, OperationKey};
use coral_functions::test_fixtures_repository as function_faults;
use coral_functions::{
    SavedFunctionChangeContext, SavedFunctionDescription, SavedFunctionModule, SavedFunctionPath,
    SavedFunctionSave, SavedFunctionSource,
};
use coral_policy::{OperationAccessState, OperationPolicyModule, OperationPolicyTarget};
use coral_provider::access::{CredentialRoute, ProviderNamespace};
use coral_provider::test_fixtures_repository as provider_faults;
use coral_provider::{
    ManagementRequestId, McpDiscoverySnapshot, ProviderModule, ProviderPublicationOutcome,
    ProviderRegenerationRequest, WorkspaceProviderPublicationRequest,
};
use coral_test_support::TestDatabase;
use coral_workspace::AuthenticatedPrincipal;
use coral_workspace::{
    AuthenticatedUser, ChangeContext, OrganizationRole, UserId, WorkspaceDisplayName, WorkspaceId,
    WorkspaceModule,
};
use wiremock::MockServer;

use crate::hit::CUT_MARKER;
use crate::signature::SIGNATURE_LINE_MAX_BYTES;
use crate::snippet::SNIPPET_MAX_BYTES;
use crate::{
    ApprovalRequirement, AuthoringDescription, CallPath, EntityKind, Error, SearchCursor,
    SearchLimit, SearchQuery, SearchRequest, UniversalSearchModule,
};

const ACCOUNT_LABEL_CANARY: &str = "private-account-label-canary";
const RAW_SCHEMA_CANARY: &str = "private-raw-schema-canary";
const EXECUTION_BINDING_CANARY: &str = "private-execution-binding-canary";
const SAVED_SOURCE_CANARY: &str = "private-saved-source-canary";

/// One User's Workspace with the tracer Operation and the tracer Saved Function.
struct TracerScope {
    upstream: MockServer,
    user: AuthenticatedUser,
    principal: AuthenticatedPrincipal,
    workspace_id: WorkspaceId,
    published: PublishedTracer,
    saved: coral_functions::SavedFunction,
}

impl TracerScope {
    const OPERATION: &str = "coral.providers.weather.getForecast";
    const FUNCTION: &str = "coral.functions.internal.formatForecast";

    async fn new(fixture: &TestModules) -> Self {
        let upstream = MockServer::start().await;
        let user = organization_user(fixture.search.database.clone(), "user:search-tracer").await;
        let principal = AuthenticatedPrincipal::user(user.clone());
        let workspace_id = fixture.create_workspace(&user).await;
        let published = fixture
            .publish_tracer(&user, &workspace_id, &upstream.uri())
            .await;
        let saved = fixture
            .save_tracer(&principal, &workspace_id, "Format one weather forecast", 40)
            .await;
        fixture.converge().await;
        Self {
            upstream,
            user,
            principal,
            workspace_id,
            published,
            saved,
        }
    }

    async fn describe(
        &self,
        fixture: &TestModules,
        path: &str,
    ) -> Result<crate::ExactAuthoringDescription, Error> {
        fixture
            .search
            .describe(
                &self.principal,
                &self.workspace_id,
                &CallPath::parse(path).expect("Call Path"),
                &[],
            )
            .await
    }

    async fn query(&self, fixture: &TestModules, query: &str) -> crate::SearchPage {
        fixture
            .search
            .search(
                &self.principal,
                &self.workspace_id,
                &SearchRequest::query(SearchQuery::parse(query).expect("query")),
            )
            .await
            .expect("search tracer Workspace")
    }
}

#[tokio::test]
async fn listing_orders_hits_by_call_path() {
    let fixture = TestModules::new().await;
    let scope = TracerScope::new(&fixture).await;
    let listed = fixture
        .search
        .search(
            &scope.principal,
            &scope.workspace_id,
            &SearchRequest::list(),
        )
        .await
        .expect("list visible authoring descriptions");
    let listed = listed.hits();
    assert_eq!(
        listed
            .iter()
            .map(|result| result.call_path().as_str())
            .collect::<Vec<_>>(),
        [TracerScope::FUNCTION, TracerScope::OPERATION],
        "an omitted query uses deterministic complete Call Path order"
    );
    assert_eq!(
        listed
            .iter()
            .map(crate::SearchHit::kind)
            .collect::<Vec<_>>(),
        [EntityKind::SavedFunction, EntityKind::Operation]
    );

    let saved_hit = listed.first().expect("Saved Function hit");
    assert_eq!(
        saved_hit.description_snippet(),
        "Format one weather forecast"
    );
    assert_eq!(
        saved_hit.signature_line(),
        "formatForecast(value: { temperatureC: number }): string",
        "the named input's members appear inline"
    );
    assert!(
        saved_hit.operation().is_none(),
        "a Saved Function hit has no Provider display name and no hints"
    );

    let operation_hit = listed.get(1).expect("Operation hit");
    assert_eq!(
        operation_hit.signature_line(),
        "getForecast(): Promise<{ data: {…} }>"
    );
    assert_eq!(
        operation_hit.description_snippet(),
        "Fetch the current conditions for one forecast location."
    );
    assert_eq!(
        (
            operation_hit.guidance_snippet(),
            operation_hit.matched_tags()
        ),
        (None, &[][..]),
        "a listing matched no term"
    );
    let operation = operation_hit.operation().expect("Operation facts");
    assert_eq!(operation.title(), Some("Get one weather forecast"));
    assert!(operation.deprecated());
    assert_eq!(operation.provider_display_name(), "Weather Tracer");
    assert_eq!(operation.hints(), OperationHints::default());
    assert_eq!(
        operation.approval_requirement(),
        ApprovalRequirement::NotRequired
    );
}

#[tokio::test]
async fn saved_function_signature_lines_show_the_inferred_result_fields() {
    let fixture = TestModules::new().await;
    let user = organization_user(fixture.search.database.clone(), "user:saved-result").await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    fixture
        .save_source(
            &principal,
            &workspace_id,
            "github.pullRequestDetails",
            "Summarize one pull request",
            r#"export default async function pullRequestDetails(input: { repo: string; number: number }) {
    return { title: "t", merged_at: null as string | null, base: { ref: "main" }, linked: [input.repo] };
}"#,
            20,
        )
        .await;
    fixture.converge().await;

    let page = fixture
        .search
        .search(&principal, &workspace_id, &SearchRequest::list())
        .await
        .expect("list Saved Functions");

    let [hit] = page.hits() else {
        panic!("one Saved Function hit: {:?}", page.hits());
    };
    let line = hit.signature_line();
    for field in ["title:", "merged_at:", "base:", "linked:"] {
        assert!(
            line.contains(field),
            "the fields of a result the compiler infers appear: {line}"
        );
    }
}

#[tokio::test]
async fn operation_signature_lines_follow_the_published_declaration() {
    let fixture = TestModules::new().await;
    let user = organization_user(fixture.search.database.clone(), "user:signature-lines").await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    let document = r##"{
      "openapi":"3.1.0",
      "info":{"title":"Issues","version":"1"},
      "servers":[{"url":"https://example.test"}],
      "security":[],
      "components":{"schemas":{
        "Issue":{"type":"object","properties":{"id":{"type":"integer"},"title":{"type":"string"}}}
      }},
      "paths":{"/repos/{owner}/issues":{
        "get":{"operationId":"listIssues","parameters":[
          {"name":"owner","in":"path","required":true,"schema":{"type":"string"}},
          {"name":"state","in":"query","schema":{"type":"string","enum":["open","closed"]}}
        ],"responses":{
          "200":{"description":"ok","content":{"application/json":{"schema":{"type":"array","items":{"$ref":"#/components/schemas/Issue"}}}}},
          "404":{"description":"missing","content":{"application/json":{"schema":{"type":"object"}}}}
        }},
        "post":{"operationId":"createIssue","parameters":[
          {"name":"owner","in":"path","required":true,"schema":{"type":"string"}}
        ],"requestBody":{"required":true,"content":{"application/json":{"schema":{"type":"object","properties":{"title":{"type":"string"}},"required":["title"]}}}},
        "responses":{"201":{"description":"ok","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Issue"}}}}}}
      }}
    }"##;
    fixture
        .publish_openapi(&user, &workspace_id, document, "github", 20)
        .await;

    let page = fixture
        .search
        .search(&principal, &workspace_id, &SearchRequest::list())
        .await
        .expect("list Operations");

    assert_eq!(
        page.hits()
            .iter()
            .map(|hit| (hit.call_path().as_str(), hit.signature_line()))
            .collect::<Vec<_>>(),
        [
            (
                "coral.providers.github.createIssue",
                "createIssue(input: { body: PostReposOwnerIssuesBodyInput; owner: string }): Promise<{ data: Issue }>",
            ),
            (
                "coral.providers.github.listIssues",
                r#"listIssues(input: { owner: string; state?: "open" | "closed" }): Promise<{ data: Array<Issue> }>"#,
            ),
        ],
        "a flat input shows its members inline; a nested body shows its type by name"
    );
}

#[tokio::test]
async fn queries_match_safe_operation_fields_and_function_declarations() {
    let fixture = TestModules::new().await;
    let scope = TracerScope::new(&fixture).await;
    for query in [
        "weather tracer",
        "getForecast",
        "get_forecast",
        "get-forecast",
        "weather.getForecast",
        "temperatureC",
        "current conditions",
        "stable location identifier",
    ] {
        assert!(
            scope
                .query(&fixture, query)
                .await
                .hits()
                .iter()
                .any(|result| result.call_path().as_str() == TracerScope::OPERATION),
            "query {query:?} must return the Operation"
        );
    }
    for (query, guidance_snippet, matched_tags) in [
        (
            "stable location identifier",
            Some("Use a stable location identifier when one is available."),
            &[][..],
        ),
        ("forecast", None, &["forecast"][..]),
        ("current conditions", None, &[][..]),
    ] {
        let page = scope.query(&fixture, query).await;
        let hit = page
            .hits()
            .iter()
            .find(|hit| hit.call_path().as_str() == TracerScope::OPERATION)
            .expect("Operation hit");
        assert_eq!(
            (
                hit.guidance_snippet(),
                hit.matched_tags()
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
            ),
            (guidance_snippet, matched_tags.to_vec()),
            "query {query:?}: guidance and tags appear only where a term matched"
        );
    }
    let function_results = scope.query(&fixture, "formatForecast").await;
    assert_eq!(
        function_results
            .hits()
            .first()
            .map(|result| result.call_path().as_str()),
        Some(TracerScope::FUNCTION)
    );
    let declaration_results = scope.query(&fixture, "temperatureC").await;
    let saved_declaration_match = declaration_results
        .hits()
        .iter()
        .find(|result| result.kind() == EntityKind::SavedFunction)
        .expect("Saved Function declaration match");
    assert_eq!(
        saved_declaration_match.evidence().paths(),
        ["input.value.temperatureC"],
        "Saved Function evidence must use callable-rooted compiler-projected declaration fields"
    );
}

#[tokio::test]
async fn describe_returns_exact_descriptions_and_only_owned_source() {
    let fixture = TestModules::new().await;
    let scope = TracerScope::new(&fixture).await;
    let described_operation = scope
        .describe(&fixture, TracerScope::OPERATION)
        .await
        .expect("describe exact Operation");
    let AuthoringDescription::Operation(operation) = described_operation.authoring() else {
        panic!("the tracer path is an Operation");
    };
    assert_eq!(operation.call_path().as_str(), TracerScope::OPERATION);
    assert_eq!(operation.tags(), ["forecast", "weather"]);
    assert_eq!(
        operation.guidance(),
        Some("Use a stable location identifier when one is available.")
    );
    assert_eq!(
        operation.provider_call_root().as_str(),
        "coral.providers.weather"
    );
    assert_eq!(operation.provider_id(), &scope.published.provider_id);
    let declaration = operation.typescript_declaration();
    assert!(
        declaration.contains(
            "export default function (): Promise<HttpResult<{ \"temperatureC\": number }"
        )
    );
    assert!(declaration.ends_with(", 200, \"application/json\">>;"));
    assert!(described_operation.saved_function_detail().is_none());

    let described_function = scope
        .describe(&fixture, TracerScope::FUNCTION)
        .await
        .expect("describe exact Saved Function");
    let declaration = described_function.authoring().typescript_declaration();
    assert_eq!(
        declaration,
        scope.saved.declaration().as_str(),
        "describe returns the exact compiler-emitted self-contained declaration"
    );
    assert!(declaration.contains("interface Forecast"));
    assert!(!declaration.contains(SAVED_SOURCE_CANARY));
    let editable = described_function
        .saved_function_detail()
        .expect("owned Saved Function editable detail");
    assert!(editable.source().as_str().contains(SAVED_SOURCE_CANARY));
    assert_eq!(editable.diagnostics(), scope.saved.diagnostics());
    assert_eq!(
        editable.diagnostics_truncated(),
        scope.saved.diagnostics_truncated()
    );
}

#[tokio::test]
async fn search_and_describe_never_read_private_material_or_dispatch_upstream() {
    let fixture = TestModules::new().await;
    let scope = TracerScope::new(&fixture).await;
    for private_term in [
        ACCOUNT_LABEL_CANARY,
        RAW_SCHEMA_CANARY,
        EXECUTION_BINDING_CANARY,
        SAVED_SOURCE_CANARY,
    ] {
        assert!(
            scope.query(&fixture, private_term).await.hits().is_empty(),
            "search leaked {private_term}"
        );
    }
    for path in [TracerScope::OPERATION, TracerScope::FUNCTION] {
        scope
            .describe(&fixture, path)
            .await
            .expect("describe tracer");
    }
    assert!(
        scope
            .upstream
            .received_requests()
            .await
            .expect("inspect upstream requests")
            .is_empty(),
        "search and describe must never dispatch upstream"
    );
}

#[tokio::test]
async fn disabled_operation_leaves_search_and_describe() {
    let fixture = TestModules::new().await;
    let scope = TracerScope::new(&fixture).await;
    fixture
        .policies
        .set_access_state(
            &change(scope.user.clone(), 50, '5'),
            scope.published.policy_target.clone(),
            OperationAccessState::Disabled,
        )
        .await
        .expect("disable tracer Operation");
    let after_disable = fixture
        .search
        .search(
            &scope.principal,
            &scope.workspace_id,
            &SearchRequest::list(),
        )
        .await
        .expect("list after policy change");
    assert_eq!(
        after_disable
            .hits()
            .iter()
            .map(|result| result.call_path().as_str())
            .collect::<Vec<_>>(),
        [TracerScope::FUNCTION]
    );
    assert!(matches!(
        scope.describe(&fixture, TracerScope::OPERATION).await,
        Err(Error::AuthoringDescriptionNotFound)
    ));
}

#[tokio::test]
async fn function_replacement_and_corrupt_relevance_keep_exact_descriptions() {
    let fixture = TestModules::new().await;
    let scope = TracerScope::new(&fixture).await;
    let replaced = fixture
        .save_tracer(
            &scope.principal,
            &scope.workspace_id,
            "Format the current weather forecast",
            60,
        )
        .await;
    let described_replacement = scope
        .describe(&fixture, TracerScope::FUNCTION)
        .await
        .expect("describe replacement");
    assert_eq!(
        described_replacement.authoring().description(),
        "Format the current weather forecast"
    );
    assert_eq!(
        described_replacement.authoring().typescript_declaration(),
        replaced.declaration().as_str()
    );

    function_faults::corrupt_declaration_fields(
        &fixture.search.database,
        &scope.workspace_id,
        replaced.id(),
    )
    .await;
    // The Saved Function matched this declaration field before the fault.
    assert!(
        scope
            .query(&fixture, "temperatureC")
            .await
            .hits()
            .iter()
            .all(|item| item.kind() == EntityKind::Operation),
        "a corrupt Saved Function declaration lane matches nothing"
    );
    let listing_with_corrupt_relevance = fixture
        .search
        .search(
            &scope.principal,
            &scope.workspace_id,
            &SearchRequest::list(),
        )
        .await
        .expect("listing must not consult relevance material");
    assert!(
        listing_with_corrupt_relevance
            .hits()
            .iter()
            .any(|item| item.call_path().as_str() == TracerScope::FUNCTION)
    );
    scope
        .describe(&fixture, TracerScope::FUNCTION)
        .await
        .expect("exact description must not decode relevance material");
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one fixture specifies both independent ranking lanes and public evidence"
)]
async fn relevance_combines_metadata_and_schema_lanes_before_call_path_ties() {
    let fixture = TestModules::new().await;
    let user = organization_user(fixture.search.database.clone(), "user:search-ranking").await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    let document = r#"{
      "openapi": "3.1.0",
      "info": {"title": "Ranking Provider", "version": "1"},
      "servers": [{"url": "https://example.test"}],
      "security": [],
      "paths": {
        "/alpha": {
          "get": {
            "operationId": "alphaData",
            "summary": "Retrieve a report",
            "parameters": [{
              "name": "weather",
              "in": "query",
              "schema": {"type": "string", "description": "Weather station"}
            }],
            "responses": {"200": {"description": "Report"}}
          }
        },
        "/zeta": {
          "get": {
            "operationId": "zetaReport",
            "summary": "Weather forecast",
            "responses": {"200": {"description": "Forecast"}}
          }
        }
      }
    }"#;
    let setup = fixture
        .providers
        .prepare_openapi_fixture(
            &change(user.clone(), 20, '2'),
            &workspace_id,
            OpenApiDocument::parse(document).expect("OpenAPI document"),
        )
        .await
        .expect("create Provider Setup");
    fixture
        .providers
        .publish_workspace_provider(
            &change(user, 30, '3'),
            WorkspaceProviderPublicationRequest::new(
                ManagementRequestId::parse("61000000-0000-4000-8000-000000000001")
                    .expect("request id"),
                workspace_id.clone(),
                setup.id().clone(),
                ProviderNamespace::parse("ranking").expect("namespace"),
                CredentialRoute::None,
            ),
        )
        .await
        .expect("publish ranking Provider");

    fixture.converge().await;
    let results = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("weather").expect("query")),
        )
        .await
        .expect("rank search results");

    assert_eq!(
        results
            .hits()
            .iter()
            .map(|result| result.call_path().as_str())
            .collect::<Vec<_>>(),
        [
            "coral.providers.ranking.zetaReport",
            "coral.providers.ranking.alphaData",
        ],
        "a strong metadata match ranks ahead of a schema-field-only match"
    );

    let call_path_results = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("alphaData").expect("query")),
        )
        .await
        .expect("search complete Call Path");
    assert_eq!(
        call_path_results
            .hits()
            .first()
            .expect("Call Path result")
            .evidence()
            .fields(),
        ["call_path"],
        "Operation Keys and relative paths must not masquerade as public metadata concepts"
    );

    let schema_description_results = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("station").expect("query")),
        )
        .await
        .expect("search authoritative schema descriptions");
    assert_eq!(
        schema_description_results
            .hits()
            .iter()
            .map(|result| result.call_path().as_str())
            .collect::<Vec<_>>(),
        ["coral.providers.ranking.alphaData"],
        "schema-field retrieval derives from authoritative schemas rather than generated TypeScript text"
    );
    assert_eq!(
        results
            .hits()
            .first()
            .expect("metadata result")
            .evidence()
            .fields(),
        ["description", "title"],
    );
    assert!(
        schema_description_results
            .hits()
            .first()
            .expect("schema result")
            .evidence()
            .paths()
            .contains(&"input.weather".to_string())
    );
}

#[tokio::test]
#[expect(
    clippy::indexing_slicing,
    reason = "the schema-graph fixture asserts one known single-result page"
)]
async fn openapi_declaration_paths_follow_references_unions_arrays_and_recursion() {
    let fixture = TestModules::new().await;
    let user =
        organization_user(fixture.search.database.clone(), "user:openapi-search-paths").await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    let document = r##"{
      "openapi": "3.1.0",
      "info": {"title": "Graph Search", "version": "1"},
      "servers": [{"url": "https://example.test"}],
      "security": [],
      "paths": {
        "/graph": {
          "get": {
            "operationId": "inspectGraph",
            "parameters": [{
              "name": "filters",
              "in": "query",
              "schema": {
                "type": "array",
                "items": {"$ref": "#/components/schemas/Filter"}
              }
            }],
            "responses": {
              "200": {
                "description": "ok",
                "content": {
                  "application/json": {
                    "schema": {
                      "type": "array",
                      "items": {
                        "oneOf": [
                          {"$ref": "#/components/schemas/Result"},
                          {
                            "type": "object",
                            "properties": {
                              "alternate": {
                                "type": "string",
                                "description": "Fallback branch marker"
                              }
                            }
                          }
                        ]
                      }
                    }
                  }
                }
              }
            }
          }
        }
      },
      "components": {
        "schemas": {
          "Filter": {
            "type": "object",
            "properties": {
              "label": {"type": "string", "description": "Recursive filter label"},
              "next": {"$ref": "#/components/schemas/Filter"}
            }
          },
          "Result": {
            "type": "object",
            "properties": {
              "status": {"type": "string", "enum": ["ready", "waiting"]}
            }
          }
        }
      }
    }"##;
    fixture
        .publish_openapi(&user, &workspace_id, document, "graph", 20)
        .await;

    fixture.converge().await;
    for (query, expected_path) in [
        ("recursive filter", "input.filters[].label"),
        ("waiting", "result.data[].status"),
        ("fallback branch", "result.data[].alternate"),
    ] {
        let page = fixture
            .search
            .search(
                &principal,
                &workspace_id,
                &SearchRequest::query(SearchQuery::parse(query).expect("query")),
            )
            .await
            .expect("search schema graph fields");
        assert_eq!(page.hits().len(), 1, "query {query:?}");
        assert!(
            page.hits()[0]
                .evidence()
                .paths()
                .iter()
                .any(|path| path == expected_path),
            "query {query:?} did not expose {expected_path:?}: {:?}",
            page.hits()[0].evidence().paths(),
        );
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one compact corpus covers Provider projection path semantics"
)]
async fn provider_declaration_paths_match_the_authoritative_typescript_surface() {
    let fixture = TestModules::new().await;
    let user = organization_user(
        fixture.search.database.clone(),
        "user:provider-declaration-path-semantics",
    )
    .await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    let document = r#"{
      "openapi":"3.1.0",
      "info":{"title":"Projection Corpus","version":"1"},
      "servers":[{"url":"https://example.test"}],
      "security":[],
      "paths":{"/project":{"post":{
        "operationId":"projectSurface",
        "requestBody":{"content":{"application/json":{"schema":{
          "type":"object",
          "properties":{
            "visibleInput":{"type":"string","description":"visiblealpha"},
            "hiddenInput":{"type":"string","readOnly":true,"description":"hiddenalpha"}
          },
          "additionalProperties":false
        }}}},
        "responses":{"200":{
          "description":"ok",
          "headers":{"X-Hidden":{"schema":{"type":"string","description":"headergamma"}}},
          "content":{"application/json":{"schema":{
            "type":"object",
            "properties":{
              "visibleOutput":{"type":"string","description":"visiblebeta"},
              "hiddenOutput":{"type":"string","writeOnly":true,"description":"hiddenbeta"},
              "tuple":{"type":"array","prefixItems":[{"type":"object","properties":{"tupleMember":{"type":"string","description":"tuplegamma"}}}],"items":false}
            },
            "additionalProperties":false
          }}}
        }}
      }}}
    }"#;
    fixture
        .publish_openapi(&user, &workspace_id, document, "projection", 20)
        .await;
    fixture
        .publish_openapi(
            &user,
            &workspace_id,
            r#"{
              "openapi":"3.1.0",
              "info":{"title":"Argumentless","version":"1"},
              "servers":[{"url":"https://example.test"}],
              "security":[],
              "paths":{"/ping":{"get":{"operationId":"ping","summary":"Ping","responses":{"200":{"description":"ok","content":{"application/json":{"schema":{"type":"string"}}}}}}}}
            }"#,
            "argumentless",
            30,
        )
        .await;

    fixture.converge().await;
    for (query, expected_path) in [
        ("visiblealpha", "input.body.visibleInput"),
        ("visiblebeta", "result.data.visibleOutput"),
        ("tuplegamma", "result.data.tuple[].tupleMember"),
    ] {
        let page = fixture
            .search
            .search(
                &principal,
                &workspace_id,
                &SearchRequest::query(SearchQuery::parse(query).expect("query")),
            )
            .await
            .expect("search visible declaration field");
        let result = page.hits().first().expect("one projection result");
        assert!(
            result
                .evidence()
                .paths()
                .iter()
                .any(|path| path == expected_path),
            "query {query:?} did not expose {expected_path:?}: {:?}",
            result.evidence().paths(),
        );
    }
    let argumentless = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("input").expect("query")).with_provider(
                crate::ProviderCallRoot::parse("coral.providers.argumentless").expect("namespace"),
            ),
        )
        .await
        .expect("search argumentless Operation");
    assert!(
        argumentless.hits().is_empty(),
        "an omitted input parameter must not produce evidence"
    );

    for omitted in ["hiddenalpha", "hiddenbeta", "headergamma", "false"] {
        let page = fixture
            .search
            .search(
                &principal,
                &workspace_id,
                &SearchRequest::query(SearchQuery::parse(omitted).expect("query")),
            )
            .await
            .expect("search omitted declaration field");
        assert!(
            page.hits().is_empty(),
            "indexed omitted field {omitted:?}: {:?}",
            page.hits()
                .iter()
                .map(|item| item.evidence().paths())
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
#[expect(
    clippy::indexing_slicing,
    reason = "the committed ranking corpus asserts known nonempty result pages"
)]
async fn synthetic_ranking_corpus_covers_phrase_weights_coverage_ties_and_evidence() {
    let fixture = TestModules::new().await;
    let user = organization_user(
        fixture.search.database.clone(),
        "user:synthetic-ranking-corpus",
    )
    .await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    let document = r#"{
      "openapi":"3.1.0",
      "info":{"title":"Synthetic Corpus","version":"1"},
      "servers":[{"url":"https://example.test"}],
      "security":[],
      "paths":{
        "/cross":{"get":{"operationId":"opCross","summary":"Invoice records","parameters":[{"name":"archive","in":"query","schema":{"type":"string"}}],"responses":{"200":{"description":"ok"}}}},
        "/metadata":{"get":{"operationId":"opMetadata","summary":"Invoice archive","responses":{"200":{"description":"ok"}}}},
        "/declaration":{"get":{"operationId":"opDeclaration","summary":"General records","parameters":[{"name":"invoice","in":"query","schema":{"type":"string"}},{"name":"archive","in":"query","schema":{"type":"string"}}],"responses":{"200":{"description":"ok"}}}},
        "/coverage":{"get":{"operationId":"opCoverage","summary":"Ledger recent","responses":{"200":{"description":"ok"}}}},
        "/partial":{"get":{"operationId":"opPartial","summary":"Ledger entries","responses":{"200":{"description":"ok"}}}},
        "/tie-a":{"get":{"operationId":"alphaTie","summary":"Quartz lookup","responses":{"200":{"description":"ok"}}}},
        "/tie-b":{"get":{"operationId":"gammaTie","summary":"Quartz lookup","responses":{"200":{"description":"ok"}}}}
      }
    }"#;
    fixture
        .publish_openapi(&user, &workspace_id, document, "corpus", 20)
        .await;

    fixture.converge().await;
    let fusion = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("invoice archive").expect("query")),
        )
        .await
        .expect("rank fused corpus");
    assert_eq!(
        fusion
            .hits()
            .iter()
            .map(|item| item.call_path().as_str())
            .collect::<Vec<_>>(),
        [
            "coral.providers.corpus.opMetadata",
            "coral.providers.corpus.opCross",
            "coral.providers.corpus.opDeclaration",
        ],
        "a whole-query phrase ranks first, then a title match over declaration paths alone"
    );
    let cross = &fusion.hits()[1];
    assert!(cross.evidence().fields().contains(&"title".to_string()));
    assert!(
        cross
            .evidence()
            .paths()
            .contains(&"input.archive".to_string())
    );

    let coverage = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("ledger recent").expect("query")),
        )
        .await
        .expect("rank term coverage");
    assert_eq!(
        coverage.hits()[0].call_path().as_str(),
        "coral.providers.corpus.opCoverage"
    );

    let ties = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("quartz").expect("query")),
        )
        .await
        .expect("rank deterministic ties");
    assert_eq!(
        ties.hits()
            .iter()
            .map(|item| item.call_path().as_str())
            .collect::<Vec<_>>(),
        [
            "coral.providers.corpus.alphaTie",
            "coral.providers.corpus.gammaTie",
        ],
        "rows of equal text and length tie and follow Call Path order"
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one phrase corpus and one table cover the phrase rule of each field"
)]
async fn phrase_points_match_whole_words_and_favor_a_whole_field() {
    let fixture = TestModules::new().await;
    let user = organization_user(fixture.search.database.clone(), "user:phrase-points").await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    // The two operations an agent wants carry long text, as their large
    // schemas do in the GitHub catalog, so their length lowers their score
    // below the short decoys'.
    let long = "Drafts are available in public repositories with paid plans. ".repeat(12);
    let document = r#"{
      "openapi":"3.1.0",
      "info":{"title":"Pull Requests","version":"1"},
      "servers":[{"url":"https://example.test"}],
      "security":[],
      "paths":{
        "/bypass":{"get":{"operationId":"interactions/getBypassList","summary":"Get the creation cap bypass list","description":"Users on this list can create pull requests regardless of the cap.","responses":{"200":{"description":"ok"}}}},
        "/pulls":{"post":{"operationId":"pulls/create","summary":"Create a pull request","description":"LONG","responses":{"200":{"description":"ok"}}}},
        "/pulls/{number}":{"get":{"operationId":"pulls/get","summary":"Get a pull request","description":"LONG","parameters":[{"name":"number","in":"path","required":true,"schema":{"type":"integer"}}],"responses":{"200":{"description":"ok"}}}},
        "/pulls/{number}/reviews/{id}":{"get":{"operationId":"pulls/getReview","summary":"Get a review for a pull request","parameters":[{"name":"number","in":"path","required":true,"schema":{"type":"integer"}},{"name":"id","in":"path","required":true,"schema":{"type":"integer"}}],"responses":{"200":{"description":"ok"}}}},
        "/pulls/{number}/reviews":{"post":{"operationId":"pulls/createReview","summary":"Create a review for a pull request","parameters":[{"name":"number","in":"path","required":true,"schema":{"type":"integer"}}],"responses":{"200":{"description":"ok"}}}},
        "/stacks":{"post":{"operationId":"pullRequestStacks/create","summary":"Create a pull request stack","responses":{"200":{"description":"ok"}}}},
        "/checks/rerun":{"get":{"operationId":"checks/getRerun","summary":"Tracer rerun details","description":"LONG","responses":{"200":{"description":"ok"}}}},
        "/runs":{"get":{"operationId":"runs/list","summary":"Get every rerun now","responses":{"200":{"description":"ok"}}}},
        "/drafts":{"post":{"operationId":"drafts/open","summary":"Open a draft review for a team","description":"LONG","responses":{"200":{"description":"ok"}}}},
        "/drafts/all":{"get":{"operationId":"drafts/list","summary":"Open draft and review drafts","responses":{"200":{"description":"ok"}}}},
        "/pulls/merge":{"put":{"operationId":"pulls/merge","summary":"Merge a pull request now","description":"LONG","responses":{"200":{"description":"ok"}}}},
        "/merges":{"get":{"operationId":"merges/list","summary":"MERGE PULL REQUESTS","responses":{"200":{"description":"ok"}}}},
        "/merges/tool":{"get":{"operationId":"merges/tool","summary":"MERGE PULLREQUEST NOW","responses":{"200":{"description":"ok"}}}},
        "/hooks/then":{"post":{"operationId":"hooks/then","summary":"Chain a hook","responses":{"200":{"description":"ok"}}}},
        "/hooks/then/review":{"post":{"operationId":"hooks/thenReview","summary":"Chain a hook review","responses":{"200":{"description":"ok"}}}},
        "/widgets":{"get":{"operationId":"widgets/lookup","summary":"Look up a widget","parameters":[{"name":"zorble_quint","in":"query","schema":{"type":"string"}}],"responses":{"200":{"description":"ok"}}}},
        "/gadgets":{"get":{"operationId":"gadgets/list","summary":"Quint values for each zorble","responses":{"200":{"description":"ok"}}}}
      }
    }"#
    .replace("LONG", long.trim_end());
    fixture
        .publish_openapi(&user, &workspace_id, &document, "github", 20)
        .await;

    fixture.converge().await;
    for (query, expected_first, reason) in [
        (
            "create pull request",
            "pulls.create",
            "a plural in a description is not the phrase, and a title equal to the query \
             but for `a` outranks a title that only contains it",
        ),
        (
            "create a pull request",
            "pulls.create",
            "a whole title outranks a title that contains it",
        ),
        (
            "pulls.create",
            "pulls.create",
            "a whole name outranks the camelCase names it begins",
        ),
        (
            "hooks.then",
            "hooks._then",
            "`_` separates words, so the escaped name `_then` is the whole name",
        ),
        (
            "create a pull request stack",
            "pullRequestStacks.create",
            "an exact title still ranks first",
        ),
        (
            "get rerun",
            "checks.getRerun",
            "a camelCase edge also separates the phrase's words, so the name \
             `getRerun` holds `get rerun`",
        ),
        (
            "open draft review",
            "drafts.open",
            "a field holds the phrase with a/an/the between its words",
        ),
        (
            "merge pull request",
            "pulls.merge",
            "a capital is a camelCase edge only after a lowercase letter or a digit, \
             so neither `REQUESTS` nor `PULLREQUEST` holds the phrase",
        ),
        (
            "zorble quint",
            "gadgets.list",
            "schema text adds no phrase points, so a parameter that holds the phrase \
             ranks below a title that holds its words in another order",
        ),
    ] {
        let page = fixture
            .search
            .search(
                &principal,
                &workspace_id,
                &SearchRequest::query(SearchQuery::parse(query).expect("query")),
            )
            .await
            .expect("rank phrase corpus");
        let order = page
            .hits()
            .iter()
            .map(|item| item.call_path().as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            order.first().copied(),
            Some(format!("coral.providers.github.{expected_first}").as_str()),
            "query {query:?}: {reason}; order {order:?}"
        );
    }
}

#[tokio::test]
async fn mcp_publication_prepares_schema_field_paths_for_search() {
    let fixture = TestModules::new().await;
    let user = organization_user(fixture.search.database.clone(), "user:mcp-search-index").await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    let snapshot = r#"{
      "protocolVersion": "2025-11-25",
      "serverInfo": {"name": "records", "title": "Records", "version": "1"},
      "server": {"transport": "streamable_http", "url": "https://example.test/mcp"},
      "capabilities": {"tools": {}},
      "tools": [{
        "name": "lookup_records",
        "title": "Record lookup",
        "description": "Find records in one tenant.",
        "inputSchema": {
          "type": "object",
          "properties": {
            "tenant": {"type": "string", "description": "Tenant partition identifier"}
          }
        },
        "outputSchema": {
          "type": "object",
          "properties": {
            "status": {"type": "string", "enum": ["active", "archived"]}
          }
        },
        "annotations": {"readOnlyHint": true}
      }]
    }"#;
    let setup = fixture
        .providers
        .prepare_mcp_fixture(
            &change(user.clone(), 20, '2'),
            &workspace_id,
            McpDiscoverySnapshot::parse(snapshot).expect("MCP snapshot"),
        )
        .await
        .expect("create MCP Provider Setup");
    fixture
        .providers
        .publish_workspace_provider(
            &change(user, 30, '3'),
            WorkspaceProviderPublicationRequest::new(
                ManagementRequestId::parse("61000000-0000-4000-8000-000000000004")
                    .expect("request id"),
                workspace_id.clone(),
                setup.id().clone(),
                ProviderNamespace::parse("records").expect("namespace"),
                CredentialRoute::None,
            ),
        )
        .await
        .expect("publish MCP Provider");

    fixture.converge().await;
    for (query, expected_path) in [
        ("tenant partition", "input.tenant"),
        ("archived", "result.data.structuredContent.status"),
    ] {
        let page = fixture
            .search
            .search(
                &principal,
                &workspace_id,
                &SearchRequest::query(SearchQuery::parse(query).expect("query")),
            )
            .await
            .expect("search MCP schema fields");
        let result = page.hits().first().expect("MCP search result");
        assert_eq!(
            result.call_path().as_str(),
            "coral.providers.records.lookupRecords"
        );
        assert!(
            result
                .evidence()
                .paths()
                .iter()
                .any(|path| path == expected_path)
        );
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the fixture proves old and replacement search data switch at publication"
)]
async fn regeneration_atomically_replaces_the_active_search_generation() {
    let fixture = TestModules::new().await;
    let user = organization_user(fixture.search.database.clone(), "user:search-regeneration").await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    let published = fixture
        .publish_tracer(&user, &workspace_id, "https://example.test")
        .await;

    fixture.converge().await;
    let before = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("temperatureC").expect("query")),
        )
        .await
        .expect("search active Generation");
    assert_eq!(before.hits().len(), 1);

    let replacement = r#"{
      "openapi": "3.1.0",
      "info": {"title": "Weather Tracer v2", "version": "2"},
      "servers": [{"url": "https://example.test"}],
      "security": [],
      "paths": {
        "/forecast": {
          "get": {
            "operationId": "getForecast",
            "summary": "Get current humidity",
            "parameters": [{
              "name": "location",
              "in": "query",
              "schema": {"type": "string", "description": "Moisture station identifier"}
            }],
            "responses": {
              "200": {
                "description": "Humidity",
                "content": {
                  "application/json": {
                    "schema": {
                      "type": "object",
                      "properties": {"humidityPercent": {"type": "number"}},
                      "required": ["humidityPercent"]
                    }
                  }
                }
              }
            }
          }
        }
      }
    }"#;
    let setup = fixture
        .providers
        .prepare_openapi_fixture(
            &change(user.clone(), 50, '5'),
            &workspace_id,
            OpenApiDocument::parse(replacement).expect("replacement OpenAPI"),
        )
        .await
        .expect("create replacement Setup");
    fixture
        .providers
        .regenerate_provider(
            &change(user, 60, '6'),
            ProviderRegenerationRequest::new(
                ManagementRequestId::parse("61000000-0000-4000-8000-000000000002")
                    .expect("request id"),
                workspace_id.clone(),
                published.provider_id,
                setup.id().clone(),
                1,
            )
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .expect("activate replacement Generation");

    fixture.converge().await;
    let old = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("temperatureC").expect("query")),
        )
        .await
        .expect("search after activation");
    assert!(old.hits().is_empty());
    let current = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("moisture").expect("query")),
        )
        .await
        .expect("search replacement Generation");
    assert_eq!(current.hits().len(), 1);
    assert_eq!(
        current
            .hits()
            .first()
            .expect("current search result")
            .call_path()
            .as_str(),
        "coral.providers.weather.getForecast"
    );
}

#[tokio::test]
async fn oversized_declaration_regenerates_with_its_shallow_fields_searchable() {
    let fixture = TestModules::new().await;
    let user = organization_user(
        fixture.search.database.clone(),
        "user:search-oversized-declaration",
    )
    .await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    let published = fixture
        .publish_tracer(&user, &workspace_id, "https://example.test")
        .await;
    let bulk = (0..8_300)
        .map(|index| {
            (
                format!("f{index:04}"),
                serde_json::json!({"type": "boolean"}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let replacement = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Oversized Declaration", "version": "2"},
        "servers": [{"url": "https://example.test"}],
        "security": [],
        "paths": {"/forecast": {"get": {
            "operationId": "getForecast",
            "summary": "Replacement with more fields than search indexes",
            "responses": {"200": {
                "description": "ok",
                "content": {"application/json": {"schema": {
                    "type": "object",
                    "properties": {
                        "bulk": {"type": "object", "properties": bulk},
                        "humidityPercent": {"type": "number"},
                    },
                }}},
            }},
        }}},
    });
    fixture
        .regenerate_workspace_provider(
            &change(user, 60, '6'),
            &workspace_id,
            &published.provider_id,
            &replacement,
        )
        .await;

    fixture.converge().await;
    let current = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("humidityPercent").expect("query")),
        )
        .await
        .expect("search the oversized replacement");
    let [item] = current.hits() else {
        panic!("one oversized Operation match");
    };
    assert_eq!(
        item.call_path().as_str(),
        "coral.providers.weather.getForecast"
    );
    assert!(
        item.evidence()
            .paths()
            .contains(&"result.data.humidityPercent".to_owned())
    );
    let deep = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("f8299").expect("query")),
        )
        .await
        .expect("search a field past the index bound");
    assert!(
        deep.hits().is_empty(),
        "fields past the bound stay unindexed"
    );
}

#[tokio::test]
#[expect(
    clippy::indexing_slicing,
    reason = "the lazy-hydration fixture asserts one known result"
)]
#[expect(
    clippy::too_many_lines,
    reason = "one fixture covers lazy hydration, isolated corruption, and exact description"
)]
async fn relevance_retrieves_before_hydrating_complete_descriptions() {
    let fixture = TestModules::new().await;
    let user = organization_user(
        fixture.search.database.clone(),
        "user:search-lazy-hydration",
    )
    .await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    let document = r#"{
      "openapi":"3.1.0",
      "info":{"title":"Lazy Provider","version":"1"},
      "servers":[{"url":"https://example.test"}],
      "security":[],
      "paths":{
        "/healthy":{"get":{"operationId":"healthyLookup","summary":"Find sapphire records","responses":{"200":{"description":"ok"}}}},
        "/broken":{"get":{"operationId":"brokenLookup","summary":"Find vermilion records","responses":{"200":{"description":"ok"}}}}
      }
    }"#;
    let lazy = fixture
        .publish_openapi(&user, &workspace_id, document, "lazy", 20)
        .await;
    fixture.converge().await;
    provider_faults::corrupt_operation_tags(
        &fixture.search.database,
        &workspace_id,
        &lazy,
        "brokenLookup",
    )
    .await;
    fixture
        .publish_openapi(
            &user,
            &workspace_id,
            r#"{
              "openapi":"3.1.0",
              "info":{"title":"Survivor","version":"1"},
              "servers":[{"url":"https://example.test"}],
              "security":[],
              "paths":{"/records":{"get":{"operationId":"durableLookup","summary":"Durable records","responses":{"200":{"description":"ok"}}}}}
            }"#,
            "survivor",
            30,
        )
        .await;

    fixture.converge().await;
    let partial = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("records").expect("query")),
        )
        .await
        .expect("a healthy Provider survives authoritative corruption");
    let mut paths = partial
        .hits()
        .iter()
        .map(|item| item.call_path().as_str())
        .collect::<Vec<_>>();
    paths.sort_unstable();
    assert_eq!(
        paths,
        [
            "coral.providers.lazy.healthyLookup",
            "coral.providers.survivor.durableLookup",
        ],
        "a corrupt Operation leaves the page; its healthy sibling stays"
    );

    let healthy = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("sapphire").expect("query")),
        )
        .await
        .expect("an unranked corrupt description must not be loaded");
    assert_eq!(healthy.hits().len(), 1);
    assert_eq!(
        healthy.hits()[0].call_path().as_str(),
        "coral.providers.lazy.healthyLookup"
    );
    assert!(matches!(
        fixture
            .search
            .search(
                &principal,
                &workspace_id,
                &SearchRequest::query(SearchQuery::parse("vermilion").expect("query")),
            )
            .await,
        Err(Error::ServiceUnavailable)
    ));
    assert!(matches!(
        fixture
            .search
            .describe(
                &principal,
                &workspace_id,
                &CallPath::parse("coral.providers.lazy.brokenLookup").expect("Call Path"),
                &[],
            )
            .await,
        Err(Error::Provider(_))
    ));
}

#[tokio::test]
async fn oversized_complete_operation_description_is_unavailable_not_truncated() {
    let fixture = TestModules::new().await;
    let user = organization_user(
        fixture.search.database.clone(),
        "user:search-description-bound",
    )
    .await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    let properties = (0..3_000)
        .map(|index| {
            format!(
                "\"field_{index:04}_{}\":{{\"type\":\"string\"}}",
                "x".repeat(80)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let document = format!(
        r#"{{
          "openapi": "3.1.0",
          "info": {{"title": "Large Provider", "version": "1"}},
          "servers": [{{"url": "https://example.test"}}],
          "security": [],
          "paths": {{
            "/large": {{
              "get": {{
                "operationId": "getLarge",
                "responses": {{
                  "200": {{
                    "description": "Large response",
                    "content": {{
                      "application/json": {{
                        "schema": {{"type": "object", "properties": {{{properties}}}}}
                      }}
                    }}
                  }}
                }}
              }}
            }}
          }}
        }}"#
    );
    let setup = fixture
        .providers
        .prepare_openapi_fixture(
            &change(user.clone(), 20, '2'),
            &workspace_id,
            OpenApiDocument::parse(&document).expect("large OpenAPI document"),
        )
        .await
        .expect("create Provider Setup");
    fixture
        .providers
        .publish_workspace_provider(
            &change(user, 30, '3'),
            WorkspaceProviderPublicationRequest::new(
                ManagementRequestId::parse("61000000-0000-4000-8000-000000000003")
                    .expect("request id"),
                workspace_id.clone(),
                setup.id().clone(),
                ProviderNamespace::parse("large").expect("namespace"),
                CredentialRoute::None,
            ),
        )
        .await
        .expect("publish catalog with unavailable large Operation");

    let page = fixture
        .search
        .search(&principal, &workspace_id, &SearchRequest::list())
        .await
        .expect("list discovery");
    assert!(page.hits().is_empty());
    assert!(matches!(
        fixture
            .search
            .describe(
                &principal,
                &workspace_id,
                &CallPath::parse("coral.providers.large.getLarge").expect("Call Path"),
                &[],
            )
            .await,
        Err(Error::AuthoringDescriptionNotFound)
    ));
}

#[tokio::test]
async fn query_evidence_is_bounded() {
    let fixture = TestModules::new().await;
    let user = organization_user(
        fixture.search.database.clone(),
        "user:search-evidence-bound",
    )
    .await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    let properties = (0..128)
        .map(|index| format!("\"field{index:03}\":{{\"type\":\"string\"}}"))
        .collect::<Vec<_>>()
        .join(",");
    let document = format!(
        r#"{{
          "openapi": "3.1.0",
          "info": {{"title": "Evidence Provider", "version": "1"}},
          "servers": [{{"url": "https://example.test"}}],
          "security": [],
          "paths": {{
            "/evidence": {{
              "get": {{
                "operationId": "getEvidence",
                "responses": {{
                  "200": {{
                    "description": "Evidence",
                    "content": {{
                      "application/json": {{
                        "schema": {{"type": "object", "properties": {{{properties}}}}}
                      }}
                    }}
                  }}
                }}
              }}
            }}
          }}
        }}"#
    );
    let setup = fixture
        .providers
        .prepare_openapi_fixture(
            &change(user.clone(), 20, '2'),
            &workspace_id,
            OpenApiDocument::parse(&document).expect("OpenAPI document"),
        )
        .await
        .expect("create Provider Setup");
    fixture
        .providers
        .publish_workspace_provider(
            &change(user, 30, '3'),
            WorkspaceProviderPublicationRequest::new(
                ManagementRequestId::parse("61000000-0000-4000-8000-000000000005")
                    .expect("request id"),
                workspace_id.clone(),
                setup.id().clone(),
                ProviderNamespace::parse("evidence").expect("namespace"),
                CredentialRoute::None,
            ),
        )
        .await
        .expect("publish evidence Provider");

    fixture.converge().await;
    let page = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("string").expect("query")),
        )
        .await
        .expect("search broad schema term");
    let result = page.hits().first().expect("complete result");
    assert_eq!(result.evidence().paths().len(), 32);
}

#[tokio::test]
async fn ordinary_saved_function_search_never_loads_source() {
    let fixture = TestModules::new().await;
    let user = organization_user(
        fixture.search.database.clone(),
        "user:search-does-not-load-source",
    )
    .await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    let saved = fixture
        .save_tracer(&principal, &workspace_id, "Source-free metadata", 20)
        .await;
    function_faults::corrupt_source(&fixture.search.database, &workspace_id, saved.id()).await;

    fixture.converge().await;
    let page = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("source-free").expect("query")),
        )
        .await
        .expect("ordinary search must not load source");
    assert_eq!(page.hits().len(), 1);
    assert!(matches!(
        fixture
            .search
            .describe(
                &principal,
                &workspace_id,
                &CallPath::parse("coral.functions.internal.formatForecast").expect("Call Path"),
                &[],
            )
            .await,
        Err(Error::Functions(coral_functions::Error::CorruptData(_)))
    ));
}

#[tokio::test]
#[expect(
    clippy::indexing_slicing,
    reason = "the projection-bound fixture asserts one known metadata result"
)]
async fn saved_function_projection_bounds_do_not_become_authoring_limits() {
    let fixture = TestModules::new().await;
    let user = organization_user(
        fixture.search.database.clone(),
        "user:saved-function-projection-bound",
    )
    .await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    let mut fields = String::new();
    for index in 0..4_200 {
        write!(fields, "a{index}:0;").expect("write field fixture");
    }
    let function = fixture
        .saved_functions
        .save(
            &SavedFunctionChangeContext::new(principal.clone(), 20, trace('2')),
            &workspace_id,
            SavedFunctionSave::new(
                SavedFunctionPath::parse("largeProjection").expect("path"),
                SavedFunctionDescription::parse("Large searchable metadata").expect("description"),
                SavedFunctionSource::parse(&format!(
                    "interface X{{{fields}}} export default function f(input:X):number{{return 1}}"
                ))
                .expect("bounded source"),
            ),
        )
        .await
        .expect("private projection bound must not reject valid authoring");
    assert!(matches!(
        function.declaration_field_projection(),
        coral_functions::SavedFunctionDeclarationFieldProjection::Failed(_)
    ));

    fixture.converge().await;
    let metadata = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("searchable metadata").expect("query")),
        )
        .await
        .expect("metadata search remains healthy");
    assert_eq!(metadata.hits().len(), 1);
    assert_eq!(
        metadata.hits()[0].call_path().as_str(),
        "coral.functions.largeProjection"
    );
    let declaration = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("a4199").expect("query")),
        )
        .await
        .expect("failed per-function declaration lane does not fail metadata search");
    assert!(declaration.hits().is_empty());
}

#[tokio::test]
async fn a_page_of_fifty_hits_fits_the_budget_with_bounded_text() {
    let fixture = TestModules::new().await;
    let user = organization_user(fixture.search.database.clone(), "user:search-hit-bounds").await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    for index in 0..50 {
        fixture
            .save_large_function(&principal, &workspace_id, index)
            .await;
    }
    fixture.converge().await;

    let page = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::query(SearchQuery::parse("paging").expect("query"))
                .with_limit(SearchLimit::parse(50).expect("maximum page limit")),
        )
        .await
        .expect("search large functions");

    assert_eq!(page.hits().len(), 50);
    let projected = serde_json::to_string(&crate::projection::search(&page)).expect("page JSON");
    assert!(projected.len() <= crate::SEARCH_PAGE_MAX_BYTES);
    assert!(
        !projected.contains("field1799"),
        "no hit carries the declaration"
    );
    for hit in page.hits() {
        assert!(
            hit.signature_line().len() <= SIGNATURE_LINE_MAX_BYTES
                && hit.signature_line().ends_with(CUT_MARKER)
        );
        assert!(
            hit.description_snippet().len() <= SNIPPET_MAX_BYTES
                && hit.description_snippet().starts_with(CUT_MARKER)
                && hit.description_snippet().contains("paging"),
            "the snippet opens around the late match: {}",
            hit.description_snippet()
        );
        assert!(
            !hit.evidence().fields().is_empty() || !hit.evidence().paths().is_empty(),
            "every hit carries evidence"
        );
    }
}

#[tokio::test]
async fn response_bytes_bound_pages_that_continue_without_repeats() {
    let fixture = TestModules::new().await;
    let user = organization_user(fixture.search.database.clone(), "user:search-byte-pages").await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    // Thirty-two evidence paths of about one KiB each make every hit large.
    let fields = (0..32)
        .map(|field| format!("pagedfield{field:02}{}: string;", "x".repeat(980)))
        .collect::<Vec<_>>()
        .join("\n");
    let mut expected = std::collections::BTreeSet::new();
    for index in 0..12 {
        fixture
            .saved_functions
            .save(
                &SavedFunctionChangeContext::new(principal.clone(), 100, trace('7')),
                &workspace_id,
                SavedFunctionSave::new(
                    SavedFunctionPath::parse(&format!("paged.function{index:02}"))
                        .expect("Saved Function path"),
                    SavedFunctionDescription::parse("Paged contract").expect("description"),
                    SavedFunctionSource::parse(&format!(
                        "interface Wide {{\n{fields}\n}}\nexport default function wide(input: Wide): number {{ return 1; }}"
                    ))
                    .expect("wide source"),
                ),
            )
            .await
            .expect("save wide function");
        expected.insert(format!("coral.functions.paged.function{index:02}"));
    }
    fixture.converge().await;

    let limit = SearchLimit::parse(50).expect("maximum page limit");
    let mut cursor = None;
    let mut observed = std::collections::BTreeSet::new();
    let mut page_count = 0;
    loop {
        page_count += 1;
        let mut request = SearchRequest::query(SearchQuery::parse("pagedfield").expect("query"))
            .with_limit(limit);
        if let Some(next) = cursor {
            request = request.with_cursor(next);
        }
        let page = fixture
            .search
            .search(&principal, &workspace_id, &request)
            .await
            .expect("read byte-bounded page");
        assert!(!page.hits().is_empty(), "every continuation must progress");
        for hit in page.hits() {
            assert!(
                observed.insert(hit.call_path().as_str().to_string()),
                "unchanged paging must not repeat a hit"
            );
        }
        if !page.has_more() {
            break;
        }
        cursor = page.next_cursor().cloned();
    }
    assert!(
        page_count > 1,
        "response bytes, not count, bound these pages"
    );
    assert_eq!(observed, expected);
}

#[tokio::test]
async fn provider_filter_selects_one_provider_and_keeps_saved_functions() {
    let fixture = TestModules::new().await;
    let user = organization_user(
        fixture.search.database.clone(),
        "user:search-provider-filter",
    )
    .await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    fixture
        .publish_tracer(&user, &workspace_id, "https://example.test")
        .await;
    let mut document = credential_document();
    *document.get_mut("security").unwrap() = serde_json::json!([]);
    fixture
        .publish_configured(
            &change(user.clone(), 40, '4'),
            &workspace_id,
            &document,
            "weatherAgain",
            CredentialRoute::None,
        )
        .await;
    fixture
        .save_tracer(&principal, &workspace_id, "Format a forecast", 50)
        .await;

    let filtered = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::list().with_provider(
                crate::ProviderCallRoot::parse("coral.providers.weather").expect("namespace"),
            ),
        )
        .await
        .expect("filter by stable Provider");
    assert_eq!(
        filtered
            .hits()
            .iter()
            .map(|item| item.call_path().as_str())
            .collect::<Vec<_>>(),
        [
            "coral.functions.internal.formatForecast",
            "coral.providers.weather.getForecast",
        ]
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one pagination scenario proves cursor binding and current-visibility reapplication"
)]
async fn pages_complete_current_results_with_query_bound_non_authorizing_cursors() {
    let fixture = TestModules::new().await;
    let user = organization_user(fixture.search.database.clone(), "user:search-pages").await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace_id = fixture.create_workspace(&user).await;
    let published = fixture
        .publish_tracer(&user, &workspace_id, "https://example.test")
        .await;
    fixture
        .save_tracer(&principal, &workspace_id, "Format a forecast", 40)
        .await;
    let one = SearchLimit::parse(1).expect("one-result page limit");

    let first = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::list().with_limit(one),
        )
        .await
        .expect("first page");
    assert_eq!(first.hits().len(), 1);
    assert!(first.has_more());
    let cursor = first.next_cursor().expect("next cursor").clone();

    let second = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::list()
                .with_limit(one)
                .with_cursor(cursor.clone()),
        )
        .await
        .expect("second page");
    assert_eq!(second.hits().len(), 1);
    assert_eq!(
        second
            .hits()
            .first()
            .expect("second-page result")
            .call_path()
            .as_str(),
        "coral.providers.weather.getForecast"
    );
    assert!(!second.has_more());
    assert!(second.next_cursor().is_none());

    let filtered = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::list().with_provider(
                crate::ProviderCallRoot::parse("coral.providers.weather").expect("namespace"),
            ),
        )
        .await
        .expect("Provider-filtered page");
    assert_eq!(
        filtered
            .hits()
            .iter()
            .map(|hit| hit.call_path().as_str())
            .collect::<Vec<_>>(),
        [
            "coral.functions.internal.formatForecast",
            "coral.providers.weather.getForecast",
        ]
    );

    assert!(matches!(
        fixture
            .search
            .search(
                &principal,
                &workspace_id,
                &SearchRequest::query(SearchQuery::parse("weather").expect("query"))
                    .with_limit(one)
                    .with_cursor(cursor.clone()),
            )
            .await,
        Err(Error::InvalidCursor)
    ));

    let other_principal = AuthenticatedPrincipal::user(
        organization_user(fixture.search.database.clone(), "user:search-pages-other").await,
    );
    assert!(matches!(
        fixture
            .search
            .search(
                &other_principal,
                &workspace_id,
                &SearchRequest::list()
                    .with_limit(one)
                    .with_cursor(cursor.clone()),
            )
            .await,
        Err(Error::InvalidCursor)
    ));
    assert!(matches!(
        fixture
            .search
            .search(
                &principal,
                &workspace_id,
                &SearchRequest::list()
                    .with_limit(one)
                    .with_provider(
                        crate::ProviderCallRoot::parse("coral.providers.weather")
                            .expect("namespace")
                    )
                    .with_cursor(cursor.clone()),
            )
            .await,
        Err(Error::InvalidCursor)
    ));

    // A cursor of the previous ranking format, with a valid digest, pages a
    // result universe that this ranking no longer produces.
    let previous_format = {
        use base64::Engine as _;
        let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let mut payload: serde_json::Value =
            serde_json::from_slice(&engine.decode(cursor.as_str()).expect("cursor bytes"))
                .expect("cursor payload");
        assert_ne!(payload.get("version"), Some(&serde_json::json!(1)));
        payload
            .as_object_mut()
            .expect("payload object")
            .insert("version".to_string(), serde_json::json!(1));
        SearchCursor::parse(&engine.encode(payload.to_string())).expect("cursor text")
    };
    assert!(matches!(
        fixture
            .search
            .search(
                &principal,
                &workspace_id,
                &SearchRequest::list()
                    .with_limit(one)
                    .with_cursor(previous_format),
            )
            .await,
        Err(Error::InvalidCursor)
    ));

    let other_workspace = fixture.create_workspace(&user).await;
    assert!(matches!(
        fixture
            .search
            .search(
                &principal,
                &other_workspace,
                &SearchRequest::list()
                    .with_limit(one)
                    .with_cursor(cursor.clone()),
            )
            .await,
        Err(Error::InvalidCursor)
    ));

    fixture
        .policies
        .set_access_state(
            &change(user, 60, '6'),
            published.policy_target,
            OperationAccessState::Disabled,
        )
        .await
        .expect("disable Operation after first page");
    let after_visibility_change = fixture
        .search
        .search(
            &principal,
            &workspace_id,
            &SearchRequest::list().with_limit(one).with_cursor(cursor),
        )
        .await
        .expect("cursor reapplies current visibility");
    assert!(after_visibility_change.hits().is_empty());
    assert!(!after_visibility_change.has_more());
}

#[test]
fn cursor_transport_shape_is_strict_and_bounded() {
    for invalid in ["", "=", "YWJj=", "not+base64", "has space"] {
        assert!(
            matches!(SearchCursor::parse(invalid), Err(Error::InvalidCursor)),
            "accepted malformed cursor {invalid:?}"
        );
    }
    assert!(matches!(
        SearchCursor::parse(&"a".repeat(crate::SEARCH_CURSOR_MAX_BYTES + 1)),
        Err(Error::InvalidCursor)
    ));
}

#[tokio::test]
async fn forged_cursor_positions_are_invalid_and_a_far_cursor_is_an_empty_page() {
    use base64::Engine as _;
    let fixture = TestModules::new().await;
    let scope = TracerScope::new(&fixture).await;
    let request = SearchRequest::list().with_limit(SearchLimit::parse(1).unwrap());
    let search = |request: SearchRequest| {
        let (search, principal, workspace) =
            (&fixture.search, &scope.principal, &scope.workspace_id);
        async move { search.search(principal, workspace, &request).await }
    };
    let cursor = search(request.clone())
        .await
        .unwrap()
        .next_cursor()
        .unwrap()
        .clone();

    // The digest binds the position, so moving it without the digest is rejected.
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let mut payload: serde_json::Value =
        serde_json::from_slice(&engine.decode(cursor.as_str()).unwrap()).unwrap();
    let position = payload
        .get("position")
        .and_then(serde_json::Value::as_u64)
        .unwrap();
    for forged_position in [0, position + 1, u64::MAX] {
        *payload.get_mut("position").unwrap() = forged_position.into();
        let forged =
            SearchCursor::parse(&engine.encode(serde_json::to_vec(&payload).unwrap())).unwrap();
        assert!(matches!(
            search(request.clone().with_cursor(forged)).await,
            Err(Error::InvalidCursor)
        ));
    }

    // A correctly bound cursor past every result is an empty final page.
    let far = crate::cursor::encode_cursor(
        usize::MAX,
        &scope.principal,
        &scope.workspace_id,
        None,
        None,
    )
    .unwrap();
    let page = search(request.with_cursor(far)).await.unwrap();
    assert!(page.hits().is_empty());
    assert!(!page.has_more());
    assert!(page.next_cursor().is_none());
}

#[test]
fn describe_requires_one_complete_safe_call_path() {
    for invalid in [
        "weather.getForecast",
        "coral.weather",
        "coral.functions",
        "coral.providers.weather",
        "coral.emit.value",
        "coral.providers.weather.bad-segment",
        "coral.functions.internal.bad-segment",
    ] {
        assert!(CallPath::parse(invalid).is_err(), "accepted {invalid:?}");
    }
    for valid in [
        "coral.providers.weather.getForecast",
        "coral.workspace.addProvider",
        "coral.functions.internal.formatForecast",
        "coral.functions.天気.予報",
    ] {
        assert_eq!(
            CallPath::parse(valid).expect("valid Call Path").as_str(),
            valid
        );
    }
}

struct PublishedTracer {
    provider_id: coral_provider::ProviderId,
    policy_target: OperationPolicyTarget,
}

/// One caller-routed Provider and an Agent that its Organization Owner provisions.
struct AgentRoute {
    user: AuthenticatedUser,
    workspace: WorkspaceId,
    context: ChangeContext,
    agent: AuthenticatedPrincipal,
    provider: coral_provider::Provider,
    accounts: coral_provider::account::AccountModule,
    _key: tempfile::TempDir,
}

impl AgentRoute {
    const CALL_PATH: &str = "coral.providers.weather.getForecast";
    const SECRET: &str = "agent-discovery-secret";

    async fn new(fixture: &TestModules) -> Self {
        let user = AuthenticatedUser::organization_member(
            UserId::parse("user:agent-search").unwrap(),
            OrganizationRole::Owner,
        );
        let workspace = fixture.create_workspace(&user).await;
        let context = change(user.clone(), 20, '2');
        let issued = fixture
            .workspaces
            .create_agent(
                &context,
                &workspace,
                coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
                None,
            )
            .await
            .unwrap();
        let agent = fixture
            .workspaces
            .authenticate_agent(&workspace, issued.credential.expose(), 21)
            .await
            .unwrap();
        let provider = fixture
            .publish_configured(
                &context,
                &workspace,
                &credential_document(),
                "weather",
                CredentialRoute::Caller,
            )
            .await;
        let key = tempfile::tempdir().unwrap();
        let accounts = coral_provider::account::AccountModule::new(
            fixture.providers.clone(),
            coral_credentials::CredentialVault::open(&key.path().join("key")).unwrap(),
        );
        Self {
            user,
            workspace,
            context,
            agent,
            provider,
            accounts,
            _key: key,
        }
    }

    fn secret() -> coral_credentials::CredentialValues {
        coral_credentials::CredentialValues::token(
            coral_credentials::Secret::new(Self::SECRET.into()).unwrap(),
        )
    }

    async fn connect(&self) -> coral_provider::account::ProviderAccountId {
        let authorization = self
            .accounts
            .start_agent_authorization(
                &self.context,
                &self.workspace,
                self.agent.principal().id(),
                self.provider.id(),
                ManagementRequestId::parse("73000000-0000-4000-8000-000000000002").unwrap(),
                None,
            )
            .await
            .unwrap();
        self.accounts
            .submit_secret(
                &self.context,
                &self.workspace,
                authorization.id,
                Self::secret(),
            )
            .await
            .unwrap()
            .account_id
            .unwrap()
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one Agent walks missing, substituted, disconnected, and reconnected Account states"
)]
async fn agent_readiness_and_discovery_use_the_exact_agents_account() {
    use coral_provider::access::{AgentAccountUse, ProviderBlocker};
    let fixture = TestModules::new().await;
    let route = AgentRoute::new(&fixture).await;
    let (agent, workspace, context) = (&route.agent, &route.workspace, &route.context);
    let inspect = || {
        fixture
            .search
            .inspect_agent_providers(&route.user, workspace, agent.principal().id())
    };
    let initial = inspect().await.unwrap().remove(0);
    assert_eq!(
        initial.readiness.blockers,
        vec![ProviderBlocker::CallerAccountRequired]
    );
    let AgentAccountUse::Separate(account) = initial.account else {
        panic!("separate Agent Account expected")
    };
    assert!(account.account.is_none());
    assert!(account.can_connect);
    assert!(!account.can_reconnect);
    fixture
        .assert_discovery(agent, workspace, &[], &[AgentRoute::CALL_PATH])
        .await;
    let target = OperationPolicyTarget::new(
        workspace.clone(),
        route.provider.id().clone(),
        coral_catalog::OperationKey::parse("getForecast").unwrap(),
    );
    fixture
        .policies
        .set_access_state(context, target.clone(), OperationAccessState::Disabled)
        .await
        .unwrap();
    assert_eq!(
        inspect().await.unwrap().first().unwrap().readiness.blockers,
        vec![
            ProviderBlocker::CallerAccountRequired,
            ProviderBlocker::NoOperationsReady
        ]
    );
    fixture
        .policies
        .set_access_state(context, target, OperationAccessState::ApprovalRequired)
        .await
        .unwrap();
    // Another User's personal Account never substitutes for the Agent's.
    let member =
        organization_user(fixture.search.database.clone(), "user:agent-search-member").await;
    fixture
        .workspaces
        .set_membership(
            context,
            workspace,
            &member,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let member_context = change(member.clone(), 22, '2');
    let member_authorization = route
        .accounts
        .start_user_authorization(
            &member_context,
            workspace,
            route.provider.id(),
            ManagementRequestId::parse("73000000-0000-4000-8000-000000000004").unwrap(),
            None,
        )
        .await
        .unwrap();
    route
        .accounts
        .submit_secret(
            &member_context,
            workspace,
            member_authorization.id,
            coral_credentials::CredentialValues::token(
                coral_credentials::Secret::new("member-discovery-secret".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    fixture
        .assert_discovery(agent, workspace, &[], &[AgentRoute::CALL_PATH])
        .await;
    assert_eq!(
        inspect().await.unwrap().remove(0).readiness.blockers,
        vec![ProviderBlocker::CallerAccountRequired]
    );
    // A disconnected Agent Account hides discovery again until it is provisioned.
    let agent_account = route.connect().await;
    route
        .accounts
        .disconnect(context, workspace, route.provider.id(), agent_account)
        .await
        .unwrap();
    fixture
        .assert_discovery(agent, workspace, &[], &[AgentRoute::CALL_PATH])
        .await;
    let reconnect = route
        .accounts
        .start_reconnect(
            context,
            workspace,
            route.provider.id(),
            agent_account,
            ManagementRequestId::parse("73000000-0000-4000-8000-000000000005").unwrap(),
            None,
        )
        .await
        .unwrap();
    route
        .accounts
        .submit_secret(context, workspace, reconnect.id, AgentRoute::secret())
        .await
        .unwrap();
    let connected = inspect().await.unwrap().remove(0);
    assert!(connected.readiness.blockers.is_empty());
    let AgentAccountUse::Separate(account) = connected.account else {
        panic!("separate Agent Account expected")
    };
    assert_eq!(
        account.account.unwrap().state,
        coral_provider::account::AccountState::Ready
    );
    assert!(!account.can_connect);
    assert!(account.can_reconnect);
    assert_eq!(
        fixture
            .search
            .inspect_provider_readiness(&route.user, workspace)
            .await
            .unwrap()
            .first()
            .unwrap()
            .blockers,
        vec![],
        "the provisioner's missing personal Account is not Provider readiness"
    );
    fixture
        .assert_discovery(agent, workspace, &[AgentRoute::CALL_PATH], &[])
        .await;
    let page = fixture
        .search
        .search(agent, workspace, &SearchRequest::list())
        .await
        .unwrap();
    assert!(!format!("{page:?}").contains(AgentRoute::SECRET));
    // Provisioning the Agent's Account gives its provisioner no personal Account.
    assert!(
        route
            .accounts
            .get_account(
                context,
                workspace,
                route.provider.id(),
                &coral_provider::account::AccountScope::User
            )
            .await
            .unwrap()
            .account
            .is_none()
    );
}

#[tokio::test]
async fn agent_provider_inspection_requires_the_agents_workspace_and_owner_authority() {
    let fixture = TestModules::new().await;
    let route = AgentRoute::new(&fixture).await;
    let agent = route.agent.principal().id();
    let other_workspace = fixture.create_workspace(&route.user).await;
    assert!(matches!(
        fixture
            .search
            .inspect_agent_providers(&route.user, &other_workspace, agent)
            .await,
        Err(Error::Workspace(coral_workspace::Error::AgentNotFound))
    ));
    let member = organization_user(
        fixture.search.database.clone(),
        "user:agent-inspection-stranger",
    )
    .await;
    fixture
        .workspaces
        .set_membership(
            &route.context,
            &route.workspace,
            &member,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .search
            .inspect_agent_providers(&member, &route.workspace, agent)
            .await,
        Err(Error::Workspace(coral_workspace::Error::AgentNotFound))
    ));
}

#[tokio::test]
async fn agent_discovery_hides_an_incompatible_account_and_rejects_a_disabled_agent() {
    let fixture = TestModules::new().await;
    let route = AgentRoute::new(&fixture).await;
    let (agent, workspace, context) = (&route.agent, &route.workspace, &route.context);
    route.connect().await;
    fixture
        .assert_discovery(agent, workspace, &[AgentRoute::CALL_PATH], &[])
        .await;

    let mut incompatible = credential_document();
    *incompatible
        .pointer_mut("/components/securitySchemes/bearer")
        .unwrap() = serde_json::json!({"type":"apiKey", "in":"header", "name":"x-api-key"});
    fixture
        .regenerate_workspace_provider(context, workspace, route.provider.id(), &incompatible)
        .await;
    fixture
        .assert_discovery(agent, workspace, &[], &[AgentRoute::CALL_PATH])
        .await;
    assert!(
        route
            .accounts
            .list_connection_requests(context, workspace)
            .await
            .unwrap()
            .is_empty(),
        "discovery never starts an interactive Account connection"
    );

    fixture
        .workspaces
        .set_agent_state(
            context,
            workspace,
            agent.principal().id(),
            coral_workspace::AgentState::Disabled,
        )
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .search
            .search(agent, workspace, &SearchRequest::list())
            .await,
        Err(Error::Workspace(coral_workspace::Error::AgentDisabled))
    ));
}

/// One caller-routed Provider, the Workspace's creator, and a second designated Owner.
struct CallerRoute {
    owner: AuthenticatedUser,
    user: AuthenticatedUser,
    principal: AuthenticatedPrincipal,
    workspace: WorkspaceId,
    admin: ChangeContext,
    provider: coral_provider::Provider,
    accounts: coral_provider::account::AccountModule,
    _key: tempfile::TempDir,
}

impl CallerRoute {
    const CALL_PATH: &str = "coral.providers.weather.getForecast";
    const SECRET: &str = "user-discovery-secret-canary";

    async fn new(fixture: &TestModules) -> Self {
        let owner = AuthenticatedUser::organization_member(
            UserId::parse("user:search-admin").unwrap(),
            OrganizationRole::Owner,
        );
        let user = organization_user(fixture.search.database.clone(), "user:search-caller").await;
        let workspace = fixture.create_workspace(&user).await;
        let admin = change(owner.clone(), 20, '2');
        coral_workspace::OrganizationModule::new(fixture.search.database.clone())
            .provision_login(&admin)
            .await
            .unwrap();
        fixture
            .workspaces
            .set_membership(
                &change(user.clone(), 15, '1'),
                &workspace,
                &owner,
                coral_workspace::WorkspaceRole::Owner,
            )
            .await
            .unwrap();
        let provider = fixture
            .publish_configured(
                &admin,
                &workspace,
                &credential_document(),
                "weather",
                CredentialRoute::Caller,
            )
            .await;
        let key = tempfile::tempdir().unwrap();
        let accounts = coral_provider::account::AccountModule::new(
            fixture.providers.clone(),
            coral_credentials::CredentialVault::open(&key.path().join("key")).unwrap(),
        );
        Self {
            owner,
            principal: AuthenticatedPrincipal::user(user.clone()),
            user,
            workspace,
            admin,
            provider,
            accounts,
            _key: key,
        }
    }

    fn context(&self) -> ChangeContext {
        change(self.user.clone(), 30, '3')
    }

    async fn connect(&self) -> coral_provider::account::ProviderAccountId {
        let authorization = self
            .accounts
            .start_user_authorization(
                &self.context(),
                &self.workspace,
                self.provider.id(),
                ManagementRequestId::parse("75000000-0000-4000-8000-000000000002").unwrap(),
                None,
            )
            .await
            .unwrap();
        self.accounts
            .submit_secret(
                &self.context(),
                &self.workspace,
                authorization.id,
                coral_credentials::CredentialValues::token(
                    coral_credentials::Secret::new(Self::SECRET.into()).unwrap(),
                ),
            )
            .await
            .unwrap()
            .account_id
            .unwrap()
    }
}

#[tokio::test]
async fn user_caller_discovery_does_not_wait_for_a_personal_account() {
    let fixture = TestModules::new().await;
    let route = CallerRoute::new(&fixture).await;
    let (principal, workspace) = (&route.principal, &route.workspace);
    let call_path = CallerRoute::CALL_PATH;
    let readiness = || {
        fixture
            .search
            .inspect_provider_readiness_for(&route.user, workspace, route.provider.id())
    };

    // Missing: the User connects when invoking, so discovery shows the Operation
    // and Provider readiness reports no personal Account state.
    fixture
        .assert_discovery(principal, workspace, &[call_path], &[])
        .await;
    assert!(readiness().await.unwrap().blockers.is_empty());
    // Another Workspace Owner needs no personal Account either.
    fixture
        .assert_discovery(
            &AuthenticatedPrincipal::user(route.owner.clone()),
            workspace,
            &[call_path],
            &[],
        )
        .await;

    // Ready.
    let account_id = route.connect().await;
    fixture
        .assert_discovery(principal, workspace, &[call_path], &[])
        .await;
    assert!(readiness().await.unwrap().blockers.is_empty());

    // Incompatible after the Provider's credential contract changes.
    let mut document = credential_document();
    *document
        .pointer_mut("/components/securitySchemes/bearer")
        .unwrap() = serde_json::json!({"type":"apiKey", "in":"header", "name":"x-api-key"});
    fixture
        .regenerate_workspace_provider(&route.admin, workspace, route.provider.id(), &document)
        .await;
    fixture
        .assert_discovery(principal, workspace, &[call_path], &[])
        .await;
    assert!(
        readiness().await.unwrap().blockers.is_empty(),
        "an incompatible personal Account is not Provider readiness"
    );
    let reconnect = route
        .accounts
        .start_reconnect(
            &route.context(),
            workspace,
            route.provider.id(),
            account_id,
            ManagementRequestId::parse("75000000-0000-4000-8000-000000000004").unwrap(),
            None,
        )
        .await
        .unwrap();
    route
        .accounts
        .submit_secret(
            &route.context(),
            workspace,
            reconnect.id,
            // The header key's field is named after the document's `bearer` scheme.
            coral_credentials::CredentialValues::new(std::collections::BTreeMap::from([(
                "bearer".into(),
                coral_credentials::Secret::new(CallerRoute::SECRET.into()).unwrap(),
            )]))
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(readiness().await.unwrap().blockers.is_empty());

    // Disconnected.
    route
        .accounts
        .disconnect(&route.context(), workspace, route.provider.id(), account_id)
        .await
        .unwrap();
    fixture
        .assert_discovery(principal, workspace, &[call_path], &[])
        .await;
    assert!(readiness().await.unwrap().blockers.is_empty());
}

#[tokio::test]
async fn user_caller_discovery_exposes_no_account_evidence_and_starts_no_connection() {
    let fixture = TestModules::new().await;
    let route = CallerRoute::new(&fixture).await;
    let (principal, workspace) = (&route.principal, &route.workspace);
    let private_account_id = route.connect().await.to_string();

    let page = fixture
        .search
        .search(principal, workspace, &SearchRequest::list())
        .await
        .unwrap();
    assert!(!format!("{page:?}").contains(&private_account_id));
    for item in page.hits() {
        let exact = fixture
            .search
            .describe(principal, workspace, item.call_path(), &[])
            .await
            .unwrap();
        assert!(!format!("{exact:?}").contains(&private_account_id));
    }
    assert!(
        fixture
            .search
            .search(
                principal,
                workspace,
                &SearchRequest::query(SearchQuery::parse(CallerRoute::SECRET).unwrap())
            )
            .await
            .unwrap()
            .hits()
            .is_empty()
    );
    for actor in [&route.user, &route.owner] {
        assert!(
            route
                .accounts
                .list_connection_requests(&change(actor.clone(), 90, '9'), workspace)
                .await
                .unwrap()
                .is_empty(),
            "discovery never starts an interactive Account connection"
        );
    }
    fixture
        .save_tracer(principal, workspace, "Private forecast helper", 40)
        .await;
    fixture
        .assert_discovery(
            &AuthenticatedPrincipal::user(route.owner.clone()),
            workspace,
            &[CallerRoute::CALL_PATH],
            &["coral.functions.internal.formatForecast"],
        )
        .await;
}

#[tokio::test]
async fn user_caller_discovery_fails_closed_on_a_corrupt_credential_contract() {
    let fixture = TestModules::new().await;
    let user = organization_user(fixture.search.database.clone(), "user:corrupt-contract").await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace = fixture.create_workspace(&user).await;
    let provider = fixture
        .publish_configured(
            &change(user, 20, '2'),
            &workspace,
            &credential_document(),
            "weather",
            CredentialRoute::Caller,
        )
        .await;
    provider_faults::corrupt_credential_contract(
        &fixture.search.database,
        &workspace,
        provider.id(),
    )
    .await;
    // Discovery reads no User Account, but corrupt Provider evidence is never discoverable.
    let corrupt = |result: Result<(), Error>| {
        matches!(
            result,
            Err(Error::Provider(coral_provider::Error::Catalog(
                coral_catalog::Error::CorruptData(_)
            )))
        )
    };
    assert!(corrupt(
        fixture
            .search
            .search(&principal, &workspace, &SearchRequest::list())
            .await
            .map(drop)
    ));
    assert!(corrupt(
        fixture
            .search
            .describe(
                &principal,
                &workspace,
                &CallPath::parse("coral.providers.weather.getForecast").unwrap(),
                &[]
            )
            .await
            .map(drop)
    ));
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "fixed-route discovery keeps per-Provider policy and Account selection without fallback"
)]
async fn fixed_route_discovery_rechecks_routes_policy_and_exact_account() {
    use coral_credentials::{CredentialVault, Secret};
    use coral_provider::account::AccountModule;

    let fixture = TestModules::new().await;
    let user = AuthenticatedUser::organization_member(
        UserId::parse("user:fixed-search").unwrap(),
        OrganizationRole::Owner,
    );
    let principal = AuthenticatedPrincipal::user(user.clone());
    let context = change(user.clone(), 20, '2');
    let workspace = fixture.create_workspace(&user).await;
    let issued = fixture
        .workspaces
        .create_agent(
            &context,
            &workspace,
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = fixture
        .workspaces
        .authenticate_agent(&workspace, issued.credential.expose(), 21)
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let accounts = AccountModule::new(
        fixture.providers.clone(),
        CredentialVault::open(&dir.path().join("key")).unwrap(),
    );
    let mut providers = Vec::new();
    for namespace in ["aWeather", "zWeather"] {
        let provider = fixture
            .providers
            .publish_shared_fixture(
                &context,
                &workspace,
                OpenApiDocument::parse(&credential_document().to_string()).unwrap(),
                namespace,
                &accounts,
                coral_credentials::CredentialValues::token(
                    Secret::new("user-discovery-secret-canary".into()).unwrap(),
                ),
            )
            .await
            .unwrap();
        let account_id = accounts
            .get_account(
                &context,
                &workspace,
                provider.id(),
                &coral_provider::account::AccountScope::Shared,
            )
            .await
            .unwrap()
            .account
            .unwrap()
            .id;
        providers.push((provider, account_id));
    }
    let [(first, first_account_id), (second, second_account_id)] = providers.as_slice() else {
        unreachable!()
    };
    let ready = fixture
        .search
        .inspect_agent_providers(&user, &workspace, agent.principal().id())
        .await
        .unwrap();
    assert_eq!(ready.len(), 2);
    assert!(
        ready
            .iter()
            .all(|provider| provider.readiness.blockers.is_empty()),
        "shared Accounts require no per-Agent connection"
    );
    let first_path = "coral.providers.aWeather.getForecast";
    let second_path = "coral.providers.zWeather.getForecast";
    let request = SearchRequest::list().with_limit(SearchLimit::parse(1).unwrap());
    let mut cursors = Vec::new();
    for actor in [&principal, &agent] {
        fixture
            .assert_discovery(actor, &workspace, &[first_path, second_path], &[])
            .await;
        let page = fixture
            .search
            .search(actor, &workspace, &request)
            .await
            .unwrap();
        cursors.push(page.next_cursor().unwrap().clone());
    }
    fixture
        .providers
        .change_account_setting_fixture(&context, &workspace, second.id(), CredentialRoute::Caller)
        .await
        .unwrap();
    // The User connects its own Account during Invocation; the Agent needs one now.
    fixture
        .assert_discovery(&principal, &workspace, &[first_path, second_path], &[])
        .await;
    fixture
        .assert_discovery(&agent, &workspace, &[first_path], &[second_path])
        .await;
    let [user_cursor, agent_cursor] = <[_; 2]>::try_from(cursors).unwrap();
    // A cursor does not retain the old fixed route.
    let user_page = fixture
        .search
        .search(
            &principal,
            &workspace,
            &request.clone().with_cursor(user_cursor),
        )
        .await
        .unwrap();
    assert_eq!(
        user_page
            .hits()
            .iter()
            .map(|item| item.call_path().as_str())
            .collect::<Vec<_>>(),
        [second_path]
    );
    assert!(
        fixture
            .search
            .search(
                &agent,
                &workspace,
                &request.clone().with_cursor(agent_cursor)
            )
            .await
            .unwrap()
            .hits()
            .is_empty()
    );
    let setup = fixture
        .providers
        .prepare_update_fixture(
            &context,
            &workspace,
            second.id(),
            OpenApiDocument::parse(&credential_document().to_string()).unwrap(),
        )
        .await
        .unwrap();
    let grant = accounts
        .authorize_setup(
            &context,
            &workspace,
            setup.id(),
            2,
            coral_credentials::CredentialValues::token(
                Secret::new("new-shared-consent".into()).unwrap(),
            ),
        )
        .await
        .unwrap()
        .provisional_grant_id
        .unwrap();
    fixture
        .providers
        .regenerate_provider(
            &context,
            ProviderRegenerationRequest::new(
                ManagementRequestId::parse(&setup.id().to_string()).unwrap(),
                workspace.clone(),
                second.id().clone(),
                setup.id().clone(),
                2,
            )
            .with_account_setting(CredentialRoute::Fixed)
            .with_account_grant(grant)
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .unwrap();
    let target = OperationPolicyTarget::new(
        workspace.clone(),
        second.id().clone(),
        OperationKey::parse("getForecast").unwrap(),
    );
    fixture
        .policies
        .set_access_state(&context, target.clone(), OperationAccessState::Disabled)
        .await
        .unwrap();
    for actor in [&principal, &agent] {
        fixture
            .assert_discovery(actor, &workspace, &[first_path], &[second_path])
            .await;
    }
    fixture
        .policies
        .set_access_state(&context, target, OperationAccessState::Enabled)
        .await
        .unwrap();
    fixture
        .policies
        .set_access_state(
            &context,
            OperationPolicyTarget::new(
                workspace.clone(),
                first.id().clone(),
                OperationKey::parse("getForecast").unwrap(),
            ),
            OperationAccessState::ApprovalRequired,
        )
        .await
        .unwrap();
    for (path, approval) in [
        (first_path, ApprovalRequirement::Required),
        (second_path, ApprovalRequirement::NotRequired),
    ] {
        let exact = fixture
            .search
            .describe(&principal, &workspace, &CallPath::parse(path).unwrap(), &[])
            .await
            .unwrap();
        let AuthoringDescription::Operation(operation) = exact.authoring() else {
            unreachable!()
        };
        assert_eq!(operation.approval_requirement(), approval);
    }
    // Explicit disconnection removes readiness; incompatible shared Update can no
    // longer publish an unusable configuration/account pair (Provider tests cover rejection).
    accounts
        .disconnect(&context, &workspace, first.id(), *first_account_id)
        .await
        .unwrap();
    accounts
        .disconnect(&context, &workspace, second.id(), *second_account_id)
        .await
        .unwrap();
    for actor in [&principal, &agent] {
        fixture
            .assert_discovery(actor, &workspace, &[], &[first_path, second_path])
            .await;
    }
    let reconnect = accounts
        .start_reconnect(
            &context,
            &workspace.clone(),
            first.id(),
            *first_account_id,
            ManagementRequestId::parse("75000000-0000-4000-8000-000000000012").unwrap(),
            None,
        )
        .await
        .unwrap();
    accounts
        .submit_secret(
            &context,
            &workspace.clone(),
            reconnect.id,
            coral_credentials::CredentialValues::token(
                Secret::new("user-discovery-secret-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    for actor in [&principal, &agent] {
        fixture
            .assert_discovery(actor, &workspace, &[first_path], &[second_path])
            .await;
    }
    // Relevance health never controls exact description, even for a credentialed route.
    let failing = fixture.failing_search(&[EntityKind::Operation, EntityKind::SavedFunction]);
    for actor in [&principal, &agent] {
        assert!(matches!(
            failing
                .search(
                    actor,
                    &workspace,
                    &SearchRequest::query(SearchQuery::parse("forecast").unwrap())
                )
                .await,
            Err(Error::ServiceUnavailable)
        ));
        failing
            .describe(
                actor,
                &workspace,
                &CallPath::parse(first_path).unwrap(),
                &[],
            )
            .await
            .unwrap();
        assert!(matches!(
            failing
                .describe(
                    actor,
                    &workspace,
                    &CallPath::parse(second_path).unwrap(),
                    &[]
                )
                .await,
            Err(Error::AuthoringDescriptionNotFound)
        ));
    }
}

#[tokio::test]
async fn none_route_discovery_requires_a_credential_free_current_contract() {
    let fixture = TestModules::new().await;
    let user = AuthenticatedUser::organization_member(
        UserId::parse("user:none-search").unwrap(),
        OrganizationRole::Owner,
    );
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace = fixture.create_workspace(&user).await;
    let context = change(user, 20, '2');
    let issued = fixture
        .workspaces
        .create_agent(
            &context,
            &workspace,
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = fixture
        .workspaces
        .authenticate_agent(&workspace, issued.credential.expose(), 21)
        .await
        .unwrap();
    let mut document = credential_document();
    *document.get_mut("security").unwrap() = serde_json::json!([]);
    let published = fixture
        .publish_configured(
            &context,
            &workspace,
            &document,
            "weather",
            CredentialRoute::None,
        )
        .await;
    let provider = published.id();
    let path = "coral.providers.weather.getForecast";
    for actor in [&principal, &agent] {
        fixture
            .assert_discovery(actor, &workspace, &[path], &[])
            .await;
    }
    fixture
        .providers
        .disable_provider(&context, &workspace, provider)
        .await
        .unwrap();
    fixture
        .assert_discovery(&principal, &workspace, &[], &[path])
        .await;
    fixture
        .providers
        .restore_provider(&context, &workspace, provider)
        .await
        .unwrap();
    fixture
        .assert_discovery(&principal, &workspace, &[path], &[])
        .await;
    let setup = fixture
        .providers
        .prepare_update_fixture(
            &context,
            &workspace,
            provider,
            OpenApiDocument::parse(&credential_document().to_string()).unwrap(),
        )
        .await
        .unwrap();
    fixture
        .providers
        .regenerate_provider(
            &context,
            ProviderRegenerationRequest::new(
                ManagementRequestId::parse(&setup.id().to_string()).unwrap(),
                workspace.clone(),
                provider.clone(),
                setup.id().clone(),
                2,
            )
            .with_account_setting(CredentialRoute::Caller)
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .unwrap();
    // The credentialed contract hides the Operation until the Agent has an Account.
    // The User connects its own Account during Invocation instead.
    fixture
        .assert_discovery(&agent, &workspace, &[], &[path])
        .await;
    fixture
        .assert_discovery(&principal, &workspace, &[path], &[])
        .await;
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "all Principals recheck Workspace lifecycle and Membership authority on cursor pages"
)]
async fn discovery_rechecks_workspace_authority_on_every_page() {
    let fixture = TestModules::new().await;
    let user = organization_user(fixture.search.database.clone(), "user:workspace-search").await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace = fixture.create_workspace(&user).await;
    let context = change(user, 20, '2');
    let member = AuthenticatedPrincipal::user(
        organization_user(fixture.search.database.clone(), "user:member-search").await,
    );
    let organization = coral_workspace::OrganizationModule::new(fixture.search.database.clone());
    fixture
        .workspaces
        .set_membership(
            &context,
            &workspace,
            member.authenticated_user().unwrap(),
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let issued = fixture
        .workspaces
        .create_agent(
            &context,
            &workspace,
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = fixture
        .workspaces
        .authenticate_agent(&workspace, issued.credential.expose(), 21)
        .await
        .unwrap();
    fixture
        .publish_tracer(context.actor(), &workspace, "https://example.test")
        .await;
    let mut document = credential_document();
    *document.get_mut("security").unwrap() = serde_json::json!([]);
    fixture
        .publish_configured(
            &context,
            &workspace,
            &document,
            "weatherAgain",
            CredentialRoute::None,
        )
        .await;
    let request = SearchRequest::list().with_limit(SearchLimit::parse(1).unwrap());
    let mut cursors = Vec::new();
    for actor in [&principal, &agent, &member] {
        let page = fixture
            .search
            .search(actor, &workspace, &request)
            .await
            .unwrap();
        cursors.push(page.next_cursor().unwrap().clone());
    }
    fixture
        .workspaces
        .archive(&context, &workspace)
        .await
        .unwrap();
    let path = CallPath::parse("coral.providers.weatherAgain.getForecast").unwrap();
    for (actor, cursor) in [&principal, &agent, &member].into_iter().zip(&cursors) {
        assert!(matches!(
            fixture
                .search
                .search(
                    actor,
                    &workspace,
                    &request.clone().with_cursor(cursor.clone())
                )
                .await,
            Err(Error::Workspace(coral_workspace::Error::WorkspaceArchived))
        ));
        assert!(matches!(
            fixture.search.describe(actor, &workspace, &path, &[]).await,
            Err(Error::Workspace(coral_workspace::Error::WorkspaceArchived))
        ));
        // A Task still starts in an archived Workspace, and its result names
        // the Call Roots, as an inspection of the Workspace may.
        assert_eq!(
            fixture
                .search
                .searchable_providers(actor, &workspace)
                .await
                .unwrap()
                .iter()
                .map(crate::ProviderCallRoot::as_str)
                .collect::<Vec<_>>(),
            ["coral.providers.weather", "coral.providers.weatherAgain"]
        );
    }
    fixture
        .workspaces
        .restore(&context, &workspace)
        .await
        .unwrap();
    for actor in [&principal, &agent, &member] {
        fixture
            .assert_discovery(
                actor,
                &workspace,
                &[
                    "coral.providers.weather.getForecast",
                    "coral.providers.weatherAgain.getForecast",
                ],
                &[],
            )
            .await;
    }
    fixture
        .workspaces
        .update_agent_registration(
            &context,
            &workspace,
            issued.registration.id(),
            true,
            Some(1),
        )
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .search
            .search(
                &agent,
                &workspace,
                &request
                    .clone()
                    .with_cursor(cursors.get(1).expect("Agent cursor").clone())
            )
            .await,
        Err(Error::Workspace(
            coral_workspace::Error::AgentRegistrationExpired
        ))
    ));
    assert!(matches!(
        fixture
            .search
            .describe(&agent, &workspace, &path, &[])
            .await,
        Err(Error::Workspace(
            coral_workspace::Error::AgentRegistrationExpired
        ))
    ));
    let administrator = change(
        AuthenticatedUser::organization_member(
            UserId::parse("user:search-role-admin").unwrap(),
            OrganizationRole::Owner,
        ),
        1,
        '1',
    );
    organization.provision_login(&administrator).await.unwrap();
    organization
        .set_enabled(
            &administrator,
            member.authenticated_user().unwrap().user_id(),
            1,
            false,
        )
        .await
        .unwrap();
    // A disabled Member is denied on a cursor page, and an Organization Owner
    // without a Membership on a first page, like any non-Member.
    let organization_owner = AuthenticatedPrincipal::user(administrator.actor().clone());
    for (actor, request) in [
        (
            &member,
            request.clone().with_cursor(cursors.last().unwrap().clone()),
        ),
        (&organization_owner, request),
    ] {
        assert!(matches!(
            fixture.search.search(actor, &workspace, &request).await,
            Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
        ));
        assert!(matches!(
            fixture.search.describe(actor, &workspace, &path, &[]).await,
            Err(Error::AuthoringDescriptionNotFound)
        ));
    }
}

#[tokio::test]
async fn exact_provider_readiness_does_not_read_unrelated_providers() {
    let fixture = TestModules::new().await;
    let upstream = MockServer::start().await;
    let user = organization_user(fixture.search.database.clone(), "user:exact-readiness").await;
    let workspace = fixture.create_workspace(&user).await;
    let wanted = fixture
        .publish_tracer(&user, &workspace, &upstream.uri())
        .await;
    let unrelated = fixture
        .publish_configured(
            &change(user.clone(), 40, '4'),
            &workspace,
            &credential_document(),
            "unrelated",
            CredentialRoute::Caller,
        )
        .await;
    provider_faults::make_provider_unreadable(&fixture.search.database, &workspace, unrelated.id())
        .await;
    assert!(
        matches!(
            fixture
                .search
                .inspect_provider_readiness(&user, &workspace)
                .await,
            Err(Error::Provider(_))
        ),
        "the repository fault must affect whole-Workspace inspection"
    );
    let exact = fixture
        .search
        .inspect_provider_readiness_for(&user, &workspace, &wanted.provider_id)
        .await
        .expect("one Provider must not depend on an unrelated catalog");
    assert_eq!(exact.provider.id(), &wanted.provider_id);
    assert!(exact.blockers.is_empty());
}

#[tokio::test]
async fn readiness_inspection_does_not_require_saved_function_persistence() {
    // Drop Functions' schema and the search index: any accidental
    // repository read must fail, even when no Functions exist. Readiness
    // depends only on Operation domains and reads no index.
    let fixture = TestModules::new().await;
    fixture
        .fixture
        .execute_fixture_sql(
            "DROP TABLE search_entities; DROP TABLE search_index_states; DROP TABLE saved_function_records; DROP TABLE saved_function_lifecycle_events;",
        )
        .await
        .expect("drop Functions and the search index");
    let upstream = MockServer::start().await;
    let user = organization_user(fixture.search.database.clone(), "user:focused-readiness").await;
    let workspace = fixture.create_workspace(&user).await;
    fixture
        .publish_tracer(&user, &workspace, &upstream.uri())
        .await;

    let inspections = fixture
        .search
        .inspect_provider_readiness(&user, &workspace)
        .await
        .expect("readiness never reads Saved Functions");
    assert_eq!(inspections.len(), 1);
    assert!(inspections.first().unwrap().blockers.is_empty());
    let agent = fixture
        .workspaces
        .create_agent(
            &change(user.clone(), 50, '5'),
            &workspace,
            coral_workspace::AgentDisplayName::parse("No function reads").unwrap(),
            None,
        )
        .await
        .unwrap();
    let inspected = fixture
        .search
        .inspect_agent_providers(&user, &workspace, agent.agent.principal().id())
        .await
        .expect("Agent readiness never reads Saved Functions either");
    assert_eq!(
        inspected
            .into_iter()
            .map(|value| value.readiness)
            .collect::<Vec<_>>(),
        inspections
    );
}

#[tokio::test]
async fn readiness_ignores_unreadable_saved_functions_for_the_same_caller() {
    let fixture = TestModules::new().await;
    let upstream = MockServer::start().await;
    let user = organization_user(
        fixture.search.database.clone(),
        "user:readiness-many-functions",
    )
    .await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace = fixture.create_workspace(&user).await;
    fixture
        .publish_tracer(&user, &workspace, &upstream.uri())
        .await;
    let change = SavedFunctionChangeContext::new(principal.clone(), 40, trace('4'));
    let mut saved = Vec::new();
    for index in 0..32 {
        let function = fixture
            .saved_functions
            .save(
                &change,
                &workspace,
                SavedFunctionSave::new(
                    SavedFunctionPath::parse(&format!("helpers.function{index}")).unwrap(),
                    SavedFunctionDescription::parse("Return a number").unwrap(),
                    SavedFunctionSource::parse(
                        "export default function value(): number { return 1; }",
                    )
                    .unwrap(),
                ),
            )
            .await
            .unwrap();
        saved.push(function);
    }
    let before = fixture
        .search
        .inspect_provider_readiness(&user, &workspace)
        .await
        .unwrap();
    for function in &saved {
        function_faults::corrupt_description(&fixture.search.database, &workspace, function.id())
            .await;
    }
    assert!(
        matches!(
            fixture
                .search
                .search(&principal, &workspace, &SearchRequest::list())
                .await,
            Err(Error::Functions(coral_functions::Error::CorruptData(_)))
        ),
        "the fault must affect real Saved Function repository reads"
    );
    let after = fixture
        .search
        .inspect_provider_readiness(&user, &workspace)
        .await
        .expect("unrelated authoring persistence must not affect readiness");
    assert_eq!(after, before);
}

#[tokio::test]
#[expect(
    clippy::indexing_slicing,
    reason = "one Workspace has exactly one Provider to inspect"
)]
async fn provider_inspection_reports_policy_and_lifecycle_blockers() {
    use coral_provider::access::ProviderBlocker;
    let fixture = TestModules::new().await;
    let upstream = MockServer::start().await;
    let user = organization_user(fixture.search.database.clone(), "user:inspect-ready").await;
    let workspace = fixture.create_workspace(&user).await;
    let published = fixture
        .publish_tracer(&user, &workspace, &upstream.uri())
        .await;
    let ready = fixture
        .search
        .inspect_provider_readiness(&user, &workspace)
        .await
        .expect("inspect Providers");
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].provider.id(), &published.provider_id);
    assert!(ready[0].blockers.is_empty());
    assert!(!format!("{ready:?}").contains(ACCOUNT_LABEL_CANARY));

    // Disabling the only Operation leaves the Provider with nothing to call.
    fixture
        .policies
        .set_access_state(
            &change(user.clone(), 50, '5'),
            published.policy_target.clone(),
            OperationAccessState::Disabled,
        )
        .await
        .unwrap();
    let disabled = fixture
        .search
        .inspect_provider_readiness(&user, &workspace)
        .await
        .unwrap();
    assert_eq!(
        disabled[0].blockers,
        vec![ProviderBlocker::NoOperationsReady]
    );

    // Revocation is reported by the Provider Module, ahead of discovery evidence.
    fixture
        .providers
        .disable_provider(
            &change(user.clone(), 60, '6'),
            &workspace.clone(),
            &published.provider_id,
        )
        .await
        .unwrap();
    let revoked = fixture
        .search
        .inspect_provider_readiness(&user, &workspace)
        .await
        .unwrap();
    assert_eq!(revoked[0].blockers, vec![ProviderBlocker::ProviderDisabled]);

    // A User without Workspace access cannot inspect Providers at all.
    let stranger =
        organization_user(fixture.search.database.clone(), "user:inspect-stranger").await;
    assert!(matches!(
        fixture
            .search
            .inspect_provider_readiness(&stranger, &workspace)
            .await,
        Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
    ));
}

fn credential_document() -> serde_json::Value {
    serde_json::json!({
        "openapi":"3.1.0", "info":{"title":"Private weather","version":"1"},
        "servers":[{"url":"https://weather.example"}], "security":[{"bearer":[]}],
        "components":{"securitySchemes":{"bearer":{"type":"http","scheme":"bearer"}}},
        "paths":{"/forecast":{"get":{"operationId":"getForecast","responses":{"200":{"description":"Forecast"}}}}}
    })
}

struct TestModules {
    fixture: TestDatabase,
    workspaces: WorkspaceModule,
    providers: ProviderModule,
    policies: OperationPolicyModule,
    saved_functions: SavedFunctionModule,
    search: UniversalSearchModule,
    index: crate::SearchIndex,
}

impl TestModules {
    async fn new() -> Self {
        Self::with_plan(coral_db::migration_plan()).await
    }

    /// The modules over a database that `plan` migrated, such as one from
    /// before a search step.
    async fn with_plan(plan: &coral_db::MigrationPlan) -> Self {
        let fixture = TestDatabase::new(plan)
            .await
            .expect("create Universal Search test database");
        let database = fixture.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let providers = ProviderModule::with_running_catalog(
            DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
            database.clone(),
            workspaces.clone(),
        );
        let policies = OperationPolicyModule::new(database.clone(), workspaces.clone());
        let saved_functions = SavedFunctionModule::new(
            database.clone(),
            workspaces.clone(),
            coral_functions::IsolateLimits::default(),
        )
        .expect("start Saved Function compiler");
        let index = crate::SearchIndex::new(database.clone(), std::time::Duration::from_mins(5));
        let search = UniversalSearchModule::new(database, workspaces.clone());
        Self {
            fixture,
            workspaces,
            providers,
            policies,
            saved_functions,
            search,
            index,
        }
    }

    /// Converges the search index on what the owners hold: one reconcile pass.
    async fn converge(&self) {
        self.index.reconcile().await;
    }

    /// Saves one small Saved Function at `path` with `description`.
    async fn save_function(
        &self,
        principal: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        path: &str,
        description: &str,
        occurred_at_unix_nanos: i64,
    ) -> coral_functions::SavedFunction {
        self.save_source(
            principal,
            workspace_id,
            path,
            description,
            "export default function run(input: { limit: number }): number { return input.limit; }",
            occurred_at_unix_nanos,
        )
        .await
    }

    /// Saves `source` as the Saved Function at `path` with `description`.
    async fn save_source(
        &self,
        principal: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        path: &str,
        description: &str,
        source: &str,
        occurred_at_unix_nanos: i64,
    ) -> coral_functions::SavedFunction {
        self.saved_functions
            .save(
                &SavedFunctionChangeContext::new(
                    principal.clone(),
                    occurred_at_unix_nanos,
                    trace('5'),
                ),
                workspace_id,
                SavedFunctionSave::new(
                    SavedFunctionPath::parse(path).expect("Saved Function path"),
                    SavedFunctionDescription::parse(description).expect("description"),
                    SavedFunctionSource::parse(source).expect("source"),
                ),
            )
            .await
            .expect("save Saved Function")
    }

    /// Deletes the Saved Function at `path`. Its row in the index cascades
    /// with it, and no index write follows.
    async fn delete_function(
        &self,
        principal: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        path: &str,
        occurred_at_unix_nanos: i64,
    ) {
        self.saved_functions
            .delete(
                &SavedFunctionChangeContext::new(
                    principal.clone(),
                    occurred_at_unix_nanos,
                    trace('5'),
                ),
                workspace_id,
                &SavedFunctionPath::parse(path).expect("Saved Function path"),
            )
            .await
            .expect("delete Saved Function");
    }

    async fn publish_configured(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        document: &serde_json::Value,
        namespace: &str,
        route: CredentialRoute,
    ) -> coral_provider::Provider {
        coral_workspace::OrganizationModule::new(self.search.database.clone())
            .provision_login(context)
            .await
            .unwrap();
        self.providers
            .publish_openapi_fixture(
                context,
                workspace,
                OpenApiDocument::parse(&document.to_string()).unwrap(),
                namespace,
                route,
            )
            .await
            .unwrap()
    }

    async fn regenerate_workspace_provider(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        provider: &coral_provider::ProviderId,
        document: &serde_json::Value,
    ) {
        let setup = self
            .providers
            .prepare_openapi_fixture(
                context,
                workspace,
                OpenApiDocument::parse(&document.to_string()).unwrap(),
            )
            .await
            .unwrap();
        let publication = self
            .providers
            .regenerate_provider(
                context,
                ProviderRegenerationRequest::new(
                    ManagementRequestId::parse(&setup.id().to_string()).unwrap(),
                    workspace.clone(),
                    provider.clone(),
                    setup.id().clone(),
                    1,
                )
                .with_incompatible_configuration_confirmation(),
            )
            .await
            .unwrap();
        assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    }

    async fn assert_discovery(
        &self,
        principal: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        visible: &[&str],
        hidden: &[&str],
    ) {
        self.converge().await;
        for request in [
            SearchRequest::list(),
            SearchRequest::query(SearchQuery::parse("forecast").unwrap()),
        ] {
            let page = self
                .search
                .search(principal, workspace, &request)
                .await
                .unwrap();
            let mut paths = page
                .hits()
                .iter()
                .map(|item| item.call_path().as_str())
                .collect::<Vec<_>>();
            paths.sort_unstable();
            assert_eq!(paths, visible);
            for item in page.hits() {
                let exact = self
                    .search
                    .describe(principal, workspace, item.call_path(), &[])
                    .await
                    .unwrap();
                assert_eq!(exact.authoring().call_path(), item.call_path());
                assert!(!format!("{exact:?}").contains("user-discovery-secret-canary"));
            }
            assert!(!format!("{page:?}").contains("user-discovery-secret-canary"));
        }
        if let Some(user) = principal.authenticated_user() {
            let inspections = self
                .search
                .inspect_provider_readiness(user, workspace)
                .await
                .unwrap();
            for inspection in inspections {
                let prefix = format!("{}.", inspection.provider.call_root().as_str());
                // A User's personal Account is not readiness, so it agrees with discovery.
                assert_eq!(
                    inspection.blockers.is_empty(),
                    visible.iter().any(|path| path.starts_with(&prefix)),
                    "User readiness must agree with discovery",
                );
            }
        }
        for path in hidden
            .iter()
            .copied()
            .chain(["coral.providers.absent.getForecast"])
        {
            assert!(matches!(
                self.search
                    .describe(principal, workspace, &CallPath::parse(path).unwrap(), &[])
                    .await,
                Err(Error::AuthoringDescriptionNotFound)
            ));
        }
    }

    async fn create_workspace(&self, user: &AuthenticatedUser) -> WorkspaceId {
        coral_workspace::OrganizationModule::new(self.search.database.clone())
            .provision_login(&change(user.clone(), 1, '1'))
            .await
            .unwrap();
        self.workspaces
            .create(
                &change(user.clone(), 10, '1'),
                WorkspaceDisplayName::parse("Search tracer").expect("display name"),
            )
            .await
            .expect("create Workspace")
            .id()
            .clone()
    }

    async fn publish_openapi(
        &self,
        user: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
        document: &str,
        namespace: &str,
        occurred_at_unix_nanos: i64,
    ) -> coral_provider::ProviderId {
        self.providers
            .publish_openapi_fixture(
                &change(user.clone(), occurred_at_unix_nanos, '8'),
                workspace_id,
                OpenApiDocument::parse(document).expect("OpenAPI document"),
                namespace,
                CredentialRoute::None,
            )
            .await
            .expect("publish Provider")
            .id()
            .clone()
    }

    /// Discovery over the same database whose named retrievers fail to rank.
    fn failing_search(&self, kinds: &[EntityKind]) -> UniversalSearchModule {
        UniversalSearchModule::with_failing_rankings(
            self.search.database.clone(),
            self.workspaces.clone(),
            kinds,
        )
    }

    async fn publish_tracer(
        &self,
        user: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
        upstream_url: &str,
    ) -> PublishedTracer {
        let document = format!(
            r#"{{
              "openapi": "3.1.0",
              "info": {{
                "title": "Weather Tracer",
                "version": "1.0.0",
                "x-account-label": "{ACCOUNT_LABEL_CANARY}"
              }},
              "servers": [{{ "url": "{upstream_url}/{EXECUTION_BINDING_CANARY}" }}],
              "security": [],
              "paths": {{
                "/forecast": {{
                  "get": {{
                    "operationId": "getForecast",
                    "summary": "Get one weather forecast",
                    "description": "Fetch the current conditions for one forecast location.",
                    "deprecated": true,
                    "tags": ["forecast", "weather"],
                    "externalDocs": {{
                      "description": "Use a stable location identifier when one is available.",
                      "url": "https://docs.example.test/forecast"
                    }},
                    "x-private-schema": "{RAW_SCHEMA_CANARY}",
                    "responses": {{
                      "200": {{
                        "description": "Forecast",
                        "content": {{
                          "application/json": {{
                            "schema": {{
                              "type": "object",
                              "properties": {{
                                "temperatureC": {{ "type": "number" }}
                              }},
                              "required": ["temperatureC"]
                            }}
                          }}
                        }}
                      }}
                    }}
                  }}
                }}
              }}
            }}"#
        );
        let setup = self
            .providers
            .prepare_openapi_fixture(
                &change(user.clone(), 20, '2'),
                workspace_id,
                OpenApiDocument::parse(&document).expect("OpenAPI document"),
            )
            .await
            .expect("create Provider Setup");
        let publication = self
            .providers
            .publish_workspace_provider(
                &change(user.clone(), 30, '3'),
                WorkspaceProviderPublicationRequest::new(
                    ManagementRequestId::parse("52000000-0000-4000-8000-000000000001")
                        .expect("request id"),
                    workspace_id.clone(),
                    setup.id().clone(),
                    ProviderNamespace::parse("weather").expect("namespace"),
                    CredentialRoute::None,
                ),
            )
            .await
            .expect("publish tracer Provider");
        let published = publication
            .published_provider()
            .expect("successful Provider publication");
        let operation_key = OperationKey::parse("getForecast").expect("Operation Key");
        PublishedTracer {
            provider_id: published.id().clone(),
            policy_target: OperationPolicyTarget::new(
                workspace_id.clone(),
                published.access().id().clone(),
                operation_key,
            ),
        }
    }

    async fn save_large_function(
        &self,
        principal: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        index: usize,
    ) -> coral_functions::SavedFunction {
        let fields = (0..1_800)
            .map(|field| format!("field{field:04}: string;"))
            .collect::<Vec<_>>()
            .join("\n");
        self.saved_functions
            .save(
                &SavedFunctionChangeContext::new(
                    principal.clone(),
                    100,
                    trace('7'),
                ),
                workspace_id,
                SavedFunctionSave::new(
                    SavedFunctionPath::parse(&format!("large.function{index:02}"))
                        .expect("Saved Function path"),
                    SavedFunctionDescription::parse(&format!(
                        "{}Large paging contract",
                        "Filler words before the match. ".repeat(100)
                    ))
                    .expect("description"),
                    SavedFunctionSource::parse(&format!(
                        "interface LargeType {{\n{fields}\n}}\nexport default function large(input: LargeType): LargeType {{ return input; }}"
                    ))
                    .expect("large source"),
                ),
            )
            .await
            .expect("save large function")
    }

    async fn save_tracer(
        &self,
        principal: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        description: &str,
        occurred_at_unix_nanos: i64,
    ) -> coral_functions::SavedFunction {
        self.saved_functions
            .save(
                &SavedFunctionChangeContext::new(
                    principal.clone(),
                    occurred_at_unix_nanos,
                    trace('4'),
                ),
                workspace_id,
                SavedFunctionSave::new(
                    SavedFunctionPath::parse("internal.formatForecast").expect("Saved path"),
                    SavedFunctionDescription::parse(description).expect("description"),
                    SavedFunctionSource::parse(&format!(
                        r#"interface Forecast {{ temperatureC: number }}
const sourceEvidence = "{SAVED_SOURCE_CANARY}";
export default function formatForecast(value: Forecast): string {{
    void sourceEvidence;
    return `${{value.temperatureC}} C`;
}}"#
                    ))
                    .expect("source"),
                ),
            )
            .await
            .expect("save tracer Saved Function")
    }
}

async fn organization_user(
    database: std::sync::Arc<coral_db::Database>,
    id: &str,
) -> AuthenticatedUser {
    coral_test_support::provision_user(database, id, OrganizationRole::Member).await
}

coral_test_support::provider_setup_fixtures!(coral_provider);
