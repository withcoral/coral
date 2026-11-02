//! `discover` with exact Call Paths: several at once, with the ones the
//! caller cannot discover reported missing, each with the nearest paths it
//! can. The visibility and abridging rules are tested at `describe` and
//! `signature::abridge`.

use super::{TestModules, TracerScope};
use crate::{Discovery, DiscoveryRequest, Error, ExactLookup, ExactPaths};
use coral_catalog::{SEARCH_PATH_MAX_BYTES, SEARCH_PATHS_MAX, ToolKey};
use coral_policy::{ToolAccessState, ToolPolicyTarget};
use coral_test_support::change;

async fn look_up(fixture: &TestModules, scope: &TracerScope, paths: &[&str]) -> ExactLookup {
    let paths = ExactPaths::new(paths.iter().map(ToString::to_string).collect(), Vec::new())
        .expect("a bounded lookup");
    let found = fixture
        .search
        .discover(
            &scope.principal,
            &scope.workspace_id,
            &DiscoveryRequest::Paths(paths),
        )
        .await
        .expect("an exact lookup");
    let Discovery::Exact(lookup) = found else {
        panic!("paths return an exact lookup: {found:?}");
    };
    lookup
}

// Catches: a lookup that fails as a whole for one path it cannot describe,
// tells a disabled Tool from an absent one, or describes a path otherwise
// than `describe` does.
#[tokio::test]
async fn a_lookup_describes_each_path_it_can_and_reports_the_rest_missing_in_order() {
    let fixture = TestModules::new().await;
    let scope = TracerScope::new(&fixture).await;
    let tool = scope.describe(&fixture, TracerScope::TOOL).await.unwrap();
    let function = scope
        .describe(&fixture, TracerScope::FUNCTION)
        .await
        .unwrap();
    let absent = "coral.providers.weather.getForecasts";
    let malformed = "coral.providers.weather.get-forecast";

    let lookup = look_up(
        &fixture,
        &scope,
        &[absent, TracerScope::TOOL, malformed, TracerScope::FUNCTION],
    )
    .await;

    assert_eq!(lookup.descriptions(), [tool, function.clone()]);
    assert_eq!(
        lookup
            .missing()
            .iter()
            .map(crate::MissingPath::call_path)
            .collect::<Vec<_>>(),
        [absent, malformed]
    );

    fixture
        .policies
        .set_access_state(
            &change(scope.user.clone(), 50, '5'),
            scope.published.policy_target.clone(),
            ToolAccessState::Disabled,
        )
        .await
        .expect("disable the tracer Tool");
    let lookup = look_up(
        &fixture,
        &scope,
        &[TracerScope::TOOL, TracerScope::FUNCTION],
    )
    .await;
    assert_eq!(lookup.descriptions(), [function]);
    assert_eq!(
        lookup
            .missing()
            .iter()
            .map(crate::MissingPath::call_path)
            .collect::<Vec<_>>(),
        [TracerScope::TOOL]
    );
}

// Catches: a lookup bound that the Built-in Tool's contract states but the
// operation does not hold, or one that refuses a lookup at its limit.
#[test]
fn a_lookup_is_bounded_before_it_reads_anything() {
    let path = |bytes: usize| format!("coral.functions.{}", "a".repeat(bytes - 16));
    let names = |count: usize| vec!["Repository".to_owned(); count];
    for (story, paths, types, accepted) in [
        ("no path", Vec::new(), Vec::new(), false),
        (
            "the most paths",
            vec![path(20); SEARCH_PATHS_MAX],
            Vec::new(),
            true,
        ),
        (
            "too many paths",
            vec![path(20); SEARCH_PATHS_MAX + 1],
            Vec::new(),
            false,
        ),
        (
            "the longest path",
            vec![path(SEARCH_PATH_MAX_BYTES)],
            Vec::new(),
            true,
        ),
        (
            "a path too long",
            vec![path(SEARCH_PATH_MAX_BYTES + 1)],
            Vec::new(),
            false,
        ),
        ("types for one path", vec![path(20)], names(1), true),
        ("types for two paths", vec![path(20); 2], names(1), false),
    ] {
        let bounded = ExactPaths::new(paths, types);
        assert_eq!(bounded.is_ok(), accepted, "{story}: {bounded:?}");
        if !accepted {
            assert!(
                matches!(bounded, Err(Error::InvalidInput(_))),
                "{story}: {bounded:?}"
            );
        }
    }
    assert!(matches!(
        ExactPaths::new(vec![path(20)], names(crate::DESCRIBE_TYPES_MAX + 1)),
        Err(Error::TypesLimit)
    ));
}

/// Notion's page Tools, named as Notion's operation ids name them.
const NOTION: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Notion", "version": "1.0.0" },
  "servers": [{ "url": "https://api.notion.example" }],
  "security": [],
  "paths": {
    "/pages/{page_id}": {
      "get": {
        "operationId": "retrieve-a-page",
        "summary": "Retrieve a page",
        "parameters": [{ "name": "page_id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "OK" } }
      },
      "patch": {
        "operationId": "update-page",
        "summary": "Update page properties",
        "parameters": [{ "name": "page_id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "OK" } }
      }
    },
    "/pages/{page_id}/properties/{property_id}": {
      "get": {
        "operationId": "retrieve-a-page-property",
        "summary": "Retrieve a page property item",
        "parameters": [
          { "name": "page_id", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "property_id", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "OK" } }
      }
    },
    "/databases/{database_id}/query": {
      "post": {
        "operationId": "query-a-database",
        "summary": "Query a database",
        "parameters": [{ "name": "database_id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "OK" } }
      }
    }
  }
}"#;

/// GitHub's repository and issue Tools, namespaced as GitHub's operation
/// ids namespace them.
const GITHUB: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "GitHub", "version": "1.0.0" },
  "servers": [{ "url": "https://api.github.example" }],
  "security": [],
  "paths": {
    "/repos/{owner}/{repo}": {
      "get": {
        "operationId": "repos/get",
        "summary": "Get a repository",
        "parameters": [
          { "name": "owner", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "repo", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "OK" } }
      }
    },
    "/orgs/{org}/repos": {
      "get": {
        "operationId": "repos/list-for-org",
        "summary": "List organization repositories",
        "parameters": [{ "name": "org", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "OK" } }
      }
    },
    "/repos/{owner}/{repo}/issues": {
      "get": {
        "operationId": "issues/list-for-repo",
        "summary": "List repository issues",
        "parameters": [
          { "name": "owner", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "repo", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "OK" } }
      }
    },
    "/user": {
      "get": {
        "operationId": "users/get-authenticated",
        "summary": "Get the authenticated user",
        "responses": { "200": { "description": "OK" } }
      }
    }
  }
}"#;

fn suggestions(lookup: &ExactLookup) -> Vec<(&str, Vec<&str>)> {
    lookup
        .missing()
        .iter()
        .map(|missing| {
            (
                missing.call_path(),
                missing.suggestions().iter().map(String::as_str).collect(),
            )
        })
        .collect()
}

// Catches: a guessed path that names its Tool's words in another order or
// spelling, but is reported with nothing to call instead; a suggestion
// from another Call Root, or one the caller cannot call.
#[tokio::test]
async fn a_missing_path_names_the_nearest_paths_the_caller_can_call_in_its_call_root() {
    let fixture = TestModules::new().await;
    let scope = TracerScope::new(&fixture).await;
    let notion = fixture
        .publish_openapi(&scope.user, &scope.workspace_id, NOTION, "notion", 45)
        .await;
    fixture
        .publish_openapi(&scope.user, &scope.workspace_id, GITHUB, "github", 46)
        .await;
    fixture.converge().await;

    let lookup = look_up(
        &fixture,
        &scope,
        &[
            "coral.providers.notion.retrievePage",
            "coral.providers.github.getRepo",
            "coral.providers.github.repos.listForOrganization",
            "coral.providers.absent.retrievePage",
            "coral.functions.retrievePage",
            "coral.providers.notion.retrieve-page",
        ],
    )
    .await;
    let found = suggestions(&lookup);
    assert_eq!(
        found.first(),
        Some(&(
            "coral.providers.notion.retrievePage",
            vec![
                "coral.providers.notion.retrieveAPage",
                "coral.providers.notion.retrieveAPageProperty",
                "coral.providers.notion.updatePage",
            ]
        )),
        "the Tool with the same words comes first, then ranked ones"
    );
    let first = |path: &str| {
        found
            .iter()
            .find(|(missing, _)| *missing == path)
            .and_then(|(_, suggested)| suggested.first().copied())
    };
    assert_eq!(
        first("coral.providers.github.getRepo"),
        Some("coral.providers.github.repos.get")
    );
    assert_eq!(
        first("coral.providers.github.repos.listForOrganization"),
        Some("coral.providers.github.repos.listForOrg")
    );
    for (missing, suggested) in &found {
        let root = missing
            .rsplit_once('.')
            .map(|(root, _leaf)| root)
            .unwrap_or_default();
        assert!(suggested.len() <= crate::SUGGESTIONS_MAX, "{missing}");
        assert!(
            suggested.iter().all(|path| {
                path.starts_with("coral.providers.notion.")
                    && missing.starts_with("coral.providers.notion.")
                    || path.starts_with("coral.providers.github.")
                        && missing.starts_with("coral.providers.github.")
            }),
            "{missing} under {root}: {suggested:?}"
        );
    }
    for nothing_to_suggest in [
        "coral.providers.absent.retrievePage",
        "coral.functions.retrievePage",
    ] {
        assert!(
            found
                .iter()
                .any(|(missing, suggested)| *missing == nothing_to_suggest && suggested.is_empty()),
            "{nothing_to_suggest}: {found:?}"
        );
    }

    fixture
        .policies
        .set_access_state(
            &change(scope.user.clone(), 50, '5'),
            ToolPolicyTarget::new(
                scope.workspace_id.clone(),
                notion,
                ToolKey::parse("retrieve-a-page").expect("Tool Key"),
            ),
            ToolAccessState::Disabled,
        )
        .await
        .expect("disable retrieveAPage");
    fixture.converge().await;
    let lookup = look_up(&fixture, &scope, &["coral.providers.notion.retrievePage"]).await;
    let found = suggestions(&lookup);
    let [(_, suggested)] = found.as_slice() else {
        panic!("one missing path: {found:?}");
    };
    assert!(
        !suggested.contains(&"coral.providers.notion.retrieveAPage"),
        "a disabled Tool is never suggested: {found:?}"
    );
    assert!(!suggested.is_empty(), "the others still are");
}
