//! `describe` with and without `types`, through a published Provider. The
//! abridging rules are tested at their owner, `signature::abridge`.

use serde_json::json;

use super::{TestModules, TracerScope};
use crate::{AuthoringDescription, CallPath, Error};

const ISSUES_LIST: &str = "coral.providers.github.issues.list";

/// A Provider whose `issues.list` result names a type too wide for the
/// extras budget.
async fn github(fixture: &TestModules, scope: &TracerScope) {
    let fields: serde_json::Map<_, _> = (0..120)
        .map(|index| (format!("field_{index:03}_url"), json!({ "type": "string" })))
        .collect();
    let document = json!({
        "openapi": "3.1.0",
        "info": { "title": "GitHub", "version": "1.0.0" },
        "servers": [{ "url": "https://api.github.example" }],
        "security": [],
        "paths": { "/issues": { "get": { "operationId": "issues/list", "responses": { "200": {
            "description": "OK",
            "content": { "application/json": { "schema": {
                "type": "array", "items": { "$ref": "#/components/schemas/issue" }
            } } }
        } } } } },
        "components": { "schemas": {
            "issue": { "type": "object", "properties": {
                "repository": { "$ref": "#/components/schemas/repository" }
            } },
            "repository": { "type": "object", "properties": fields },
        } },
    });
    fixture
        .publish_openapi(
            &scope.user,
            &scope.workspace_id,
            &document.to_string(),
            "github",
            20,
        )
        .await;
}

async fn describe(
    fixture: &TestModules,
    scope: &TracerScope,
    types: &[&str],
) -> Result<crate::ExactAuthoringDescription, Error> {
    let types: Vec<String> = types.iter().map(ToString::to_string).collect();
    fixture
        .search
        .describe(
            &scope.principal,
            &scope.workspace_id,
            &CallPath::parse(ISSUES_LIST).unwrap(),
            &types,
        )
        .await
}

/// Both results are the Tool's description; only the declaration
/// differs.
#[tokio::test]
async fn describe_returns_the_fitted_declaration_or_the_named_types_of_the_same_tool() {
    let fixture = TestModules::new().await;
    let scope = TracerScope::new(&fixture).await;
    github(&fixture, &scope).await;

    let mut declarations = Vec::new();
    for types in [&[][..], &["Repository"]] {
        let exact = describe(&fixture, &scope, types).await.unwrap();
        let AuthoringDescription::Tool(tool) = exact.authoring() else {
            panic!("{types:?}: a Tool path describes a Tool");
        };
        assert_eq!(tool.call_path().as_str(), ISSUES_LIST);
        declarations.push(tool.typescript_declaration().to_string());
    }

    let [fitted, repository] = declarations.as_slice() else {
        unreachable!();
    };
    assert!(fitted.contains("export default function ("), "{fitted}");
    assert!(
        fitted.contains(r#"describe this path with types: ["Repository"]"#),
        "{fitted}"
    );
    // The stub names fields; the full type quotes and types them.
    let full = r#"type Repository = { "field_000_url"?: string;"#;
    assert!(!fitted.contains(full), "{fitted}");
    assert!(repository.starts_with(full), "{repository}");
    assert!(
        !repository.contains("export default function ("),
        "{repository}"
    );
}

/// A rejected `types` changes nothing: an undeclared name names itself and
/// what the declaration declares, and a list over its limits is invalid input.
#[tokio::test]
async fn describe_rejects_undeclared_or_oversized_types_and_changes_nothing() {
    let fixture = TestModules::new().await;
    let scope = TracerScope::new(&fixture).await;
    github(&fixture, &scope).await;
    let before = describe(&fixture, &scope, &[]).await.unwrap();
    let too_many = vec!["Repository"; crate::DESCRIBE_TYPES_MAX + 1];
    let too_long = "R".repeat(crate::DESCRIBE_TYPE_NAME_MAX_BYTES + 1);

    for (story, types) in [
        ("an undeclared name", vec!["Repository", "Missing"]),
        ("too many names", too_many),
        ("a name too long", vec![too_long.as_str()]),
    ] {
        let rejected = describe(&fixture, &scope, &types).await;
        match (story, rejected) {
            (
                "an undeclared name",
                Err(Error::UndeclaredTypes {
                    undeclared,
                    declared,
                }),
            ) => {
                assert_eq!(undeclared, ["Missing"]);
                assert_eq!(declared, ["Issue", "Repository", "HttpResult"]);
            }
            ("too many names" | "a name too long", Err(Error::TypesLimit)) => {}
            (story, rejected) => panic!("{story}: {rejected:?}"),
        }
        assert_eq!(
            describe(&fixture, &scope, &[]).await.unwrap(),
            before,
            "{story}"
        );
    }
}

/// The caller already has a Saved Function's source from describing it, so a
/// description cut to `types` leaves it out.
#[tokio::test]
async fn a_saved_function_described_for_types_leaves_out_its_source() {
    let fixture = TestModules::new().await;
    let scope = TracerScope::new(&fixture).await;
    let path = CallPath::parse("coral.functions.internal.formatForecast").unwrap();
    let describe = |types: Vec<String>| {
        let (fixture, scope, path) = (&fixture, &scope, &path);
        async move {
            fixture
                .search
                .describe(&scope.principal, &scope.workspace_id, path, &types)
                .await
                .unwrap()
        }
    };

    let whole = describe(Vec::new()).await;
    let forecast = describe(vec!["Forecast".to_string()]).await;

    assert!(whole.saved_function_detail().is_some());
    assert!(forecast.saved_function_detail().is_none());
    assert!(
        forecast
            .authoring()
            .typescript_declaration()
            .starts_with("interface Forecast"),
        "{}",
        forecast.authoring().typescript_declaration()
    );
}
