use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::naming::{pluralize, singularize};
use super::test_support::github_openapi;
use super::*;
use crate::{PaginationMode, parse_source_manifest_yaml};

#[test]
fn imports_and_generates_github_issue_slice() {
    let manifest = parse_source_manifest_yaml(
        r#"
name: github
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    inputs:
      GITHUB_API_BASE:
        kind: variable
        default: https://api.github.com
    base_url: "{{input.GITHUB_API_BASE}}"
"#,
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let ir = import_openapi_surface(v4, surface, github_openapi().as_bytes()).expect("import");
    let catalog = generate_projection_catalog(v4, &[ir]).expect("catalog");
    let published = catalog
        .projections
        .iter()
        .filter(|projection| projection.visibility == ProjectionVisibility::Published)
        .map(|projection| projection.name.as_str())
        .collect::<Vec<_>>();
    assert!(published.contains(&"issues"), "{published:?}");
    assert!(published.contains(&"search_issues"), "{published:?}");
    assert!(published.contains(&"get_issue"), "{published:?}");
}

#[test]
fn projection_generation_keeps_pagination_inputs_internal() {
    let manifest = parse_source_manifest_yaml(
        r"
name: github
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.github.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let ir = import_openapi_surface(v4, surface, github_openapi().as_bytes()).expect("import");
    let catalog = generate_projection_catalog(v4, std::slice::from_ref(&ir)).expect("catalog");
    let projection = catalog
        .projections
        .iter()
        .find(|projection| projection.operation_id == "issues_list_for_repo")
        .expect("repo issues projection");
    let operation = ir
        .operations
        .iter()
        .find(|operation| operation.id == projection.operation_id)
        .expect("repo issues operation");

    assert_eq!(projection.pagination.mode, PaginationMode::Page);
    assert_eq!(projection.pagination.page_param.as_deref(), Some("page"));
    assert_eq!(
        projection
            .pagination
            .page_size
            .as_ref()
            .and_then(|page_size| page_size.query_param.as_deref()),
        Some("per_page")
    );

    let exposures = projection
        .inputs
        .iter()
        .map(|input| (input.wire_name.as_str(), input.sql_exposure))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(exposures.get("owner"), Some(&SqlInputExposure::Filter));
    assert_eq!(exposures.get("repo"), Some(&SqlInputExposure::Filter));
    assert_eq!(exposures.get("state"), Some(&SqlInputExposure::Filter));
    assert_eq!(exposures.get("page"), Some(&SqlInputExposure::Internal));
    assert_eq!(exposures.get("per_page"), Some(&SqlInputExposure::Internal));

    let filter_names = projection_filter_specs(projection)
        .into_iter()
        .map(|filter| filter.name)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        filter_names,
        BTreeSet::from(["owner".to_string(), "repo".to_string(), "state".to_string()])
    );

    let column_names = projection_column_specs(projection)
        .into_iter()
        .map(|column| column.name)
        .collect::<BTreeSet<_>>();
    assert!(column_names.contains("owner"));
    assert!(column_names.contains("repo"));
    assert!(column_names.contains("state"));
    assert!(!column_names.contains("page"));
    assert!(!column_names.contains("per_page"));

    let request = request_spec_for_projection(projection, operation).expect("request");
    let query_names = request
        .query
        .iter()
        .map(|param| param.name.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(query_names, BTreeSet::from(["state"]));

    let mut stale_projection = projection.clone();
    for input in &mut stale_projection.inputs {
        if matches!(input.wire_name.as_str(), "page" | "per_page") {
            input.sql_exposure = SqlInputExposure::Filter;
        }
    }
    let stale_filter_names = projection_filter_specs(&stale_projection)
        .into_iter()
        .map(|filter| filter.name)
        .collect::<BTreeSet<_>>();
    assert_eq!(stale_filter_names, filter_names);

    let stale_request =
        request_spec_for_projection(&stale_projection, operation).expect("stale request");
    let stale_query_names = stale_request
        .query
        .iter()
        .map(|param| param.name.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(stale_query_names, query_names);

    for input in &mut stale_projection.inputs {
        if matches!(input.wire_name.as_str(), "page" | "per_page") {
            input.sql_exposure = SqlInputExposure::FunctionArg;
        }
    }
    let stale_arg_names = projection_arg_specs(&stale_projection)
        .into_iter()
        .map(|arg| arg.name)
        .collect::<BTreeSet<_>>();
    assert!(!stale_arg_names.contains("page"));
    assert!(!stale_arg_names.contains("per_page"));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "The OpenAPI fixture keeps related collision cases together."
)]
fn projection_names_use_path_context_for_collisions() {
    let manifest = parse_source_manifest_yaml(
        r"
name: github
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: https://api.github.com
",
    )
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let ir = import_openapi_surface(
        v4,
        surface,
        r"
openapi: 3.0.3
paths:
  /issues:
    get:
      operationId: issues/list
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/Issue'}}
  /orgs/{org}/issues:
    get:
      operationId: issues/list-for-org
      parameters:
        - {name: org, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/Issue'}}
  /repos/{owner}/{repo}/issues:
    get:
      operationId: issues/list-for-repo
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/Issue'}}
  /repos/{owner}/{repo}/pulls:
    get:
      operationId: pulls/list
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/PullRequestSimple'}}
  /repos/{owner}/{repo}/commits:
    get:
      operationId: repos/list-commits
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/Commit'}}
  /repos/{owner}/{repo}/pulls/{pull_number}/commits:
    get:
      operationId: pulls/list-commits
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
        - {name: pull_number, in: path, required: true, schema: {type: integer}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {$ref: '#/components/schemas/Commit'}}
components:
  schemas:
    Issue:
      type: object
      properties:
        id: {type: integer}
    PullRequestSimple:
      type: object
      properties:
        id: {type: integer}
    Commit:
      type: object
      properties:
        sha: {type: string}
"
        .as_bytes(),
    )
    .expect("import");
    let catalog = generate_projection_catalog(v4, &[ir]).expect("catalog");
    let names_by_operation = catalog
        .projections
        .iter()
        .map(|projection| {
            (
                projection.operation_id.as_str(),
                (projection.name.as_str(), &projection.kind),
            )
        })
        .collect::<HashMap<_, _>>();

    let issues_list = names_by_operation
        .get("issues_list")
        .expect("issues_list projection");
    assert_eq!(issues_list.0, "issues");
    let org_issues = names_by_operation
        .get("issues_list_for_org")
        .expect("issues_list_for_org projection");
    assert_eq!(org_issues.0, "orgs_issues");
    let repo_issues = names_by_operation
        .get("issues_list_for_repo")
        .expect("issues_list_for_repo projection");
    assert_eq!(repo_issues.0, "repos_issues");
    let pulls = names_by_operation
        .get("pulls_list")
        .expect("pulls_list projection");
    assert_eq!(pulls.0, "pull_requests");
    assert!(matches!(pulls.1, ProjectionKind::Table));
    let commits = names_by_operation
        .get("repos_list_commits")
        .expect("repos_list_commits projection");
    assert_eq!(commits.0, "commits");
    let pull_commits = names_by_operation
        .get("pulls_list_commits")
        .expect("pulls_list_commits projection");
    assert_eq!(pull_commits.0, "repos_pulls_commits");

    let catalog_collision_diagnostics = catalog
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.code == "PROJECTION_NAME_COLLISION_RESOLVED")
        .collect::<Vec<_>>();
    assert_eq!(catalog_collision_diagnostics.len(), 3);
    let projection_collision_diagnostics = catalog
        .projections
        .iter()
        .flat_map(|projection| &projection.diagnostics)
        .filter(|diagnostic| diagnostic.code == "PROJECTION_NAME_COLLISION_RESOLVED")
        .count();
    assert_eq!(
        projection_collision_diagnostics,
        catalog_collision_diagnostics.len()
    );
}

#[test]
fn projection_names_avoid_obvious_bad_singulars() {
    assert_eq!(singularize("status"), "status");
    assert_eq!(singularize("news"), "news");
    assert_eq!(singularize("analytics"), "analytics");
    assert_eq!(singularize("addresses"), "address");
    assert_eq!(pluralize("box"), "boxes");
}
