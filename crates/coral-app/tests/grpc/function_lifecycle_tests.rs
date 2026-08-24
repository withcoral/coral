use std::fs;

use coral_api::v1::{
    AddFunctionRequest, CreateWorkspaceRequest, DeleteFunctionRequest, FunctionWriteSurface,
    ListFunctionsRequest, Workspace, function,
};
use coral_client::default_workspace;
use tonic::Request;

use crate::harness::{GrpcHarness, fixture_manifest_yaml};

fn workspace(name: &str) -> Workspace {
    Workspace {
        name: name.to_string(),
    }
}

fn function_sql(body: &str) -> String {
    format!(
        r"/*
name: echo_value
schema: functions
description: Echo one value
guide: Use this function to echo a typed value.
*/

{body}
"
    )
}

#[tokio::test]
async fn function_lifecycle_is_scoped_to_the_selected_workspace() {
    let harness = GrpcHarness::with_workspace().await;
    let work = workspace("work");
    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(work.clone()),
        }))
        .await
        .expect("create workspace");

    let sql_body = "select cast($value as VARCHAR) as value";
    let sql = function_sql(sql_body);
    let added = harness
        .function_client()
        .add_function(Request::new(AddFunctionRequest {
            workspace: Some(work.clone()),
            sql: sql.clone(),
            fail_if_exists: false,
            write_surface: FunctionWriteSurface::Mcp as i32,
        }))
        .await
        .expect("add function")
        .into_inner()
        .function
        .expect("added function");
    assert_eq!(added.name, "echo_value");
    assert_eq!(added.workspace.as_ref(), Some(&work));
    let Some(function::Runtime::Ready(ready)) = added.runtime else {
        panic!("expected runtime-ready function");
    };
    assert_eq!(ready.sql_body, sql_body);
    assert!(ready.source_names.is_empty());
    assert_eq!(
        ready.table_function.expect("table function").guide,
        "Use this function to echo a typed value."
    );
    assert_eq!(added.write_surface, FunctionWriteSurface::Mcp as i32);

    let default_functions = harness
        .function_client()
        .list_functions(Request::new(ListFunctionsRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect("list default functions")
        .into_inner()
        .functions;
    assert!(default_functions.is_empty());

    let work_functions = harness
        .function_client()
        .list_functions(Request::new(ListFunctionsRequest {
            workspace: Some(work.clone()),
        }))
        .await
        .expect("list work functions")
        .into_inner()
        .functions;
    assert_eq!(work_functions.len(), 1);
    let listed = work_functions.into_iter().next().expect("listed function");
    assert_eq!(listed.write_surface, FunctionWriteSurface::Mcp as i32);
    let Some(function::Runtime::Ready(ready)) = listed.runtime else {
        panic!("expected listed runtime-ready function");
    };
    assert_eq!(ready.sql_body, sql_body);
    assert!(ready.source_names.is_empty());
    let config_raw =
        fs::read_to_string(harness.config_dir().join("config.toml")).expect("read config");
    assert!(config_raw.contains("write_surface = \"mcp\""));

    harness
        .function_client()
        .delete_function(Request::new(DeleteFunctionRequest {
            workspace: Some(work.clone()),
            name: "echo_value".to_string(),
        }))
        .await
        .expect("delete function");

    let remaining = harness
        .function_client()
        .list_functions(Request::new(ListFunctionsRequest {
            workspace: Some(work),
        }))
        .await
        .expect("list functions after delete")
        .into_inner()
        .functions;
    assert!(remaining.is_empty());
}

#[tokio::test]
async fn function_sources_are_returned_when_added_and_listed() {
    let harness = GrpcHarness::with_workspace().await;
    harness
        .import_source(
            fixture_manifest_yaml(harness.temp_path()),
            Vec::new(),
            Vec::new(),
        )
        .await;

    let added = harness
        .function_client()
        .add_function(Request::new(AddFunctionRequest {
            workspace: Some(default_workspace()),
            sql: function_sql(r#"select "sessionId" as session_id from local_messages.messages"#),
            fail_if_exists: false,
            write_surface: 0,
        }))
        .await
        .expect("add function")
        .into_inner()
        .function
        .expect("added function");
    let Some(function::Runtime::Ready(ready)) = added.runtime else {
        panic!("expected runtime-ready function");
    };
    assert_eq!(ready.source_names, ["local_messages"]);

    let listed = harness
        .function_client()
        .list_functions(Request::new(ListFunctionsRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect("list functions")
        .into_inner()
        .functions;
    let listed = listed.into_iter().next().expect("listed function");
    let Some(function::Runtime::Ready(ready)) = listed.runtime else {
        panic!("expected listed runtime-ready function");
    };
    assert_eq!(ready.source_names, ["local_messages"]);
}

#[tokio::test]
async fn untyped_function_is_not_persisted() {
    let harness = GrpcHarness::with_workspace().await;

    let error = harness
        .function_client()
        .add_function(Request::new(AddFunctionRequest {
            workspace: Some(default_workspace()),
            sql: function_sql("select $value as value"),
            fail_if_exists: false,
            write_surface: 0,
        }))
        .await
        .expect_err("untyped function should fail");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);

    let functions = harness
        .function_client()
        .list_functions(Request::new(ListFunctionsRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect("list functions")
        .into_inner()
        .functions;
    assert!(functions.is_empty());
    assert!(
        !harness
            .config_dir()
            .join("workspaces/default/functions/echo_value")
            .exists()
    );
}

#[tokio::test]
async fn typescript_function_is_rejected_without_persistence() {
    let harness = GrpcHarness::with_workspace().await;
    let artifact = r"/*
name: review_summary
schema: functions
description: Summarize a review queue.
language: typescript
signature:
  arguments:
    - name: owner
      data_type: Utf8
  result_columns:
    - name: title
      data_type: Utf8
*/

export async function run(owner: string): Promise<string> {
  return `queue for ${owner}`;
}
";

    let error = harness
        .function_client()
        .add_function(Request::new(AddFunctionRequest {
            workspace: Some(default_workspace()),
            sql: artifact.to_string(),
            fail_if_exists: false,
            write_surface: FunctionWriteSurface::Cli as i32,
        }))
        .await
        .expect_err("TypeScript function should be rejected");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error
            .message()
            .contains("no TypeScript executor is available")
    );
    let functions = harness
        .function_client()
        .list_functions(Request::new(ListFunctionsRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect("list functions")
        .into_inner()
        .functions;
    assert!(functions.is_empty());
    let config_raw =
        fs::read_to_string(harness.config_dir().join("config.toml")).expect("read config");
    assert!(!config_raw.contains("review_summary"));
    assert!(
        !harness
            .config_dir()
            .join("workspaces/default/functions/review_summary")
            .exists()
    );
}

#[tokio::test]
async fn create_only_preserves_an_existing_function_and_legacy_add_replaces_it() {
    let harness = GrpcHarness::with_workspace().await;
    let workspace = default_workspace();
    let original = function_sql("select cast($value as VARCHAR) as value");
    let added = harness
        .function_client()
        .add_function(Request::new(AddFunctionRequest {
            workspace: Some(workspace.clone()),
            sql: original.clone(),
            fail_if_exists: false,
            write_surface: FunctionWriteSurface::Mcp as i32,
        }))
        .await
        .expect("add original function")
        .into_inner();
    assert!(!added.replaced);

    let replacement = original
        .replace("Echo one value", "Replacement function")
        .replace(" as value", " as replacement");
    let error = harness
        .function_client()
        .add_function(Request::new(AddFunctionRequest {
            workspace: Some(workspace.clone()),
            sql: replacement.clone(),
            fail_if_exists: true,
            write_surface: FunctionWriteSurface::Cli as i32,
        }))
        .await
        .expect_err("create-only add should reject an existing function");
    assert_eq!(error.code(), tonic::Code::AlreadyExists);

    let functions = harness
        .function_client()
        .list_functions(Request::new(ListFunctionsRequest {
            workspace: Some(workspace.clone()),
        }))
        .await
        .expect("list preserved function")
        .into_inner()
        .functions;
    let ready = match functions
        .first()
        .and_then(|function| function.runtime.as_ref())
    {
        Some(function::Runtime::Ready(ready)) => ready,
        runtime => panic!("expected ready function, got {runtime:?}"),
    };
    assert_eq!(ready.description, "Echo one value");
    assert_eq!(
        ready
            .result_columns
            .first()
            .expect("original result column")
            .name,
        "value"
    );

    let replaced = harness
        .function_client()
        .add_function(Request::new(AddFunctionRequest {
            workspace: Some(workspace),
            sql: replacement,
            fail_if_exists: false,
            write_surface: FunctionWriteSurface::Cli as i32,
        }))
        .await
        .expect("legacy add should replace an existing function")
        .into_inner();
    assert!(replaced.replaced);
    let replacement = replaced.function.expect("replacement function");
    assert_eq!(replacement.write_surface, FunctionWriteSurface::Cli as i32);
    let ready = match replacement.runtime {
        Some(function::Runtime::Ready(ready)) => ready,
        runtime => panic!("expected ready replacement, got {runtime:?}"),
    };
    assert_eq!(ready.description, "Replacement function");
    assert_eq!(
        ready
            .result_columns
            .first()
            .expect("replacement result column")
            .name,
        "replacement"
    );
}
