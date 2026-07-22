use coral_api::v1::{
    AddFunctionRequest, CreateWorkspaceRequest, DeleteFunctionRequest, GetFunctionRequest,
    ListFunctionsRequest, Workspace, function,
};
use coral_client::default_workspace;
use tonic::Request;

use crate::harness::GrpcHarness;

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
*/

{body}
"
    )
}

async fn create_workspace(harness: &GrpcHarness, workspace: &Workspace) {
    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace.clone()),
        }))
        .await
        .expect("create workspace");
}

async fn get_function_sql(harness: &GrpcHarness, workspace: &Workspace) -> String {
    harness
        .function_client()
        .get_function(Request::new(GetFunctionRequest {
            workspace: Some(workspace.clone()),
            name: "echo_value".to_string(),
        }))
        .await
        .expect("get function")
        .into_inner()
        .sql
}

async fn assert_create_only_rejects_existing(
    harness: &GrpcHarness,
    workspace: &Workspace,
    expected_sql: &str,
) {
    let duplicate = harness
        .function_client()
        .add_function(Request::new(AddFunctionRequest {
            fail_if_exists: true,
            workspace: Some(workspace.clone()),
            sql: function_sql("select 'replacement' as value"),
        }))
        .await
        .expect_err("create-only add should reject an existing function");
    assert_eq!(duplicate.code(), tonic::Code::AlreadyExists);

    assert_eq!(get_function_sql(harness, workspace).await, expected_sql);
}

#[tokio::test]
async fn function_lifecycle_is_scoped_to_the_selected_workspace() {
    let harness = GrpcHarness::new().await;
    let work = workspace("work");
    create_workspace(&harness, &work).await;

    let original_sql = function_sql("select cast($value as VARCHAR) as value");
    let added = harness
        .function_client()
        .add_function(Request::new(AddFunctionRequest {
            fail_if_exists: false,
            workspace: Some(work.clone()),
            sql: original_sql.clone(),
        }))
        .await
        .expect("add function")
        .into_inner()
        .function
        .expect("added function");
    assert_eq!(added.name, "echo_value");
    assert_eq!(added.workspace.as_ref(), Some(&work));
    assert!(matches!(added.runtime, Some(function::Runtime::Ready(_))));

    assert_eq!(get_function_sql(&harness, &work).await, original_sql);

    assert_create_only_rejects_existing(&harness, &work, &original_sql).await;

    let missing_from_default = harness
        .function_client()
        .get_function(Request::new(GetFunctionRequest {
            workspace: Some(default_workspace()),
            name: "echo_value".to_string(),
        }))
        .await
        .expect_err("function should stay scoped to work workspace");
    assert_eq!(missing_from_default.code(), tonic::Code::NotFound);

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

    let updated_sql = function_sql("select upper(cast($value as VARCHAR)) as value");
    let updated = harness
        .function_client()
        .add_function(Request::new(AddFunctionRequest {
            fail_if_exists: false,
            workspace: Some(work.clone()),
            sql: updated_sql.clone(),
        }))
        .await
        .expect("replace function")
        .into_inner()
        .function
        .expect("updated function");
    assert_eq!(updated.name, "echo_value");

    assert_eq!(get_function_sql(&harness, &work).await, updated_sql);

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
async fn untyped_function_is_not_persisted() {
    let harness = GrpcHarness::new().await;

    let error = harness
        .function_client()
        .add_function(Request::new(AddFunctionRequest {
            fail_if_exists: false,
            workspace: Some(default_workspace()),
            sql: function_sql("select $value as value"),
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
