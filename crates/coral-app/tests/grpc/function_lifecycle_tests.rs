use coral_api::v1::{
    AddFunctionRequest, CreateWorkspaceRequest, DeleteFunctionRequest, ListFunctionsRequest,
    Workspace, function,
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
guide: Use this function to echo a typed value.
*/

{body}
"
    )
}

#[tokio::test]
async fn function_lifecycle_is_scoped_to_the_selected_workspace() {
    let harness = GrpcHarness::new().await;
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
    assert_eq!(
        ready.table_function.expect("table function").guide,
        "Use this function to echo a typed value."
    );

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
    let Some(function::Runtime::Ready(ready)) = listed.runtime else {
        panic!("expected listed runtime-ready function");
    };
    assert_eq!(ready.sql_body, sql_body);

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
