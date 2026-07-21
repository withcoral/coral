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

    let added = harness
        .function_client()
        .add_function(Request::new(AddFunctionRequest {
            workspace: Some(work.clone()),
            sql: function_sql("select cast($value as VARCHAR) as value"),
        }))
        .await
        .expect("add function")
        .into_inner()
        .function
        .expect("added function");
    assert_eq!(added.name, "echo_value");
    assert_eq!(added.workspace.as_ref(), Some(&work));
    assert!(matches!(added.runtime, Some(function::Runtime::Ready(_))));

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
    let config =
        std::fs::read_to_string(harness.config_dir().join("config.toml")).expect("read config");
    assert!(!config.contains("functions"));

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
async fn installed_function_survives_server_restart() {
    let temp = tempfile::tempdir().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let harness = GrpcHarness::start_with_config_dir(config_dir.clone()).await;
    harness
        .function_client()
        .add_function(Request::new(AddFunctionRequest {
            workspace: Some(default_workspace()),
            sql: function_sql("select cast($value as VARCHAR) as value"),
        }))
        .await
        .expect("add function");
    drop(harness);

    let restarted = GrpcHarness::start_with_config_dir(config_dir).await;
    let functions = restarted
        .function_client()
        .list_functions(Request::new(ListFunctionsRequest {
            workspace: Some(default_workspace()),
        }))
        .await
        .expect("list functions after restart")
        .into_inner()
        .functions;

    assert_eq!(functions.len(), 1);
    assert_eq!(functions.first().expect("one function").name, "echo_value");
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
}
