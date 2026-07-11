use coral_api::v1::{
    AddIdentitySpecRequest, CreateWorkspaceRequest, DeleteIdentitySpecRequest,
    GetIdentitySpecRequest, IdentitySpec, IdentitySpecInputValue, ListIdentitySpecsRequest,
    Workspace,
};
use coral_api::{
    CORAL_ERROR_REASON_IDENTITY_SPEC_NOT_FOUND, CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND,
};
use coral_client::{
    AppClient, IdentitySpecClient,
    local::{RunningServer, ServerBuilder},
};
use tempfile::TempDir;
use tonic::{Code, Request, Status};
use tonic_types::{ErrorDetail, StatusExt as _};

fn workspace(name: &str) -> Workspace {
    Workspace {
        name: name.to_string(),
    }
}

fn oauth_manifest(name: &str, label: &str) -> String {
    format!(
        "kind: identity\nspec_version: 1\nname: {name}\nversion: {label}\ndescription: OAuth {label}\nissuer: issuer_{label}\ntype: oauth\ninputs:\n  TENANT:\n    kind: variable\n    default: tenant-{label}\n  CLIENT_SECRET:\n    kind: secret\n    required: true\noauth:\n  method:\n    flow:\n      type: authorization_code\n      pkce: disabled\n    redirect_uri: http://127.0.0.1:53682/oauth/callback\n    endpoints:\n      authorization_url: https://provider.example.com/authorize\n      token_url: https://provider.example.com/token\n    client:\n      id:\n        input: TENANT\n      secret:\n        input: CLIENT_SECRET\n        transport: basic_auth\n"
    )
}

async fn add_spec(
    client: &mut IdentitySpecClient,
    manifest_yaml: String,
    secret: &str,
    workspace: Option<Workspace>,
) -> coral_api::v1::AddIdentitySpecResponse {
    client
        .add_identity_spec(Request::new(AddIdentitySpecRequest {
            manifest_yaml,
            input_values: vec![IdentitySpecInputValue {
                key: "CLIENT_SECRET".to_string(),
                value: secret.to_string(),
            }],
            workspace,
        }))
        .await
        .expect("add identity spec")
        .into_inner()
}

fn assert_spec(spec: &IdentitySpec, manifest_yaml: &str, label: &str, scope: Option<&str>) {
    assert_eq!(spec.name, "shared");
    assert_eq!(spec.version, label);
    assert_eq!(spec.description, format!("OAuth {label}"));
    assert_eq!(spec.issuer, format!("issuer_{label}"));
    assert_eq!(spec.identity_type, "oauth");
    assert_eq!(spec.manifest_yaml, manifest_yaml);
    assert_eq!(
        spec.workspace.as_ref().map(|value| value.name.as_str()),
        scope
    );
}

fn assert_error_reason(status: &Status, expected: &str) {
    let reason = status
        .get_error_details_vec()
        .into_iter()
        .find_map(|detail| match detail {
            ErrorDetail::ErrorInfo(info) => Some(info.reason),
            _ => None,
        });
    assert_eq!(reason.as_deref(), Some(expected));
}

async fn start_server(config_dir: &std::path::Path) -> (RunningServer, AppClient) {
    let server = ServerBuilder::new()
        .with_config_dir(config_dir)
        .start()
        .await
        .expect("start server");
    let app = AppClient::connect(server.endpoint_uri())
        .await
        .expect("connect client");
    (server, app)
}

async fn seed_scoped_specs(
    config_dir: &std::path::Path,
    work: &Workspace,
    global_v1: &str,
    workspace_v1: &str,
) {
    let (server, app) = start_server(config_dir).await;
    app.workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(work.clone()),
        }))
        .await
        .expect("create workspace");
    let global = add_spec(
        &mut app.identity_spec_client(),
        global_v1.to_string(),
        "global-secret",
        None,
    )
    .await;
    assert!(!global.replaced);
    assert_spec(
        global.identity_spec.as_ref().expect("global spec"),
        global_v1,
        "global_v1",
        None,
    );
    let scoped = add_spec(
        &mut app.identity_spec_client(),
        workspace_v1.to_string(),
        "workspace-secret",
        Some(work.clone()),
    )
    .await;
    assert!(!scoped.replaced);
    assert_spec(
        scoped.identity_spec.as_ref().expect("workspace spec"),
        workspace_v1,
        "workspace_v1",
        Some("work"),
    );
    server.shutdown().await.expect("shutdown first server");
}

async fn assert_replacement_and_lists(
    client: &mut IdentitySpecClient,
    work: &Workspace,
    global_v2: &str,
    workspace_v1: &str,
) {
    let replaced = add_spec(client, global_v2.to_string(), "  ", None).await;
    assert!(replaced.replaced);
    assert_spec(
        replaced.identity_spec.as_ref().expect("replaced spec"),
        global_v2,
        "global_v2",
        None,
    );

    let globals = client
        .list_identity_specs(Request::new(ListIdentitySpecsRequest::default()))
        .await
        .expect("list global specs")
        .into_inner()
        .identity_specs;
    assert_eq!(globals.len(), 1);
    assert_spec(
        globals.first().expect("one global spec"),
        global_v2,
        "global_v2",
        None,
    );

    let exact_global = client
        .get_identity_spec(Request::new(GetIdentitySpecRequest {
            name: "shared".to_string(),
            workspace: None,
        }))
        .await
        .expect("get exact global spec")
        .into_inner()
        .identity_spec
        .expect("global spec");
    assert_spec(&exact_global, global_v2, "global_v2", None);

    let workspace_only = client
        .list_identity_specs(Request::new(ListIdentitySpecsRequest {
            workspace: Some(work.clone()),
            include_global: false,
        }))
        .await
        .expect("list workspace specs")
        .into_inner()
        .identity_specs;
    assert_eq!(workspace_only.len(), 1);
    assert_spec(
        workspace_only.first().expect("one workspace spec"),
        workspace_v1,
        "workspace_v1",
        Some("work"),
    );

    let combined = client
        .list_identity_specs(Request::new(ListIdentitySpecsRequest {
            workspace: Some(work.clone()),
            include_global: true,
        }))
        .await
        .expect("list workspace and global specs")
        .into_inner()
        .identity_specs;
    assert_eq!(combined.len(), 2);
    assert_spec(
        combined.first().expect("global entry"),
        global_v2,
        "global_v2",
        None,
    );
    assert_spec(
        combined.get(1).expect("workspace entry"),
        workspace_v1,
        "workspace_v1",
        Some("work"),
    );
}

async fn assert_exact_delete_contract(
    client: &mut IdentitySpecClient,
    work: &Workspace,
    workspace_v1: &str,
) {
    let exact = client
        .get_identity_spec(Request::new(GetIdentitySpecRequest {
            name: "shared".to_string(),
            workspace: Some(work.clone()),
        }))
        .await
        .expect("get exact workspace spec")
        .into_inner()
        .identity_spec
        .expect("workspace spec");
    assert_spec(&exact, workspace_v1, "workspace_v1", Some("work"));

    let deleted = client
        .delete_identity_spec(Request::new(DeleteIdentitySpecRequest {
            name: "shared".to_string(),
            workspace: Some(work.clone()),
            force: false,
        }))
        .await
        .expect("delete workspace spec")
        .into_inner();
    assert_eq!(deleted.orphaned_identities, 0);

    let missing_exact = client
        .get_identity_spec(Request::new(GetIdentitySpecRequest {
            name: "shared".to_string(),
            workspace: Some(work.clone()),
        }))
        .await
        .expect_err("workspace lookup must not fall back to global");
    assert_eq!(missing_exact.code(), Code::NotFound);
    assert_error_reason(&missing_exact, CORAL_ERROR_REASON_IDENTITY_SPEC_NOT_FOUND);

    let missing_workspace = client
        .list_identity_specs(Request::new(ListIdentitySpecsRequest {
            workspace: Some(workspace("missing")),
            include_global: true,
        }))
        .await
        .expect_err("missing workspace should fail");
    assert_eq!(missing_workspace.code(), Code::NotFound);
    assert_error_reason(&missing_workspace, CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND);
}

#[tokio::test]
async fn identity_spec_service_preserves_exact_scopes_and_inputs_across_restart() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let work = workspace("work");
    let global_v1 = oauth_manifest("shared", "global_v1");
    let global_v2 = oauth_manifest("shared", "global_v2");
    let workspace_v1 = oauth_manifest("shared", "workspace_v1");

    seed_scoped_specs(&config_dir, &work, &global_v1, &workspace_v1).await;
    let (server, app) = start_server(&config_dir).await;
    let mut client = app.identity_spec_client();
    assert_replacement_and_lists(&mut client, &work, &global_v2, &workspace_v1).await;
    assert_exact_delete_contract(&mut client, &work, &workspace_v1).await;

    server.shutdown().await.expect("shutdown restarted server");
}
