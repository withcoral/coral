use coral_api::v1::{
    AddIdentitySpecRequest, AddIdentitySpecResponse, CreateWorkspaceRequest,
    DeleteIdentitySpecRequest, GetIdentitySpecRequest, GlobalIdentitySpecScope, IdentitySpec,
    IdentitySpecInputValue, IdentitySpecScope, ListIdentitySpecsRequest, Workspace,
    identity_spec_scope,
};
use tonic::{Code, Request};

use crate::harness::GrpcHarness;

const NAME: &str = "demo_oauth";
const GLOBAL_TENANT: &str = "global-tenant-value";
const GLOBAL_SECRET: &str = "global-secret-value";
const WORKSPACE_TENANT: &str = "workspace-tenant-value";
const WORKSPACE_SECRET: &str = "workspace-secret-value";
const REPLACEMENT_TENANT: &str = "replacement-tenant-value";
const REPLACEMENT_SECRET: &str = "replacement-secret-value";
const SUPPLIED_VALUES: [&str; 6] = [
    GLOBAL_TENANT,
    GLOBAL_SECRET,
    WORKSPACE_TENANT,
    WORKSPACE_SECRET,
    REPLACEMENT_TENANT,
    REPLACEMENT_SECRET,
];

#[tokio::test]
async fn manages_identity_specs_in_exact_global_and_workspace_scopes() {
    let harness = GrpcHarness::new().await;
    let workspace = Workspace {
        name: "team".to_string(),
    };
    harness
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(workspace.clone()),
        }))
        .await
        .expect("create workspace");
    let global_scope = global_scope();
    let workspace_scope = workspace_scope(workspace);

    let global_add = add(
        &harness,
        "global-v1",
        global_scope.clone(),
        [GLOBAL_TENANT, GLOBAL_SECRET],
    )
    .await;
    assert!(!global_add.replaced);
    assert_spec(
        global_add.identity_spec.as_ref(),
        "global-v1",
        &global_scope,
    );
    assert_no_supplied_values(&global_add);

    let workspace_add = add(
        &harness,
        "workspace-v1",
        workspace_scope.clone(),
        [WORKSPACE_TENANT, WORKSPACE_SECRET],
    )
    .await;
    assert!(!workspace_add.replaced);
    assert_spec(
        workspace_add.identity_spec.as_ref(),
        "workspace-v1",
        &workspace_scope,
    );
    assert_no_supplied_values(&workspace_add);

    let global_get = get(&harness, global_scope.clone()).await;
    assert_spec(Some(&global_get), "global-v1", &global_scope);
    assert_no_supplied_values(&global_get);
    let workspace_get = get(&harness, workspace_scope.clone()).await;
    assert_spec(Some(&workspace_get), "workspace-v1", &workspace_scope);
    assert_no_supplied_values(&workspace_get);

    assert_combined_list(&harness, &workspace_scope, &global_scope).await;

    let replacement = add(
        &harness,
        "workspace-v2",
        workspace_scope.clone(),
        [REPLACEMENT_TENANT, REPLACEMENT_SECRET],
    )
    .await;
    assert!(replacement.replaced);
    assert_spec(
        replacement.identity_spec.as_ref(),
        "workspace-v2",
        &workspace_scope,
    );
    assert_no_supplied_values(&replacement);
    assert_spec(
        Some(&get(&harness, global_scope.clone()).await),
        "global-v1",
        &global_scope,
    );

    harness
        .identity_spec_client()
        .delete_identity_spec(Request::new(DeleteIdentitySpecRequest {
            name: NAME.to_string(),
            scope: Some(workspace_scope.clone()),
        }))
        .await
        .expect("delete exact workspace identity spec");
    let deleted = harness
        .identity_spec_client()
        .get_identity_spec(Request::new(GetIdentitySpecRequest {
            name: NAME.to_string(),
            scope: Some(workspace_scope),
        }))
        .await
        .expect_err("deleted workspace identity spec should be absent");
    assert_eq!(deleted.code(), Code::NotFound);
    assert_spec(
        Some(&get(&harness, global_scope.clone()).await),
        "global-v1",
        &global_scope,
    );

    assert_invalid_list_requests(&harness, &global_scope).await;
}

async fn assert_combined_list(
    harness: &GrpcHarness,
    workspace_scope: &IdentitySpecScope,
    global_scope: &IdentitySpecScope,
) {
    let combined = harness
        .identity_spec_client()
        .list_identity_specs(Request::new(ListIdentitySpecsRequest {
            scope: Some(workspace_scope.clone()),
            include_global: true,
        }))
        .await
        .expect("list workspace and global identity specs")
        .into_inner();
    assert_eq!(combined.identity_specs.len(), 2);
    let global_summary = combined.identity_specs.first().expect("global summary");
    assert_eq!(global_summary.name, NAME);
    assert_eq!(global_summary.version, "global-v1");
    assert_eq!(global_summary.scope.as_ref(), Some(global_scope));
    let workspace_summary = combined.identity_specs.get(1).expect("workspace summary");
    assert_eq!(workspace_summary.name, NAME);
    assert_eq!(workspace_summary.version, "workspace-v1");
    assert_eq!(workspace_summary.scope.as_ref(), Some(workspace_scope));
    assert_no_supplied_values(&combined);
}

async fn assert_invalid_list_requests(harness: &GrpcHarness, global_scope: &IdentitySpecScope) {
    for (request, reason) in [
        (
            ListIdentitySpecsRequest {
                scope: None,
                include_global: false,
            },
            "missing identity spec scope should fail",
        ),
        (
            ListIdentitySpecsRequest {
                scope: Some(global_scope.clone()),
                include_global: true,
            },
            "global scope cannot include global fallback",
        ),
    ] {
        let error = harness
            .identity_spec_client()
            .list_identity_specs(Request::new(request))
            .await
            .expect_err(reason);
        assert_eq!(error.code(), Code::InvalidArgument);
    }
}

async fn add(
    harness: &GrpcHarness,
    version: &str,
    scope: IdentitySpecScope,
    values: [&str; 2],
) -> AddIdentitySpecResponse {
    harness
        .identity_spec_client()
        .add_identity_spec(Request::new(AddIdentitySpecRequest {
            manifest_yaml: oauth_manifest(version),
            input_values: ["TENANT", "CLIENT_SECRET"]
                .into_iter()
                .zip(values)
                .map(|(key, value)| IdentitySpecInputValue {
                    key: key.to_string(),
                    value: value.to_string(),
                })
                .collect(),
            scope: Some(scope),
        }))
        .await
        .expect("add identity spec")
        .into_inner()
}

async fn get(harness: &GrpcHarness, scope: IdentitySpecScope) -> IdentitySpec {
    harness
        .identity_spec_client()
        .get_identity_spec(Request::new(GetIdentitySpecRequest {
            name: NAME.to_string(),
            scope: Some(scope),
        }))
        .await
        .expect("get identity spec")
        .into_inner()
        .identity_spec
        .expect("identity spec response")
}

fn assert_spec(spec: Option<&IdentitySpec>, version: &str, scope: &IdentitySpecScope) {
    let spec = spec.expect("identity spec");
    assert_eq!(spec.name, NAME);
    assert_eq!(spec.version, version);
    assert_eq!(spec.scope.as_ref(), Some(scope));
    assert!(spec.manifest_yaml.contains("port: 443"));
}

fn assert_no_supplied_values(response: &impl std::fmt::Debug) {
    let response = format!("{response:?}");
    for value in SUPPLIED_VALUES {
        assert!(
            !response.contains(value),
            "identity-spec response leaked supplied input value"
        );
    }
}

fn global_scope() -> IdentitySpecScope {
    IdentitySpecScope {
        value: Some(identity_spec_scope::Value::Global(
            GlobalIdentitySpecScope {},
        )),
    }
}

fn workspace_scope(workspace: Workspace) -> IdentitySpecScope {
    IdentitySpecScope {
        value: Some(identity_spec_scope::Value::Workspace(workspace)),
    }
}

fn oauth_manifest(version: &str) -> String {
    format!(
        "kind: identity\nspec_version: 1\nname: {NAME}\nversion: {version}\ndescription: test identity {version}\nissuer: demo\ntype: oauth\naudience: {{host: api.example.com, port: 443}}\ninputs:\n  TENANT: {{kind: variable, required: true}}\n  CLIENT_SECRET: {{kind: secret, required: true}}\noauth:\n  method:\n    flow: {{type: authorization_code, pkce: disabled}}\n    redirect_uri: http://127.0.0.1:53682/oauth/callback\n    endpoints:\n      authorization_url: https://provider.example.com/authorize\n      token_url: https://provider.example.com/token\n    client:\n      id: {{input: TENANT}}\n      secret: {{input: CLIENT_SECRET, transport: basic_auth}}\n"
    )
}
