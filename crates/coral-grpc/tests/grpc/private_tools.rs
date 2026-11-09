use super::{Fixture, error_reason, management, request};
use coral_tools::{
    PrivateToolChangeContext, PrivateToolDescription, PrivateToolPath, PrivateToolSave,
    PrivateToolSource,
};
use coral_workspace::{AuthenticatedPrincipal, AuthenticatedUser, TraceId};

async fn save(f: &Fixture, user: &AuthenticatedUser, path: &str, source: &str) {
    f.private_tools
        .save(
            &PrivateToolChangeContext::new(
                AuthenticatedPrincipal::user(user.clone()),
                1_700_000_000_000_000_007,
                TraceId::current_or_new(),
            ),
            f.workspace.id(),
            PrivateToolSave::new(
                PrivateToolPath::parse(path).unwrap(),
                PrivateToolDescription::parse(&format!("Describe {path}")).unwrap(),
                PrivateToolSource::parse(source).unwrap(),
            ),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn list_pages_round_trip_as_source_free_summaries_and_bind_their_token() {
    let f = Fixture::new().await;
    for path in ["reports.total", "alpha.first", "zeta.last"] {
        save(&f, &f.owner, path, "export default () => 1;").await;
    }
    let parent = format!("workspaces/{}", f.workspace.id());
    let list = async |user: &AuthenticatedUser, page_token: String| {
        f.private_tools()
            .list_private_tools(request(
                management::ListPrivateToolsRequest {
                    parent: parent.clone(),
                    page_size: 2,
                    page_token,
                },
                &f.token(user, "reef"),
                "reef",
            ))
            .await
            .map(tonic::Response::into_inner)
    };

    let first = list(&f.owner, String::new()).await.unwrap();
    assert_eq!(
        first.private_tools.first(),
        Some(&management::PrivateToolSummary {
            path: "alpha.first".into(),
            description: "Describe alpha.first".into(),
            update_time: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 7,
            }),
        })
    );
    let second = list(&f.owner, first.next_page_token.clone()).await.unwrap();
    let paths = [first.private_tools, second.private_tools]
        .concat()
        .into_iter()
        .map(|summary| summary.path)
        .collect::<Vec<_>>();
    assert_eq!(paths, ["alpha.first", "reports.total", "zeta.last"]);
    assert!(second.next_page_token.is_empty());

    let foreign = list(&f.member, first.next_page_token).await.unwrap_err();
    assert_eq!(
        (foreign.code(), error_reason(&foreign).as_str()),
        (tonic::Code::InvalidArgument, "INVALID_ARGUMENT")
    );
}

#[tokio::test]
async fn get_returns_authoring_material_and_conceals_other_owners_as_not_found() {
    let f = Fixture::new().await;
    save(
        &f,
        &f.owner,
        "reports.total",
        "export default function total(): number { return missing; }",
    )
    .await;
    let get = async |user: &AuthenticatedUser, path: &str, audience: &str| {
        f.private_tools()
            .get_private_tool(request(
                management::GetPrivateToolRequest {
                    parent: format!("workspaces/{}", f.workspace.id()),
                    path: path.into(),
                },
                &f.token(user, audience),
                audience,
            ))
            .await
    };

    let function = get(&f.owner, "reports.total", "reef")
        .await
        .unwrap()
        .into_inner();
    assert_eq!(function.path, "reports.total");
    assert_eq!(function.description, "Describe reports.total");
    assert!(function.source.contains("return missing;"));
    assert!(function.declaration.contains("export default"));
    assert!(matches!(
        function.diagnostics.as_slice(),
        [diagnostic]
            if diagnostic.category() == management::PrivateToolDiagnosticCategory::Error
                && diagnostic.message.contains("missing")
                && diagnostic.line == Some(1)
    ));
    assert!(!function.diagnostics_truncated);

    for (user, path, audience, expected) in [
        (
            &f.member,
            "reports.total",
            "reef",
            (tonic::Code::NotFound, "NOT_FOUND"),
        ),
        (
            &f.owner,
            "reports.absent",
            "reef",
            (tonic::Code::NotFound, "NOT_FOUND"),
        ),
        (
            &f.owner,
            "reports..total",
            "reef",
            (tonic::Code::InvalidArgument, "INVALID_ARGUMENT"),
        ),
        (
            &f.owner,
            "reports.total",
            "mcp",
            (tonic::Code::PermissionDenied, "PERMISSION_DENIED"),
        ),
    ] {
        let status = get(user, path, audience).await.unwrap_err();
        assert_eq!(
            (status.code(), error_reason(&status).as_str()),
            expected,
            "{path} via {audience}"
        );
    }
}

#[tokio::test]
async fn delete_removes_only_the_callers_own_function() {
    let f = Fixture::new().await;
    save(&f, &f.owner, "reports.total", "export default () => 1;").await;
    let parent = format!("workspaces/{}", f.workspace.id());
    let delete = async |user: &AuthenticatedUser, path: &str, audience: &str| {
        f.private_tools()
            .delete_private_tool(request(
                management::DeletePrivateToolRequest {
                    parent: parent.clone(),
                    path: path.into(),
                },
                &f.token(user, audience),
                audience,
            ))
            .await
    };
    let exists = async || {
        f.private_tools
            .get(
                &AuthenticatedPrincipal::user(f.owner.clone()),
                f.workspace.id(),
                &PrivateToolPath::parse("reports.total").unwrap(),
            )
            .await
            .is_ok()
    };

    for (user, path, audience, expected) in [
        (
            &f.member,
            "reports.total",
            "reef",
            (tonic::Code::NotFound, "NOT_FOUND"),
        ),
        (
            &f.owner,
            "reports..total",
            "reef",
            (tonic::Code::InvalidArgument, "INVALID_ARGUMENT"),
        ),
        (
            &f.owner,
            "reports.total",
            "mcp",
            (tonic::Code::PermissionDenied, "PERMISSION_DENIED"),
        ),
    ] {
        let status = delete(user, path, audience).await.unwrap_err();
        assert_eq!(
            (status.code(), error_reason(&status).as_str()),
            expected,
            "{path} via {audience}"
        );
        assert!(exists().await, "a refused delete keeps the function");
    }

    delete(&f.owner, "reports.total", "reef").await.unwrap();
    assert!(!exists().await);
    let again = delete(&f.owner, "reports.total", "reef").await.unwrap_err();
    assert_eq!(
        (again.code(), error_reason(&again).as_str()),
        (tonic::Code::NotFound, "NOT_FOUND")
    );
}

/// Each `ToolAccess` row of `provider` as `user` lists it: (relative path,
/// agent, access state, explicit, tool name), in Tool Key order.
async fn private_tool_access(
    f: &Fixture,
    user: &AuthenticatedUser,
    provider: &str,
) -> Vec<(String, String, i32, bool, String)> {
    f.policies()
        .list_tool_access(request(
            management::ListToolAccessRequest {
                parent: provider.to_owned(),
                page_size: 0,
                page_token: String::new(),
            },
            &f.token(user, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner()
        .tools
        .into_iter()
        .map(|tool| {
            (
                tool.relative_path,
                tool.agent,
                tool.access_state,
                tool.explicit,
                tool.tool,
            )
        })
        .collect()
}

// Catches: a Private Tool listing that does not name the Private Provider; a
// Private Tool policy route that refuses the Private Provider, does not name
// an Agent's row, or maps a Member's Private Tool to anything but NOT_FOUND;
// or a kill switch that answers anything but the Provider it changed.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey covers each route an Owner manages Private Tool policy through"
)]
async fn an_owner_manages_private_tool_policy_through_the_private_provider() {
    let f = Fixture::new().await;
    save(&f, &f.owner, "people.greet", "export default () => 1;").await;
    save(&f, &f.member, "people.greet", "export default () => 2;").await;
    let issued = f
        .workspaces
        .create_agent(
            &super::change(&f.owner),
            f.workspace.id(),
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = f
        .workspaces
        .authenticate_agent(f.workspace.id(), issued.credential.expose(), 2)
        .await
        .unwrap();
    f.private_tools
        .save(
            &PrivateToolChangeContext::new(agent.clone(), 3, TraceId::current_or_new()),
            f.workspace.id(),
            PrivateToolSave::new(
                PrivateToolPath::parse("people.greet").unwrap(),
                PrivateToolDescription::parse("Describe the Agent's").unwrap(),
                PrivateToolSource::parse("export default () => 3;").unwrap(),
            ),
        )
        .await
        .unwrap();

    let provider = f
        .private_tools()
        .list_private_tools(request(
            management::ListPrivateToolsRequest {
                parent: format!("workspaces/{}", f.workspace.id()),
                page_size: 0,
                page_token: String::new(),
            },
            &f.token(&f.owner, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner()
        .private_provider;
    assert!(
        provider.starts_with(&format!("workspaces/{}/providers/", f.workspace.id())),
        "{provider}"
    );

    let enabled = i32::from(management::ToolAccessState::Enabled);
    let mut owner_rows = private_tool_access(&f, &f.owner, &provider).await;
    owner_rows.sort_by(|a, b| a.1.cmp(&b.1));
    let agent_name = format!(
        "workspaces/{}/agents/{}",
        f.workspace.id(),
        agent.principal().id()
    );
    assert_eq!(
        owner_rows
            .iter()
            .map(|(path, agent, state, explicit, _tool)| (
                path.as_str(),
                agent.as_str(),
                *state,
                *explicit
            ))
            .collect::<Vec<_>>(),
        [
            ("people.greet", "", enabled, false),
            ("people.greet", agent_name.as_str(), enabled, false),
        ]
    );
    let member_rows = private_tool_access(&f, &f.member, &provider).await;
    let [(_, member_agent, _, _, member_tool)] = member_rows.as_slice() else {
        panic!("a Member lists their own Private Tool: {member_rows:?}");
    };
    assert_eq!(member_agent, "");

    let update =
        async |user: &AuthenticatedUser, tool: &str, state: management::ToolAccessState| {
            f.policies()
                .update_tool_policies(request(
                    management::UpdateToolPoliciesRequest {
                        parent: provider.clone(),
                        policies: vec![management::ToolPolicy {
                            tool: tool.to_owned(),
                            access_state: state.into(),
                            explicit: false,
                        }],
                    },
                    &f.token(user, "reef"),
                    "reef",
                ))
                .await
        };
    let hidden = update(&f.owner, member_tool, management::ToolAccessState::Disabled)
        .await
        .unwrap_err();
    assert_eq!(
        (hidden.code(), error_reason(&hidden).as_str()),
        (tonic::Code::NotFound, "NOT_FOUND")
    );
    let set = update(
        &f.owner,
        &owner_rows.get(1).expect("the Agent's row").4,
        management::ToolAccessState::Disabled,
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        set.policies
            .iter()
            .map(|policy| (policy.access_state, policy.explicit))
            .collect::<Vec<_>>(),
        [(i32::from(management::ToolAccessState::Disabled), true)]
    );

    let token = f.token(&f.owner, "reef");
    let mut providers = f.providers();
    let current = providers
        .get_provider(request(
            management::GetProviderRequest {
                name: provider.clone(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let disabled = providers
        .disable_provider(request(
            management::DisableProviderRequest {
                name: provider.clone(),
                etag: current.etag,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (disabled.name.as_str(), disabled.state()),
        (provider.as_str(), management::ProviderState::Disabled)
    );
    let restored = providers
        .restore_provider(request(
            management::RestoreProviderRequest {
                name: provider.clone(),
                etag: disabled.etag,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(restored.state(), management::ProviderState::Active);
}
