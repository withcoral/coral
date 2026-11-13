use super::{DENIED, Fixture, INVALID, NOT_FOUND, error_reason, management, request};
use coral_tools::{
    AuthoredToolDescription, AuthoredToolSource, PrivateToolChangeContext, PrivateToolPath,
    PrivateToolSave,
};
use coral_workspace::{AuthenticatedPrincipal, AuthenticatedUser, TraceId};

async fn save(f: &Fixture, user: &AuthenticatedUser, path: &str, source: &str) {
    save_as(f, AuthenticatedPrincipal::user(user.clone()), path, source).await;
}

async fn save_as(f: &Fixture, actor: AuthenticatedPrincipal, path: &str, source: &str) {
    f.private_tools
        .save(
            &PrivateToolChangeContext::new(
                actor,
                1_700_000_000_000_000_007,
                TraceId::current_or_new(),
            ),
            f.workspace.id(),
            PrivateToolSave::new(
                PrivateToolPath::parse(path).unwrap(),
                AuthoredToolDescription::parse(&format!("Describe {path}")).unwrap(),
                AuthoredToolSource::parse(source).unwrap(),
            ),
        )
        .await
        .unwrap();
}

/// A new Agent of the Workspace with one Private Tool saved at `path`, and
/// the Agent's resource name.
async fn agent_with_private_tool(
    f: &Fixture,
    path: &str,
    source: &str,
) -> (AuthenticatedPrincipal, String) {
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
    save_as(f, agent.clone(), path, source).await;
    let name = format!(
        "workspaces/{}/agents/{}",
        f.workspace.id(),
        agent.principal().id()
    );
    (agent, name)
}

/// `agent`, an Agent's resource name, with another Workspace in its place.
fn in_another_workspace(agent: &str) -> String {
    let id = agent.rsplit('/').next().unwrap();
    format!("workspaces/{}/agents/{id}", uuid::Uuid::new_v4())
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
    assert_eq!(second.next_page_token, "");

    let foreign = list(&f.member, first.next_page_token).await.unwrap_err();
    assert_eq!((foreign.code(), error_reason(&foreign).as_str()), INVALID);
}

// Catches: a read that ignores `agent` and answers with the caller's own
// Private Tool, drops the source or the saved diagnostics, takes an Agent of
// another Workspace, or maps a concealed Private Tool to anything but
// NOT_FOUND.
#[tokio::test]
async fn get_returns_the_named_private_tools_authoring_material_and_maps_each_refusal() {
    let f = Fixture::new().await;
    save(
        &f,
        &f.owner,
        "reports.total",
        "export default function total(): number { return missing; }",
    )
    .await;
    let (_principal, agent) = agent_with_private_tool(
        &f,
        "reports.total",
        "export default function total(): number { return absent; }",
    )
    .await;
    let get = async |user: &AuthenticatedUser, agent: &str, path: &str, audience: &str| {
        f.private_tools()
            .get_private_tool(request(
                management::GetPrivateToolRequest {
                    parent: format!("workspaces/{}", f.workspace.id()),
                    path: path.into(),
                    agent: agent.into(),
                },
                &f.token(user, audience),
                audience,
            ))
            .await
    };

    for (of_agent, undeclared) in [("", "missing"), (agent.as_str(), "absent")] {
        let tool = get(&f.owner, of_agent, "reports.total", "reef")
            .await
            .unwrap()
            .into_inner();
        assert_eq!(tool.path, "reports.total");
        assert_eq!(tool.description, "Describe reports.total");
        assert!(
            tool.source.contains(&format!("return {undeclared};")),
            "{of_agent:?}: {}",
            tool.source
        );
        assert!(tool.declaration.contains("export default"));
        assert!(
            matches!(
                tool.diagnostics.as_slice(),
                [diagnostic]
                    if diagnostic.category() == management::AuthoredToolDiagnosticCategory::Error
                        && diagnostic.message.contains(undeclared)
                        && diagnostic.line == Some(1)
            ),
            "{of_agent:?}: {:?}",
            tool.diagnostics
        );
        assert!(!tool.diagnostics_truncated);
    }

    let (agent, elsewhere) = (agent.as_str(), in_another_workspace(&agent));
    for (user, of_agent, path, audience, expected) in [
        (&f.member, "", "reports.total", "reef", NOT_FOUND),
        (&f.owner, "", "reports.absent", "reef", NOT_FOUND),
        (&f.owner, "", "reports..total", "reef", INVALID),
        (&f.owner, "", "reports.total", "mcp", DENIED),
        (&f.member, agent, "reports.total", "reef", NOT_FOUND),
        (&f.owner, agent, "reports.absent", "reef", NOT_FOUND),
        (&f.owner, &elsewhere, "reports.total", "reef", INVALID),
        (&f.owner, "agents/worker", "reports.total", "reef", INVALID),
    ] {
        let status = get(user, of_agent, path, audience).await.unwrap_err();
        assert_eq!(
            (status.code(), error_reason(&status).as_str()),
            expected,
            "{path} of {of_agent:?} via {audience}"
        );
    }
}

// Catches: a delete that ignores `agent` and removes the caller's own Private
// Tool, takes an Agent of another Workspace, or maps a concealed Private Tool
// to anything but NOT_FOUND.
#[tokio::test]
async fn delete_removes_only_the_private_tool_the_request_names() {
    let f = Fixture::new().await;
    save(&f, &f.owner, "reports.total", "export default () => 1;").await;
    let (agent_principal, agent) =
        agent_with_private_tool(&f, "reports.total", "export default () => 2;").await;
    let parent = format!("workspaces/{}", f.workspace.id());
    let delete = async |user: &AuthenticatedUser, agent: &str, path: &str, audience: &str| {
        f.private_tools()
            .delete_private_tool(request(
                management::DeletePrivateToolRequest {
                    parent: parent.clone(),
                    path: path.into(),
                    agent: agent.into(),
                },
                &f.token(user, audience),
                audience,
            ))
            .await
    };
    // Whether [the Owner's, the Agent's] Private Tool at the path exists.
    let exist = async || {
        let path = PrivateToolPath::parse("reports.total").unwrap();
        let owner = AuthenticatedPrincipal::user(f.owner.clone());
        let mut exist = [false; 2];
        for (exists, principal) in exist.iter_mut().zip([&owner, &agent_principal]) {
            *exists = match f
                .private_tools
                .get(principal, f.workspace.id(), &path)
                .await
            {
                Ok(_) => true,
                Err(coral_tools::Error::NotFound(_)) => false,
                Err(error) => panic!("reading the Private Tool failed: {error}"),
            };
        }
        exist
    };

    let (agent, elsewhere) = (agent.as_str(), in_another_workspace(&agent));
    for (user, of_agent, path, audience, expected) in [
        (&f.member, "", "reports.total", "reef", NOT_FOUND),
        (&f.owner, "", "reports..total", "reef", INVALID),
        (&f.owner, "", "reports.total", "mcp", DENIED),
        (&f.member, agent, "reports.total", "reef", NOT_FOUND),
        (&f.owner, agent, "reports.absent", "reef", NOT_FOUND),
        (&f.owner, &elsewhere, "reports.total", "reef", INVALID),
        (&f.owner, "agents/worker", "reports.total", "reef", INVALID),
    ] {
        let status = delete(user, of_agent, path, audience).await.unwrap_err();
        assert_eq!(
            (status.code(), error_reason(&status).as_str()),
            expected,
            "{path} of {of_agent:?} via {audience}"
        );
        assert_eq!(
            exist().await,
            [true, true],
            "a refused delete keeps both Private Tools"
        );
    }

    delete(&f.owner, agent, "reports.total", "reef")
        .await
        .unwrap();
    assert_eq!(exist().await, [true, false]);
    delete(&f.owner, "", "reports.total", "reef").await.unwrap();
    assert_eq!(exist().await, [false, false]);
    for of_agent in ["", agent] {
        let again = delete(&f.owner, of_agent, "reports.total", "reef")
            .await
            .unwrap_err();
        assert_eq!(
            (again.code(), error_reason(&again).as_str()),
            NOT_FOUND,
            "{of_agent:?}"
        );
    }
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
    let (_principal, agent_name) =
        agent_with_private_tool(&f, "people.greet", "export default () => 3;").await;

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
    assert_eq!((hidden.code(), error_reason(&hidden).as_str()), NOT_FOUND);
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
