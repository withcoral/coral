use super::*;

#[tokio::test]
async fn management_rejects_unspecified_and_unknown_membership_roles() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let mut members = f.memberships();
    let member = members
        .get_workspace_membership(request(
            management::GetWorkspaceMembershipRequest {
                name: format!(
                    "workspaces/{}/memberships/{}",
                    f.workspace.id(),
                    f.member.user_id()
                ),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(member.role, management::WorkspaceRole::Member as i32);
    for role in [management::WorkspaceRole::Unspecified as i32, 999] {
        let error = members
            .update_workspace_membership(request(
                management::UpdateWorkspaceMembershipRequest {
                    membership: Some(management::WorkspaceMembership {
                        role,
                        ..member.clone()
                    }),
                    update_mask: Some(prost_types::FieldMask {
                        paths: vec!["role".into()],
                    }),
                },
                &token,
                "reef",
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(error_reason(&error), "INVALID_ARGUMENT");
    }
    let unchanged = members
        .get_workspace_membership(request(
            management::GetWorkspaceMembershipRequest {
                name: member.name.clone(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(unchanged, member);
}

#[tokio::test]
async fn management_rejects_unspecified_and_unknown_agent_states() {
    let f = Fixture::new().await;
    let token = f.token(&f.owner, "reef");
    let mut agents = f.agents();
    let agent = agents
        .create_agent(request(
            management::CreateAgentRequest {
                parent: format!("workspaces/{}", f.workspace.id()),
                display_name: "Enum validation".into(),
                request_id: uuid::Uuid::new_v4().to_string(),
                expire_time: None,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner()
        .agent
        .unwrap();
    assert_eq!(agent.state, management::AgentState::Enabled as i32);
    for state in [management::AgentState::Unspecified as i32, 999] {
        let error = agents
            .update_agent(request(
                management::UpdateAgentRequest {
                    agent: Some(management::Agent {
                        state,
                        ..agent.clone()
                    }),
                    update_mask: Some(prost_types::FieldMask {
                        paths: vec!["state".into()],
                    }),
                },
                &token,
                "reef",
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(error_reason(&error), "INVALID_ARGUMENT");
    }
    let unchanged = agents
        .get_agent(request(
            management::GetAgentRequest {
                name: agent.name.clone(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(unchanged, agent);
}

#[tokio::test]
async fn management_validation_returns_standard_safe_error_info_and_preserves_trace_parent() {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::prelude::*;
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(opentelemetry_sdk::trace::InMemorySpanExporter::default())
        .build();
    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("grpc-contract")))
        .try_init()
        .unwrap();
    let f = Fixture::new().await;
    let mut client = f.workspace_service();
    let mut update = request(
        management::UpdateWorkspaceRequest {
            workspace: None,
            update_mask: Some(prost_types::FieldMask {
                paths: vec!["private-injected-path".into()],
            }),
        },
        &f.token(&f.owner, "reef"),
        "reef",
    );
    update.metadata_mut().insert(
        "traceparent",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            .parse()
            .unwrap(),
    );
    let failure = client.update_workspace(update).await.unwrap_err();
    assert_eq!(failure.code(), tonic::Code::InvalidArgument);
    let detail = error_info(&failure);
    assert_eq!(detail.reason, "INVALID_ARGUMENT");
    assert_eq!(detail.domain, "coral.management");
    assert_eq!(detail.metadata.len(), 1);
    let (key, trace) = detail.metadata.into_iter().next().unwrap();
    assert_eq!(key, "trace_id");
    assert_eq!(trace, "4bf92f3577b34da6a3ce929d0e0e4736");
    assert!(!failure.message().contains("private-injected-path"));
}

#[tokio::test]
async fn management_membership_creation_conceals_targets_before_eligibility() {
    let f = Fixture::new().await;
    let private = f
        .workspaces
        .create(
            &change(&f.owner),
            WorkspaceDisplayName::parse("concealed").unwrap(),
        )
        .await
        .unwrap();
    let mut client = f.memberships();
    for user in ["users/owner", "users/absent"] {
        let failure = client
            .create_workspace_membership(request(
                management::CreateWorkspaceMembershipRequest {
                    parent: format!("workspaces/{}", private.id()),
                    email: format!("{user}@example.test"),
                    role: management::WorkspaceRole::Member.into(),
                    request_id: uuid::Uuid::new_v4().to_string(),
                },
                &f.token(&f.member, "reef"),
                "reef",
            ))
            .await
            .unwrap_err();
        assert_eq!(failure.code(), tonic::Code::NotFound);
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "private profile projections follow one creator through membership loss and disablement"
)]
async fn management_profiles_follow_people_and_retained_creator_visibility() {
    let f = Fixture::new().await;
    let database = f.database.database();
    let organization = OrganizationModule::new(database.clone());
    let third = AuthenticatedUser::organization_member(
        UserId::parse("third").unwrap(),
        OrganizationRole::Member,
    );
    organization.provision_login(&change(&third)).await.unwrap();
    f.workspaces
        .set_membership(
            &change(&f.owner),
            f.workspace.id(),
            &third,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    f.sign_in_profiles(&[
        (&f.owner, "Owner Name", "owner@example.test", false),
        (&f.member, "Creator Name", "creator@example.test", false),
        (&third, "Private Member", "private@example.test", false),
    ])
    .await;
    let mut people = f.memberships();
    let parent = format!("workspaces/{}", f.workspace.id());
    let token = f.token(&f.member, "reef");
    let owners = people
        .list_workspace_owners(request(
            management::ListWorkspaceOwnersRequest {
                parent: parent.clone(),
                ..Default::default()
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        owners
            .owners
            .first()
            .unwrap()
            .profile
            .as_ref()
            .unwrap()
            .email,
        "owner@example.test"
    );
    let own = people
        .get_workspace_membership(request(
            management::GetWorkspaceMembershipRequest {
                name: format!("{parent}/memberships/member"),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(own.profile.unwrap().display_name, "Creator Name");
    for user in ["third", "missing"] {
        assert_eq!(
            people
                .get_workspace_membership(request(
                    management::GetWorkspaceMembershipRequest {
                        name: format!("{parent}/memberships/{user}")
                    },
                    &token,
                    "reef"
                ))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
    }
    let members = people
        .list_workspace_memberships(request(
            management::ListWorkspaceMembershipsRequest {
                parent: parent.clone(),
                ..Default::default()
            },
            &f.token(&f.owner, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(
        members
            .memberships
            .iter()
            .any(|m| m.profile.as_ref().unwrap().email == "private@example.test")
    );
    f.workspaces
        .set_membership(
            &change(&f.owner),
            f.workspace.id(),
            &f.member,
            WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    let issued = f
        .workspaces
        .create_agent(
            &change(&f.member),
            f.workspace.id(),
            coral_workspace::AgentDisplayName::parse("Attribution").unwrap(),
            None,
        )
        .await
        .unwrap();
    coral_administration::AccessLifecycleModule::new(f.database.database(), f.workspaces.clone())
        .remove_workspace_member(&change(&f.owner), f.workspace.id(), f.member.user_id())
        .await
        .unwrap();
    organization
        .set_enabled(&change(&f.owner), f.member.user_id(), 1, false)
        .await
        .unwrap();
    let name = format!("{parent}/agents/{}", issued.agent.principal().id());
    let mut agents = f.agents();
    let retained = agents
        .get_agent(request(
            management::GetAgentRequest { name: name.clone() },
            &f.token(&f.owner, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        retained.creator_profile.unwrap().email,
        "creator@example.test"
    );
    assert_eq!(
        agents
            .get_agent(request(
                management::GetAgentRequest { name },
                &f.token(&third, "reef"),
                "reef"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
}

#[tokio::test]
async fn paged_memberships_carry_each_members_role_and_enabled_state() {
    let f = Fixture::new().await;
    let mut client = f.memberships();
    let parent = format!("workspaces/{}", f.workspace.id());
    let token = f.token(&f.owner, "reef");
    let mut next = String::new();
    let mut members = Vec::new();
    loop {
        let page = client
            .list_workspace_memberships(request(
                management::ListWorkspaceMembershipsRequest {
                    parent: parent.clone(),
                    page_size: 1,
                    page_token: next,
                },
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(page.memberships.len(), 1);
        members.extend(page.memberships);
        next = page.next_page_token;
        if next.is_empty() {
            break;
        }
    }
    let mut members: Vec<_> = members
        .iter()
        .map(|m| (m.user.as_str(), m.role, m.enabled))
        .collect();
    members.sort_unstable();
    assert_eq!(
        members,
        [
            (
                "users/member",
                management::WorkspaceRole::Member as i32,
                true
            ),
            ("users/owner", management::WorkspaceRole::Owner as i32, true),
        ]
    );
}

#[tokio::test]
async fn management_email_grant_conceals_disabled_and_absent_users() {
    let f = Fixture::new().await;
    f.sign_in_profiles(&[(&f.member, "Member", "Member@Example.test", true)])
        .await;
    let mut client = f.memberships();
    let parent = format!("workspaces/{}", f.workspace.id());
    let grant = management::CreateWorkspaceMembershipRequest {
        parent: parent.clone(),
        email: " member@example.test ".into(),
        role: management::WorkspaceRole::Member.into(),
        request_id: uuid::Uuid::new_v4().to_string(),
    };
    let token = f.token(&f.owner, "reef");
    assert_eq!(
        client
            .create_workspace_membership(request(
                grant.clone(),
                &f.token(&f.member, "reef"),
                "reef"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    coral_administration::AccessLifecycleModule::new(f.database.database(), f.workspaces.clone())
        .remove_workspace_member(&change(&f.owner), f.workspace.id(), f.member.user_id())
        .await
        .unwrap();
    let added = client
        .create_workspace_membership(request(grant.clone(), &token, "reef"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(added.user, "users/member");
    let profile = added.profile.unwrap();
    assert_eq!(profile.display_name, "Member");
    assert_eq!(profile.email, "Member@Example.test");
    OrganizationModule::new(f.database.database())
        .set_enabled(&change(&f.owner), f.member.user_id(), 1, false)
        .await
        .unwrap();
    let disabled = client
        .create_workspace_membership(request(
            management::CreateWorkspaceMembershipRequest {
                request_id: uuid::Uuid::new_v4().to_string(),
                ..grant
            },
            &token,
            "reef",
        ))
        .await
        .unwrap_err();
    let absent = client
        .create_workspace_membership(request(
            management::CreateWorkspaceMembershipRequest {
                parent,
                email: "absent@example.test".into(),
                role: management::WorkspaceRole::Member.into(),
                request_id: uuid::Uuid::new_v4().to_string(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(disabled.code(), tonic::Code::NotFound);
    assert_eq!(
        (disabled.code(), disabled.message()),
        (absent.code(), absent.message())
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one resource is followed through replay, pagination, and archive over native gRPC"
)]
async fn management_pagination_replay_and_archived_configuration_are_bounded() {
    let f = Fixture::new().await;
    let mut client = f.workspace_service();
    let token = f.token(&f.owner, "reef");
    let create = management::CreateWorkspaceRequest {
        display_name: "Receipt".into(),
        request_id: uuid::Uuid::new_v4().to_string(),
    };
    let w = client
        .create_workspace(request(create.clone(), &token, "reef"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        client
            .create_workspace(request(create, &token, "reef"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let first = client
        .list_workspaces(request(
            management::ListWorkspacesRequest {
                page_size: 1,
                page_token: String::new(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.workspaces.len(), 1);
    assert!(!first.next_page_token.is_empty());
    let second = client
        .list_workspaces(request(
            management::ListWorkspacesRequest {
                page_size: 1,
                page_token: first.next_page_token.clone(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second.workspaces.len(), 1);
    assert!(second.next_page_token.is_empty());
    assert_eq!(
        [&first, &second].map(|page| page.workspaces.first().unwrap().display_name.as_str()),
        ["private", "Receipt"],
        "pages follow the alphabetical order, whatever the case"
    );
    assert_eq!(
        client
            .list_workspaces(request(
                management::ListWorkspacesRequest {
                    page_size: 1,
                    page_token: first.next_page_token
                },
                &f.token(&f.member, "reef"),
                "reef"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    let archived = client
        .archive_workspace(request(
            management::ArchiveWorkspaceRequest {
                name: w.name.clone(),
                etag: w.etag,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        client
            .update_workspace(request(
                management::UpdateWorkspaceRequest {
                    workspace: Some(management::Workspace {
                        display_name: "Rejected".into(),
                        ..archived.clone()
                    }),
                    update_mask: Some(prost_types::FieldMask {
                        paths: vec!["display_name".into()]
                    })
                },
                &token,
                "reef"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        client
            .update_workspace(request(
                management::UpdateWorkspaceRequest {
                    workspace: Some(archived),
                    update_mask: Some(prost_types::FieldMask {
                        paths: vec!["name".into()]
                    })
                },
                &token,
                "reef"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        client
            .get_workspace(request(
                management::GetWorkspaceRequest { name: w.name },
                &f.token(&f.member, "mcp"),
                "mcp"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one Agent is followed through authorization, exact-target cleanup, and retained attribution"
)]
async fn management_agent_deletion_preserves_attribution_and_checks_exact_workspace() {
    let f = Fixture::new().await;
    let mut agents = f.agents();
    let token = f.token(&f.owner, "reef");
    let create = management::CreateAgentRequest {
        parent: format!("workspaces/{}", f.workspace.id()),
        display_name: "Retained attribution".into(),
        request_id: uuid::Uuid::new_v4().to_string(),
        expire_time: None,
    };
    let issued = agents
        .create_agent(request(create.clone(), &token, "reef"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        agents
            .create_agent(request(create.clone(), &token, "reef"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        agents
            .create_agent(request(create.clone(), &f.token(&f.member, "reef"), "reef"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        agents
            .create_agent(request(create, &issued.credential, "reef"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    let principal = f
        .workspaces
        .authenticate_agent(f.workspace.id(), &issued.credential, 1)
        .await
        .unwrap();
    let task = f.task(&principal).await;
    let agent = issued.agent.unwrap();
    let other = f
        .workspaces
        .create(
            &change(&f.owner),
            WorkspaceDisplayName::parse("Other").unwrap(),
        )
        .await
        .unwrap();
    let cross = agent
        .name
        .replace(&f.workspace.id().to_string(), &other.id().to_string());
    assert_eq!(
        agents
            .delete_agent(request(
                management::DeleteAgentRequest {
                    name: cross,
                    etag: agent.etag.clone()
                },
                &token,
                "reef"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    agents
        .delete_agent(request(
            management::DeleteAgentRequest {
                name: agent.name.clone(),
                etag: agent.etag,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap();
    assert_eq!(
        agents
            .get_agent(request(
                management::GetAgentRequest { name: agent.name },
                &token,
                "reef"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    f.workspaces
        .authenticate_agent(f.workspace.id(), &issued.credential, 1)
        .await
        .unwrap_err();
    let mut tasks = f.task_service();
    let retained = tasks
        .get_task(request(
            management::GetTaskRequest {
                name: format!("workspaces/{}/tasks/{}", f.workspace.id(), task.id()),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retained.intent, "private-intent");
}

#[tokio::test]
async fn management_registration_masks_preserve_unmasked_fields_and_reject_stale_etags() {
    let f = Fixture::new().await;
    let created = f
        .workspaces
        .create_agent(
            &change(&f.owner),
            f.workspace.id(),
            AgentDisplayName::parse("masked").unwrap(),
            None,
        )
        .await
        .unwrap();
    let name = format!(
        "workspaces/{}/agents/{}/registration",
        f.workspace.id(),
        created.agent.principal().id()
    );
    let mut client = f.registrations();
    let token = f.token(&f.owner, "reef");
    let original = client
        .get_agent_registration(request(
            management::GetAgentRegistrationRequest { name: name.clone() },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let updated = client
        .update_agent_registration(request(
            management::UpdateAgentRegistrationRequest {
                registration: Some(management::AgentRegistration {
                    enabled: false,
                    expire_time: Some(prost_types::Timestamp {
                        seconds: 1,
                        nanos: 0,
                    }),
                    ..original.clone()
                }),
                update_mask: Some(prost_types::FieldMask {
                    paths: vec!["expire_time".into()],
                }),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(updated.enabled);
    assert_eq!(updated.expire_time.unwrap().seconds, 1);
    assert_eq!(
        client
            .update_agent_registration(request(
                management::UpdateAgentRegistrationRequest {
                    registration: Some(original),
                    update_mask: Some(prost_types::FieldMask {
                        paths: vec!["enabled".into()]
                    })
                },
                &token,
                "reef"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Aborted
    );
}

/// A second Organization Owner and designated Workspace Owner, who may list
/// the same collections.
async fn second_owner(f: &Fixture) -> AuthenticatedUser {
    let owner = f
        .organization_user("second-owner", OrganizationRole::Owner)
        .await;
    f.workspaces
        .set_membership(
            &change(&f.owner),
            f.workspace.id(),
            &owner,
            WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    owner
}

/// Pages each item once in name order; a token is bound to the caller who got it.
#[tokio::test]
async fn agent_and_organization_membership_pages_round_trip_and_bind_their_token() {
    let f = Fixture::new().await;
    for name in ["first", "second"] {
        f.workspaces
            .create_agent(
                &change(&f.owner),
                f.workspace.id(),
                AgentDisplayName::parse(name).unwrap(),
                None,
            )
            .await
            .unwrap();
    }
    let other = second_owner(&f).await;
    let parent = format!("workspaces/{}", f.workspace.id());
    let agents = async |user: &AuthenticatedUser, page_token: String| {
        f.agents()
            .list_agents(request(
                management::ListAgentsRequest {
                    parent: parent.clone(),
                    page_size: 1,
                    page_token,
                },
                &f.token(user, "reef"),
                "reef",
            ))
            .await
            .map(|page| {
                let page = page.into_inner();
                let names = page.agents.into_iter().map(|a| a.name).collect::<Vec<_>>();
                (names, page.next_page_token)
            })
    };
    let memberships = async |user: &AuthenticatedUser, page_token: String| {
        f.organization_memberships()
            .list_organization_memberships(request(
                management::ListOrganizationMembershipsRequest {
                    page_size: 1,
                    page_token,
                },
                &f.token(user, "reef"),
                "reef",
            ))
            .await
            .map(|page| {
                let page = page.into_inner();
                let names = page
                    .memberships
                    .into_iter()
                    .map(|m| m.name)
                    .collect::<Vec<_>>();
                (names, page.next_page_token)
            })
    };
    let (first, token) = agents(&f.owner, String::new()).await.unwrap();
    let (second, last) = agents(&f.owner, token.clone()).await.unwrap();
    assert_eq!((first.len(), second.len()), (1, 1));
    assert!(first < second, "name order without repeats");
    assert!(last.is_empty());
    let foreign = agents(&other, token).await.unwrap_err();
    assert_eq!(
        (foreign.code(), error_reason(&foreign).as_str()),
        (tonic::Code::InvalidArgument, "INVALID_ARGUMENT")
    );
    let mut seen = Vec::new();
    let mut token = String::new();
    loop {
        let (names, next) = memberships(&f.owner, token).await.unwrap();
        assert_eq!(names.len(), 1);
        seen.extend(names);
        if next.is_empty() {
            break;
        }
        token = next;
    }
    assert_eq!(
        seen,
        [
            "organizationMemberships/member",
            "organizationMemberships/owner",
            "organizationMemberships/second-owner"
        ],
        "each membership once, in name order"
    );
    let (_, token) = memberships(&f.owner, String::new()).await.unwrap();
    let foreign = memberships(&other, token).await.unwrap_err();
    assert_eq!(
        (foreign.code(), error_reason(&foreign).as_str()),
        (tonic::Code::InvalidArgument, "INVALID_ARGUMENT")
    );
}
