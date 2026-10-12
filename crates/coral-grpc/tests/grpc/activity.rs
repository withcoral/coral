use super::{Fixture, management as wire, request};

#[path = "activity_evidence.rs"]
mod evidence;

#[tokio::test]
async fn activity_watch_is_reef_only_and_stops_when_current_user_authority_is_lost() {
    use std::time::Duration;
    let f = Fixture::new().await;
    let mut client = f.activity();
    assert_eq!(
        client
            .watch_activity(request(
                wire::WatchActivityRequest {},
                &f.token(&f.member, "mcp"),
                "mcp"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut stream = client
        .watch_activity(request(
            wire::WatchActivityRequest {},
            &f.token(&f.member, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(stream.message().await.unwrap().unwrap().changed);
    coral_workspace::OrganizationModule::new(f.database.database())
        .set_enabled(&super::change(&f.owner), f.member.user_id(), 1, false)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match stream.message().await {
                Err(status) => {
                    assert!(matches!(
                        status.code(),
                        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated
                    ));
                    break;
                }
                Ok(Some(_)) => {}
                Ok(None) => panic!("authority loss must be explicit"),
            }
        }
    })
    .await
    .unwrap();
    drop(stream);
}

/// After its opening message, the stream reports each committed change so Reef rereads.
#[tokio::test]
async fn activity_watch_reports_a_committed_change() {
    let f = Fixture::new().await;
    let mut stream = f
        .activity()
        .watch_activity(request(
            wire::WatchActivityRequest {},
            &f.token(&f.member, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(stream.message().await.unwrap().unwrap().changed);
    f.task(&coral_workspace::AuthenticatedPrincipal::user(
        f.member.clone(),
    ))
    .await;
    // The next heartbeat is five seconds away, so the next message is the change.
    let next = tokio::time::timeout(std::time::Duration::from_secs(2), stream.message())
        .await
        .expect("the committed Task wakes the stream")
        .unwrap()
        .unwrap();
    assert!(next.changed, "a change is not reported as a heartbeat");
}

#[tokio::test]
async fn a_removed_agent_keeps_owner_only_attribution_without_a_registration() {
    let f = Fixture::new().await;
    let issued = f
        .workspaces
        .create_agent(
            &super::change(&f.owner),
            f.workspace.id(),
            coral_workspace::AgentDisplayName::parse("Retained worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = issued.agent.principal().id();
    coral_administration::AccessLifecycleModule::new(f.database.database(), f.workspaces.clone())
        .delete_agent(&super::change(&f.owner), f.workspace.id(), agent)
        .await
        .unwrap();
    let mut client = f.activity();
    let name = format!("workspaces/{}/agents/{agent}", f.workspace.id());
    let retained = client
        .inspect_agent_history(request(
            wire::InspectAgentHistoryRequest { name: name.clone() },
            &f.token(&f.owner, "reef"),
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(retained.deleted);
    assert_eq!(retained.display_name, "Retained worker");
    assert_eq!(retained.name, name);
    assert_eq!(
        client
            .inspect_agent_history(request(
                wire::InspectAgentHistoryRequest { name },
                &f.token(&f.member, "reef"),
                "reef"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
}

use coral_workspace::AuthenticatedPrincipal;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one authenticated hierarchy scenario checks large exact payloads, summary isolation, scope, audience, and authority loss"
)]
async fn execution_transport_keeps_large_exact_output_out_of_roots_and_detail_independent_from_scope()
 {
    let f = Fixture::new().await;
    let actor = AuthenticatedPrincipal::user(f.member.clone());
    let source = "console.log('private-log'); coral.emit('private-output'); return 'x'.repeat(5 * 1024 * 1024);";
    let accepted = f
        .runs
        .start(
            &actor,
            f.workspace.id(),
            coral_run::RunSubmission::new(
                coral_run::ProgramSource::parse(source).unwrap(),
                coral_run::RunTimeout::from_millis(60_000).unwrap(),
            ),
        )
        .await
        .unwrap();
    f.settle(&actor, accepted.run_id()).await;
    let mut client = f.activity().max_decoding_message_size(128 << 20);
    let member = f.token(&f.member, "reef");
    let page = client
        .browse_activity_roots(request(
            wire::BrowseActivityRootsRequest::default(),
            &member,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.roots.len(), 1);
    assert!(!page.has_before && !page.has_after && !page.anchor_unavailable);
    let Some(wire::activity_root::Record::Run(run)) = &page.roots.first().unwrap().record else {
        panic!("standalone root")
    };
    assert_eq!(run.state(), wire::ActivityRunState::Succeeded);
    assert!(
        run.action_page.is_none(),
        "ordinary history does not claim an observed empty action snapshot"
    );
    let encoded = prost::Message::encode_to_vec(&page);
    assert!(
        encoded.len() < 1024,
        "summary excludes source and large output"
    );
    assert!(!String::from_utf8_lossy(&encoded).contains("private-log"));
    assert!(!String::from_utf8_lossy(&encoded).contains("private-output"));
    let name = run.name.clone();
    let other = f
        .workspaces
        .create(
            &super::change(&f.member),
            coral_workspace::WorkspaceDisplayName::parse("Other history").unwrap(),
        )
        .await
        .unwrap();
    let excluded = client
        .browse_activity_roots(request(
            wire::BrowseActivityRootsRequest {
                scope: Some(wire::ActivityScope {
                    workspace: format!("workspaces/{}", other.id()),
                    agent: String::new(),
                }),
                ..Default::default()
            },
            &member,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(excluded.roots.is_empty());
    assert!(!excluded.has_before && !excluded.has_after && !excluded.anchor_unavailable);
    let detail = client
        .inspect_run(request(
            wire::InspectRunRequest {
                name: name.clone(),
                page_size: 1,
                event_after: 0,
                ..Default::default()
            },
            &member,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(detail.submitted_source, source);
    assert_eq!(detail.events.len(), 1);
    assert!(detail.has_more_events);
    assert!(matches!(
        detail.events.first().unwrap().payload,
        Some(wire::activity_run_event::Payload::Started(_))
    ));
    let Some(wire::inspect_run_response::Terminal::Succeeded(success)) = detail.terminal else {
        panic!("typed successful terminal evidence")
    };
    assert_eq!(
        success.final_value_json.as_ref().unwrap().len(),
        5 * 1024 * 1024 + 2
    );
    let events = client
        .inspect_run(request(
            wire::InspectRunRequest {
                name: name.clone(),
                event_after: detail.next_event_after,
                page_size: 100,
                ..Default::default()
            },
            &member,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner()
        .events;
    assert!(events.iter().any(|event| matches!(&event.payload,
        Some(wire::activity_run_event::Payload::ProgramLogged(log))
            if log.level() == wire::ActivityLogLevel::Info && log.message == "private-log")));
    assert!(events.iter().any(|event| matches!(&event.payload,
        Some(wire::activity_run_event::Payload::ProgramEmitted(emission))
            if emission.value_json == "\"private-output\"")));
    assert!(events.iter().any(|event| matches!(&event.payload,
        Some(wire::activity_run_event::Payload::Ended(ended))
            if ended.state() == wire::ActivityRunState::Succeeded)));
    let owner = f.token(&f.owner, "reef");
    assert_eq!(
        client
            .inspect_run(request(
                wire::InspectRunRequest {
                    name: name.clone(),
                    ..Default::default()
                },
                &owner,
                "reef"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    let issued = f
        .workspaces
        .create_agent(
            &super::change(&f.owner),
            f.workspace.id(),
            coral_workspace::AgentDisplayName::parse("history agent").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = f
        .workspaces
        .authenticate_agent(f.workspace.id(), issued.credential.expose(), 2)
        .await
        .unwrap();
    let agent_run = f
        .runs
        .start(
            &agent,
            f.workspace.id(),
            coral_run::RunSubmission::new(
                coral_run::ProgramSource::parse("return 1;").unwrap(),
                coral_run::RunTimeout::from_millis(60_000).unwrap(),
            ),
        )
        .await
        .unwrap();
    f.settle(&agent, agent_run.run_id()).await;
    let agent_detail = client
        .inspect_run(request(
            wire::InspectRunRequest {
                name: format!(
                    "workspaces/{}/runs/{}",
                    f.workspace.id(),
                    agent_run.run_id()
                ),
                ..Default::default()
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let exact_agent = agent_detail.run.unwrap();
    assert_eq!(
        exact_agent.agent,
        format!(
            "workspaces/{}/agents/{}",
            f.workspace.id(),
            agent.principal().id()
        )
    );
    // Agent credentials cannot use the human history audience, even for their own Run.
    assert_eq!(
        client
            .inspect_run(request(
                wire::InspectRunRequest {
                    name: exact_agent.name.clone(),
                    ..Default::default()
                },
                issued.credential.expose(),
                "mcp"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    // Child browsing derives scope from independently authorized exact attribution, not list scope.
    let children = client
        .browse_run_entries(request(
            wire::BrowseRunEntriesRequest {
                scope: Some(wire::ActivityScope {
                    workspace: exact_agent.workspace,
                    agent: exact_agent.agent,
                }),
                name: exact_agent.name.clone(),
                ..Default::default()
            },
            &owner,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(children.has_final_value);
    assert!(
        matches!(children.entries.as_slice(), [wire::ActivityRunEntry {
            record: Some(wire::activity_run_entry::Record::Event(event))
        }] if event.kind == "run.ended" && event.context_impact_bytes == Some(1)),
        "{:?}",
        children.entries
    );
    let mcp = f.token(&f.member, "mcp");
    assert_eq!(
        client
            .browse_activity_roots(request(
                wire::BrowseActivityRootsRequest::default(),
                &mcp,
                "mcp"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        client
            .browse_run_entries(request(
                wire::BrowseRunEntriesRequest {
                    name: exact_agent.name,
                    ..Default::default()
                },
                &mcp,
                "mcp"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    coral_administration::AccessLifecycleModule::new(f.database.database(), f.workspaces.clone())
        .remove_workspace_member(
            &super::change(&f.owner),
            f.workspace.id(),
            f.member.user_id(),
        )
        .await
        .unwrap();
    let lost = client
        .browse_activity_roots(request(
            wire::BrowseActivityRootsRequest::default(),
            &member,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(lost.roots.is_empty());
    assert!(!lost.has_before && !lost.has_after && !lost.anchor_unavailable);
    assert_eq!(
        client
            .inspect_run(request(
                wire::InspectRunRequest {
                    name,
                    ..Default::default()
                },
                &member,
                "reef"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
}

/// coral-task owns root order and anchor reauthorization
/// (`root_windows_keep_qualified_order_and_reauthorize_rows_and_anchors`). This
/// checks the anchor request, the page flags, and the adapter's window rule.
#[tokio::test]
async fn root_reveal_keeps_neighboring_roots_and_reauthorizes_its_anchor() {
    let f = Fixture::new().await;
    let actor = AuthenticatedPrincipal::user(f.member.clone());
    f.task(&actor).await;
    f.task(&actor).await;
    let mut client = f.activity();
    let token = f.token(&f.member, "reef");
    let page = client
        .browse_activity_roots(request(
            wire::BrowseActivityRootsRequest::default(),
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let Some(wire::activity_root::Record::Task(first)) = &page.roots.first().unwrap().record else {
        panic!("Task root")
    };
    let revealed = client
        .browse_activity_roots(request(
            wire::BrowseActivityRootsRequest {
                anchor: first.name.clone(),
                ..Default::default()
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(revealed.roots.len(), 2);
    assert_eq!(revealed.roots, page.roots);
    assert!(!revealed.has_before && !revealed.has_after && !revealed.anchor_unavailable);
    assert_eq!(
        client
            .browse_activity_roots(request(
                wire::BrowseActivityRootsRequest {
                    anchor: first.name.clone(),
                    before: first.name.clone(),
                    ..Default::default()
                },
                &token,
                "reef"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

/// coral-task owns which retained identities still resolve
/// (`empty_root_windows_distinguish_boundaries_from_lost_retained_identities`).
/// This checks the retained request, its projection, and the adapter's rule that
/// a retained window takes no page or anchor.
#[tokio::test]
async fn retained_windows_are_bounded_current_authorized_selections_not_payload_caches() {
    let f = Fixture::new().await;
    let actor = AuthenticatedPrincipal::user(f.member.clone());
    let goal = f.task(&actor).await;
    let name = format!("workspaces/{}/tasks/{}", f.workspace.id(), goal.id());
    let other = f.task(&actor).await;
    let mut client = f.activity();
    let token = f.token(&f.member, "reef");
    let retained = wire::ActivityRecords {
        names: vec![name.clone()],
    };
    let page = client
        .browse_activity_roots(request(
            wire::BrowseActivityRootsRequest {
                retained: Some(retained.clone()),
                ..Default::default()
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    let [root] = page.roots.as_slice() else {
        panic!("only the retained root: {page:?}")
    };
    assert!(
        matches!(&root.record, Some(wire::activity_root::Record::Task(task)) if task.name == name)
    );
    assert!(!page.anchor_unavailable);
    for input in [
        wire::BrowseActivityRootsRequest {
            retained: Some(wire::ActivityRecords {
                names: vec![
                    name.clone(),
                    format!("workspaces/{}/tasks/{}", f.workspace.id(), other.id()),
                ],
            }),
            page_size: 1,
            ..Default::default()
        },
        wire::BrowseActivityRootsRequest {
            retained: Some(retained),
            anchor: name,
            ..Default::default()
        },
    ] {
        assert_eq!(
            client
                .browse_activity_roots(request(input, &token, "reef"))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[tokio::test]
async fn activity_transport_keeps_personal_history_private_and_mcp_unreachable() {
    let f = Fixture::new().await;
    let member_task = f
        .task(&AuthenticatedPrincipal::user(f.member.clone()))
        .await;
    f.task(&AuthenticatedPrincipal::user(f.owner.clone())).await;
    let mut client = f.activity();
    let member = f.token(&f.member, "reef");
    let page = client
        .browse_activity_roots(request(
            wire::BrowseActivityRootsRequest::default(),
            &member,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!page.has_before && !page.has_after && !page.anchor_unavailable);
    assert_eq!(page.roots.len(), 1);
    let name = format!("workspaces/{}/tasks/{}", f.workspace.id(), member_task.id());
    let Some(wire::activity_root::Record::Task(task)) = &page.roots.first().unwrap().record else {
        panic!("Task root")
    };
    assert_eq!(task.name, name);
    let detail = client
        .inspect_task(request(
            wire::InspectTaskRequest {
                name: name.clone(),
                ..Default::default()
            },
            &member,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(detail.task.unwrap().name, name);
    assert!(detail.interactions.is_empty());
    let owner = f.token(&f.owner, "reef");
    assert_eq!(
        client
            .inspect_task(request(
                wire::InspectTaskRequest {
                    name: name.clone(),
                    ..Default::default()
                },
                &owner,
                "reef"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        client
            .inspect_task_interaction(request(
                wire::InspectTaskInteractionRequest {
                    name: format!("{name}/interactions/00000000-0000-4000-8000-000000000001")
                },
                &member,
                "reef"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    let mcp = f.token(&f.member, "mcp");
    assert_eq!(
        client
            .browse_activity_roots(request(
                wire::BrowseActivityRootsRequest::default(),
                &mcp,
                "mcp"
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
}
