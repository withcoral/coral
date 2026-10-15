use super::{Fixture, error_reason, management, management as wire, request};
use coral_middleware::{
    MiddlewareChangeContext, MiddlewareDescription, MiddlewareName, MiddlewareSave,
    MiddlewareScopes, MiddlewareSource,
};
use coral_workspace::{AuthenticatedPrincipal, TraceId};

async fn save(f: &Fixture, name: &str) -> String {
    let saved = f
        .middleware
        .save(
            &MiddlewareChangeContext::new(
                AuthenticatedPrincipal::user(f.owner.clone()),
                1_700_000_000_000_000_007,
                TraceId::current_or_new(),
            ),
            f.workspace.id(),
            MiddlewareSave::new(
                MiddlewareName::parse(name).unwrap(),
                MiddlewareDescription::parse("Observe Operations").unwrap(),
                MiddlewareSource::parse(
                    "export default async (ctx: CoralMiddleware.Invocation, next: CoralMiddleware.Next) => next();",
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    format!("workspaces/{}/middleware/{}", f.workspace.id(), saved.id())
}

fn chain(response: &management::ListMiddlewareResponse) -> Vec<(String, u32, bool, bool)> {
    response
        .middleware
        .iter()
        .map(|item| {
            (
                item.display_name.clone(),
                item.position,
                item.enabled,
                item.scopes.as_ref().is_some_and(|scopes| scopes.operation),
            )
        })
        .collect()
}

#[tokio::test]
async fn owners_manage_scopes_activation_order_and_deletion() {
    let f = Fixture::new().await;
    let audit = save(&f, "audit").await;
    let limit = save(&f, "limit").await;
    let token = f.token(&f.owner, "reef");
    let mut client = f.middleware_client();

    for name in [&audit, &limit] {
        client
            .update_middleware_scopes(request(
                management::UpdateMiddlewareScopesRequest {
                    name: name.clone(),
                    scopes: Some(management::MiddlewareScopes {
                        run: false,
                        function: false,
                        operation: true,
                    }),
                },
                &token,
                "reef",
            ))
            .await
            .unwrap();
        client
            .set_middleware_enabled(request(
                management::SetMiddlewareEnabledRequest {
                    name: name.clone(),
                    enabled: true,
                },
                &token,
                "reef",
            ))
            .await
            .unwrap();
    }
    let moved = client
        .move_middleware(request(
            management::MoveMiddlewareRequest {
                name: limit.clone(),
                direction: management::MiddlewareMoveDirection::Up.into(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!moved.enabled, "a move disables the moved Middleware");

    let list = client
        .list_middleware(request(
            management::ListMiddlewareRequest {
                parent: format!("workspaces/{}", f.workspace.id()),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        chain(&list),
        [
            ("limit".to_string(), 0, false, true),
            ("audit".to_string(), 1, true, true),
        ]
    );
    assert!(list.middleware.iter().all(|item| item.source.is_empty()));

    let detail = client
        .get_middleware(request(
            management::GetMiddlewareRequest {
                name: audit.clone(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(detail.source.contains("export default"));

    client
        .delete_middleware(request(
            management::DeleteMiddlewareRequest {
                name: limit.clone(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap();
    let status = client
        .get_middleware(request(
            management::GetMiddlewareRequest { name: limit },
            &token,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn members_and_other_audiences_cannot_reach_middleware() {
    let f = Fixture::new().await;
    let audit = save(&f, "audit").await;
    let mut client = f.middleware_client();
    let member = f.token(&f.member, "reef");

    let status = client
        .list_middleware(request(
            management::ListMiddlewareRequest {
                parent: format!("workspaces/{}", f.workspace.id()),
            },
            &member,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert_eq!(error_reason(&status), "PERMISSION_DENIED");

    let status = client
        .set_middleware_enabled(request(
            management::SetMiddlewareEnabledRequest {
                name: audit.clone(),
                enabled: true,
            },
            &member,
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);

    let status = client
        .list_middleware(request(
            management::ListMiddlewareRequest {
                parent: format!("workspaces/{}", f.workspace.id()),
            },
            &f.token(&f.owner, "mcp"),
            "mcp",
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);

    let owner = f.token(&f.owner, "reef");
    let detail = client
        .get_middleware(request(
            management::GetMiddlewareRequest { name: audit },
            &owner,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!detail.enabled, "a denied change changed nothing");
}

/// An enabled Operation Middleware that holds every Program Emission for approval
/// after marking it reviewed.
async fn enable_emission_review(f: &Fixture) {
    let owner = MiddlewareChangeContext::new(
        AuthenticatedPrincipal::user(f.owner.clone()),
        1_700_000_000_000_000_007,
        TraceId::current_or_new(),
    );
    let saved = f
        .middleware
        .save(
            &owner,
            f.workspace.id(),
            MiddlewareSave::new(
                MiddlewareName::parse("review").unwrap(),
                MiddlewareDescription::parse("Review invoices before they leave").unwrap(),
                MiddlewareSource::parse(
                    r#"export default async (ctx: CoralMiddleware.Invocation, next: CoralMiddleware.Next) => {
                        if (ctx.kind === "operation" && ctx.path === "coral.emit") {
                            ctx.currentPolicy = "approval_required";
                            ctx.args = { ...(ctx.args as object), reviewed: true };
                        }
                        return next();
                    };"#,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    let scopes = MiddlewareScopes {
        run: false,
        function: false,
        operation: true,
    };
    f.middleware
        .set_scopes(&owner, f.workspace.id(), saved.id(), scopes)
        .await
        .unwrap();
    f.middleware
        .set_enabled(&owner, f.workspace.id(), saved.id(), true)
        .await
        .unwrap();
}

async fn first_queued_action(
    client: &mut management::activity_service_client::ActivityServiceClient<
        tonic::transport::Channel,
    >,
    token: &str,
) -> wire::ActivityActionSummary {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let queued = client
                .browse_required_actions(request(
                    wire::BrowseRequiredActionsRequest::default(),
                    token,
                    "reef",
                ))
                .await
                .unwrap()
                .into_inner();
            if let Some(queued) = queued.actions.first() {
                return queued.action.clone().unwrap();
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the held Emission becomes a required action")
}

async fn emitted_values(
    client: &mut management::activity_service_client::ActivityServiceClient<
        tonic::transport::Channel,
    >,
    token: &str,
    workspace: &coral_workspace::WorkspaceId,
    run: &coral_run::RunId,
) -> Vec<serde_json::Value> {
    let detail = client
        .inspect_run(request(
            wire::InspectRunRequest {
                name: format!("workspaces/{workspace}/runs/{run}"),
                ..Default::default()
            },
            token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    detail
        .events
        .iter()
        .filter_map(|event| match &event.payload {
            Some(wire::activity_run_event::Payload::ProgramEmitted(value)) => {
                Some(serde_json::from_str(&value.value_json).unwrap())
            }
            _ => None,
        })
        .collect()
}

/// coral-run owns what an approval-gated Emission does. This checks its projection:
/// the queued card, the review of the exact value, a concealed decision, and the
/// Emission that approval writes.
#[tokio::test]
async fn an_emission_approval_projects_the_exact_value_without_a_provider() {
    let f = Fixture::new().await;
    enable_emission_review(&f).await;
    let member = AuthenticatedPrincipal::user(f.member.clone());
    let run = f
        .runs
        .start(
            &member,
            f.workspace.id(),
            coral_run::RunSubmission::new(
                coral_run::ProgramSource::parse(
                    r#"await coral.emit({ invoice: "INV-7" }); return "done";"#,
                )
                .unwrap(),
                coral_run::RunTimeout::from_millis(60_000).unwrap(),
            ),
        )
        .await
        .unwrap()
        .run_id()
        .clone();
    let token = f.token(&f.member, "reef");
    let mut client = f.activity();
    let action = first_queued_action(&mut client, &token).await;
    assert_eq!(action.call_path, "coral.emit");
    assert_eq!(action.provider_label, "", "an Emission has no Provider");
    let Some(wire::activity_request_reference::Request::Approval(approval)) =
        action.request.unwrap().request
    else {
        panic!("an Emission request is an Approval Request")
    };
    let review = client
        .inspect_approval(request(
            wire::InspectApprovalRequest {
                name: approval.clone(),
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(review.subject(), wire::ActivityApprovalSubject::Emission);
    assert_eq!(review.call_path, "coral.emit");
    assert!(review.invocation.is_empty() && review.provider.is_empty());
    assert!(review.provider_label.is_empty());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&review.arguments_json).unwrap(),
        serde_json::json!({"invoice": "INV-7", "reviewed": true})
    );
    assert!(review.actionable);
    let concealed = client
        .decide_approval(request(
            wire::DecideApprovalRequest {
                name: approval.clone(),
                decision: "approve".into(),
                comment: None,
            },
            &f.token(&f.owner, "reef"),
            "reef",
        ))
        .await
        .unwrap_err();
    assert_eq!(
        concealed.code(),
        tonic::Code::NotFound,
        "only the invoking User decides"
    );
    let decided = client
        .decide_approval(request(
            wire::DecideApprovalRequest {
                name: approval,
                decision: "approve".into(),
                comment: None,
            },
            &token,
            "reef",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(decided.state, "approved");
    f.settle(&member, &run).await;
    let emitted = emitted_values(&mut client, &token, f.workspace.id(), &run).await;
    assert_eq!(
        emitted,
        [serde_json::json!({"invoice": "INV-7", "reviewed": true})]
    );
}
