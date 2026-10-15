use super::{Fixture, error_reason, management, request};
use coral_middleware::{
    MiddlewareChangeContext, MiddlewareDescription, MiddlewareName, MiddlewareSave,
    MiddlewareSource,
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
