//! The Shared Tool service's contract: its status mapping and projection.
//! Publishing and removal rules are tested at their owner, coral-tools.
use super::{Fixture, change, error_info, error_reason, management, request};
use coral_tools::{
    AuthoredToolDescription, AuthoredToolSource, PrivateToolChangeContext, PrivateToolPath,
    PrivateToolSave,
};
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, OrganizationModule, OrganizationRole, TraceId,
    UserId,
};
use management::publish_shared_tool_request::Origin;

const SOURCE: &str =
    "export default async function remind(name: string): Promise<string> { return name; }";

/// Saves `user`'s Private Tool `draft`, which each publish copies.
async fn save_draft(f: &Fixture, user: &AuthenticatedUser) {
    f.private_tools
        .save(
            &PrivateToolChangeContext::new(
                AuthenticatedPrincipal::user(user.clone()),
                1_700_000_000_000_000_007,
                TraceId::current_or_new(),
            ),
            f.workspace.id(),
            PrivateToolSave::new(
                PrivateToolPath::parse("draft").unwrap(),
                AuthoredToolDescription::parse("A draft").unwrap(),
                AuthoredToolSource::parse(SOURCE).unwrap(),
            ),
        )
        .await
        .unwrap();
}

/// A publish of the caller's own `draft` at `path`.
fn publish(
    f: &Fixture,
    path: &str,
    expected_revision: u64,
) -> management::PublishSharedToolRequest {
    management::PublishSharedToolRequest {
        parent: format!("workspaces/{}", f.workspace.id()),
        path: path.into(),
        origin: Some(Origin::PrivateTool(management::PrivateToolOrigin {
            agent: String::new(),
            path: "draft".into(),
        })),
        description: "Remind someone".into(),
        source: SOURCE.into(),
        default_access_state: management::ToolAccessState::ApprovalRequired.into(),
        expected_revision,
        validate_only: false,
    }
}

async fn send(
    f: &Fixture,
    user: &AuthenticatedUser,
    publish: management::PublishSharedToolRequest,
) -> Result<management::PublishSharedToolResponse, tonic::Status> {
    f.shared_tools()
        .publish_shared_tool(request(publish, &f.token(user, "reef"), "reef"))
        .await
        .map(tonic::Response::into_inner)
}

// Catches: a publish refusal that reaches Reef with the wrong status or
// without the reason that names what to fix.
#[tokio::test]
async fn a_refused_publish_maps_to_its_status_and_reason() {
    let f = Fixture::new().await;
    save_draft(&f, &f.owner).await;
    save_draft(&f, &f.member).await;
    let outsider = outsider(&f).await;
    send(&f, &f.owner, publish(&f, "mail.remind", 0))
        .await
        .expect("publish the first revision");

    let missing_origin = management::PublishSharedToolRequest {
        origin: Some(Origin::PrivateTool(management::PrivateToolOrigin {
            agent: String::new(),
            path: "absent".into(),
        })),
        ..publish(&f, "mail.other", 0)
    };
    let cases = [
        (
            "a Member publishes",
            &f.member,
            publish(&f, "mail.other", 0),
            (tonic::Code::PermissionDenied, "PERMISSION_DENIED"),
        ),
        (
            "the source cannot be read",
            &f.owner,
            missing_origin,
            (
                tonic::Code::FailedPrecondition,
                "SHARED_TOOL_SOURCE_UNREADABLE",
            ),
        ),
        (
            "the path is taken",
            &f.owner,
            publish(&f, "mail", 0),
            (tonic::Code::FailedPrecondition, "SHARED_TOOL_PATH_TAKEN"),
        ),
        (
            "the revision moved",
            &f.owner,
            publish(&f, "mail.remind", 0),
            (tonic::Code::Aborted, "ABORTED"),
        ),
        (
            "the caller is outside the Workspace",
            &outsider,
            publish(&f, "mail.other", 0),
            (tonic::Code::NotFound, "NOT_FOUND"),
        ),
    ];
    for (case, user, publish, expected) in cases {
        let status = send(&f, user, publish).await.expect_err(case);
        assert_eq!(
            (status.code(), error_reason(&status).as_str()),
            expected,
            "{case}"
        );
    }

    let read = async |user: &AuthenticatedUser, path: &str| {
        f.shared_tools()
            .get_shared_tool(request(
                management::GetSharedToolRequest {
                    parent: format!("workspaces/{}", f.workspace.id()),
                    path: path.into(),
                },
                &f.token(user, "reef"),
                "reef",
            ))
            .await
    };
    for (case, user, path) in [
        ("an absent path", &f.member, "mail.absent"),
        ("a caller outside the Workspace", &outsider, "mail.remind"),
    ] {
        let status = read(user, path).await.expect_err(case);
        assert_eq!(
            (status.code(), error_reason(&status).as_str()),
            (tonic::Code::NotFound, "NOT_FOUND"),
            "{case}"
        );
    }
}

// Catches: a projection that drops or mislabels what Reef shows of a
// publish: the review, and the new revision.
#[tokio::test]
async fn a_publish_projects_its_review_and_the_shared_tool() {
    let f = Fixture::new().await;
    save_draft(&f, &f.owner).await;

    let validated = send(
        &f,
        &f.owner,
        management::PublishSharedToolRequest {
            validate_only: true,
            ..publish(&f, "mail.remind", 0)
        },
    )
    .await
    .expect("validate");
    assert_eq!(validated.shared_tool, None);
    let review = validated.review.expect("the review");
    assert_eq!(review.previous_revision, None);
    assert!(review.declaration.contains("name: string"), "{review:?}");

    let published = send(&f, &f.owner, publish(&f, "mail.remind", 0))
        .await
        .expect("publish");
    let tool = published.shared_tool.expect("the Shared Tool");
    let current = tool.current_revision.clone().expect("the current revision");
    assert_eq!(
        (tool.path.as_str(), tool.creator.as_str()),
        ("mail.remind", "users/owner")
    );
    assert_eq!(
        (
            current.revision,
            current.description.as_str(),
            current.source.as_str(),
            current.publisher.as_str(),
            current.default_access_state(),
            current.restored_from_revision,
            current.content_hash.len(),
        ),
        (
            1,
            "Remind someone",
            SOURCE,
            "users/owner",
            management::ToolAccessState::ApprovalRequired,
            0,
            64,
        )
    );
}

// Catches: a publish the adapter does not type from the Tools its Owner can
// discover, so its review shows no diagnostic that a Private Tool save has.
#[tokio::test]
async fn a_publish_review_is_typed_from_the_tools_its_owner_can_discover() {
    let f = Fixture::new().await;
    f.install_builtin().await;
    save_draft(&f, &f.owner).await;

    let validated = send(
        &f,
        &f.owner,
        management::PublishSharedToolRequest {
            // `coral.listProviders` takes no second argument.
            source: "export default async function listed() { return coral.listProviders({}, 1); }"
                .into(),
            validate_only: true,
            ..publish(&f, "ops.listed", 0)
        },
    )
    .await
    .expect("validate");
    let review = validated.review.expect("the review");
    assert!(
        review
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == 2554),
        "{:?}",
        review.diagnostics
    );
}

// Catches: a read that hides a Shared Tool from a Member, or that drops or
// mislabels its current revision, its summary or its revisions.
#[tokio::test]
async fn a_member_reads_the_shared_tool_its_summary_and_its_revisions() {
    let f = Fixture::new().await;
    save_draft(&f, &f.owner).await;
    let parent = format!("workspaces/{}", f.workspace.id());
    let tool = send(&f, &f.owner, publish(&f, "mail.remind", 0))
        .await
        .expect("publish")
        .shared_tool
        .expect("the Shared Tool");
    let current = tool.current_revision.clone().expect("the current revision");

    let token = f.token(&f.member, "reef");
    let read = f
        .shared_tools()
        .get_shared_tool(request(
            management::GetSharedToolRequest {
                parent: parent.clone(),
                path: "mail.remind".into(),
            },
            &token,
            "reef",
        ))
        .await
        .expect("get")
        .into_inner();
    assert_eq!(read, tool);
    let listed = f
        .shared_tools()
        .list_shared_tools(request(
            management::ListSharedToolsRequest {
                parent: parent.clone(),
                page_size: 10,
                page_token: String::new(),
            },
            &token,
            "reef",
        ))
        .await
        .expect("list")
        .into_inner();
    assert!(
        matches!(
            listed.shared_tools.as_slice(),
            [summary] if summary.path == "mail.remind" && summary.revision == 1
        ),
        "{listed:?}"
    );
    assert!(
        listed
            .shared_provider
            .starts_with(&format!("{parent}/providers/")),
        "{listed:?}"
    );
    assert_eq!(
        listed.shared_tools.first().map(|summary| &summary.name),
        Some(&tool.name)
    );
    assert!(
        tool.name.starts_with(&format!("{parent}/sharedTools/")),
        "{tool:?}"
    );
    let revisions = revisions(&f, &f.member, &tool.name).await;
    assert!(
        matches!(
            revisions.as_slice(),
            [summary]
                if summary.revision == 1
                    && summary.content_hash == current.content_hash
                    && summary.publisher == "users/owner"
        ),
        "{revisions:?}"
    );
    assert_eq!(first_revision(&f, &f.member, &tool.name).await, current);
}

/// A User of the Organization with no Membership of the Workspace.
async fn outsider(f: &Fixture) -> AuthenticatedUser {
    let outsider = AuthenticatedUser::organization_member(
        UserId::parse("outsider").unwrap(),
        OrganizationRole::Member,
    );
    OrganizationModule::new(f.database.database())
        .provision_login(&change(&outsider))
        .await
        .unwrap();
    outsider
}

/// The source-free revisions of the Shared Tool named `shared_tool`.
async fn revisions(
    f: &Fixture,
    user: &AuthenticatedUser,
    shared_tool: &str,
) -> Vec<management::SharedToolRevisionSummary> {
    f.shared_tools()
        .list_shared_tool_revisions(request(
            management::ListSharedToolRevisionsRequest {
                shared_tool: shared_tool.into(),
            },
            &f.token(user, "reef"),
            "reef",
        ))
        .await
        .expect("list revisions")
        .into_inner()
        .revisions
}

/// Revision 1 of the Shared Tool named `shared_tool`.
async fn first_revision(
    f: &Fixture,
    user: &AuthenticatedUser,
    shared_tool: &str,
) -> management::SharedToolRevision {
    f.shared_tools()
        .get_shared_tool_revision(request(
            management::GetSharedToolRevisionRequest {
                shared_tool: shared_tool.into(),
                revision: 1,
            },
            &f.token(user, "reef"),
            "reef",
        ))
        .await
        .expect("get the revision")
        .into_inner()
}

/// A removal of the Shared Tool at `path`.
async fn remove(
    f: &Fixture,
    user: &AuthenticatedUser,
    path: &str,
    expected_revision: u64,
) -> Result<management::RemoveSharedToolResponse, tonic::Status> {
    f.shared_tools()
        .remove_shared_tool(request(
            management::RemoveSharedToolRequest {
                parent: format!("workspaces/{}", f.workspace.id()),
                path: path.into(),
                expected_revision,
            },
            &f.token(user, "reef"),
            "reef",
        ))
        .await
        .map(tonic::Response::into_inner)
}

/// One page of Shared Tool events, of `shared_tool` alone unless it is
/// empty.
async fn events(
    f: &Fixture,
    user: &AuthenticatedUser,
    shared_tool: &str,
    page_size: i32,
    page_token: &str,
) -> Result<management::ListSharedToolEventsResponse, tonic::Status> {
    f.shared_tools()
        .list_shared_tool_events(request(
            management::ListSharedToolEventsRequest {
                parent: format!("workspaces/{}", f.workspace.id()),
                shared_tool: shared_tool.into(),
                page_size,
                page_token: page_token.into(),
            },
            &f.token(user, "reef"),
            "reef",
        ))
        .await
        .map(tonic::Response::into_inner)
}

// Catches: a removal refusal that reaches Reef with the wrong status, or a
// refusal over dependents that does not say which Shared Tool to deal with.
#[tokio::test]
async fn a_refused_removal_maps_to_its_status_and_reason() {
    let f = Fixture::new().await;
    save_draft(&f, &f.owner).await;
    send(&f, &f.owner, publish(&f, "mail.remind", 0))
        .await
        .expect("publish the Shared Tool");
    send(
        &f,
        &f.owner,
        management::PublishSharedToolRequest {
            source:
                "export default async function digest() { return coral.shared.mail.remind(\"a\"); }"
                    .into(),
            ..publish(&f, "mail.digest", 0)
        },
    )
    .await
    .expect("publish a Shared Tool that calls it");
    let outsider = outsider(&f).await;

    // (case, who removes, the path, the expected revision, status and reason)
    let cases = [
        (
            "a Member removes",
            &f.member,
            "mail.digest",
            1,
            (tonic::Code::PermissionDenied, "PERMISSION_DENIED"),
        ),
        (
            "no Shared Tool is at the path",
            &f.owner,
            "mail.absent",
            1,
            (tonic::Code::NotFound, "NOT_FOUND"),
        ),
        (
            "the revision moved",
            &f.owner,
            "mail.digest",
            2,
            (tonic::Code::Aborted, "ABORTED"),
        ),
        (
            "another Shared Tool calls it",
            &f.owner,
            "mail.remind",
            1,
            (
                tonic::Code::FailedPrecondition,
                "SHARED_TOOL_HAS_DEPENDENTS",
            ),
        ),
        (
            "the caller is outside the Workspace",
            &outsider,
            "mail.digest",
            1,
            (tonic::Code::NotFound, "NOT_FOUND"),
        ),
    ];
    for (case, user, path, expected_revision, expected) in cases {
        let status = remove(&f, user, path, expected_revision)
            .await
            .expect_err(case);
        assert_eq!(
            (status.code(), error_reason(&status).as_str()),
            expected,
            "{case}"
        );
        // Only the refusal over dependents names a Shared Tool.
        assert_eq!(
            error_info(&status)
                .metadata
                .get("dependent")
                .map(String::as_str),
            (expected.1 == "SHARED_TOOL_HAS_DEPENDENTS").then_some("mail.digest"),
            "{case}"
        );
    }
}

// Catches: a User outside the Workspace reading its history, or a malformed
// or foreign Shared Tool name reaching the domain.
#[tokio::test]
async fn a_refused_history_read_maps_to_its_status_and_reason() {
    let f = Fixture::new().await;
    let outsider = outsider(&f).await;
    let other_workspace = format!(
        "workspaces/{}/sharedTools/{}",
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4()
    );
    for (case, user, shared_tool, expected) in [
        (
            "the caller is outside the Workspace",
            &outsider,
            "",
            (tonic::Code::NotFound, "NOT_FOUND"),
        ),
        (
            "the name is not a Shared Tool's",
            &f.member,
            "mail.remind",
            (tonic::Code::InvalidArgument, "INVALID_ARGUMENT"),
        ),
        (
            "the name is in another Workspace",
            &f.member,
            other_workspace.as_str(),
            (tonic::Code::InvalidArgument, "INVALID_ARGUMENT"),
        ),
    ] {
        let status = events(&f, user, shared_tool, 10, "").await.expect_err(case);
        assert_eq!(
            (status.code(), error_reason(&status).as_str()),
            expected,
            "{case}"
        );
    }
}

// Catches: a projection that drops or mislabels what Reef shows of a
// removed Shared Tool: its name, its events and its kept revisions; or a
// page token that repeats or skips an event.
#[tokio::test]
async fn a_member_reads_the_history_and_the_revisions_of_a_removed_shared_tool() {
    use management::SharedToolEventAction::{Published, Removed};
    let f = Fixture::new().await;
    save_draft(&f, &f.owner).await;
    let tool = send(&f, &f.owner, publish(&f, "mail.remind", 0))
        .await
        .expect("publish")
        .shared_tool
        .expect("the Shared Tool");
    let current = tool.current_revision.clone().expect("the current revision");
    let other = send(&f, &f.owner, publish(&f, "mail.other", 0))
        .await
        .expect("publish another")
        .shared_tool
        .expect("the other Shared Tool");

    let removed = remove(&f, &f.owner, "mail.remind", 1)
        .await
        .expect("remove");
    assert_eq!(removed.shared_tool, tool.name);

    let project = |events: &[management::SharedToolEvent]| {
        events
            .iter()
            .map(|event| {
                assert!(event.event_time.is_some(), "{event:?}");
                (
                    event.action(),
                    event.shared_tool.clone(),
                    event.path.clone(),
                    event.revision,
                    event.content_hash.clone(),
                    event.actor.clone(),
                )
            })
            .collect::<Vec<_>>()
    };
    let event = |action, tool: &management::SharedTool| {
        (
            action,
            tool.name.clone(),
            tool.path.clone(),
            1,
            tool.current_revision
                .as_ref()
                .expect("the current revision")
                .content_hash
                .clone(),
            "users/owner".to_string(),
        )
    };
    let whole = events(&f, &f.member, "", 10, "")
        .await
        .expect("list events");
    assert_eq!(
        project(&whole.events),
        [
            event(Removed, &tool),
            event(Published, &other),
            event(Published, &tool),
        ]
    );
    assert_eq!(whole.next_page_token, "");
    let of_the_removed = events(&f, &f.member, &tool.name, 10, "")
        .await
        .expect("list the removed Shared Tool's events");
    assert_eq!(
        project(&of_the_removed.events),
        [event(Removed, &tool), event(Published, &tool)]
    );

    // Each page continues the one before it, in its own listing only.
    let first = events(&f, &f.member, "", 2, "").await.expect("page one");
    let second = events(&f, &f.member, "", 2, &first.next_page_token)
        .await
        .expect("page two");
    assert_eq!(
        [first.events.clone(), second.events.clone()].concat(),
        whole.events
    );
    assert_eq!(second.next_page_token, "");
    let reused = events(&f, &f.member, &tool.name, 2, &first.next_page_token)
        .await
        .expect_err("a token of another listing");
    assert_eq!(
        (reused.code(), error_reason(&reused).as_str()),
        (tonic::Code::InvalidArgument, "INVALID_ARGUMENT")
    );

    // The removed Shared Tool's name still reads its revisions.
    let revisions = revisions(&f, &f.member, &tool.name).await;
    assert!(
        matches!(
            revisions.as_slice(),
            [summary] if summary.revision == 1 && summary.content_hash == current.content_hash
        ),
        "{revisions:?}"
    );
    assert_eq!(first_revision(&f, &f.member, &tool.name).await, current);
}
