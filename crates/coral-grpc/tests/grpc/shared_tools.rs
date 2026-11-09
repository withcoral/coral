//! The Shared Tool service's contract: its status mapping and projection.
//! Publishing rules are tested at their owner, coral-tools.
use super::{Fixture, change, error_reason, management, request};
use coral_tools::{
    PrivateToolChangeContext, PrivateToolDescription, PrivateToolPath, PrivateToolSave,
    PrivateToolSource,
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
                PrivateToolDescription::parse("A draft").unwrap(),
                PrivateToolSource::parse(SOURCE).unwrap(),
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
    let outsider = AuthenticatedUser::organization_member(
        UserId::parse("outsider").unwrap(),
        OrganizationRole::Member,
    );
    OrganizationModule::new(f.database.database())
        .provision_login(&change(&outsider))
        .await
        .unwrap();
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
    let revisions = f
        .shared_tools()
        .list_shared_tool_revisions(request(
            management::ListSharedToolRevisionsRequest {
                parent: parent.clone(),
                path: "mail.remind".into(),
            },
            &token,
            "reef",
        ))
        .await
        .expect("list revisions")
        .into_inner()
        .revisions;
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
    let revision = f
        .shared_tools()
        .get_shared_tool_revision(request(
            management::GetSharedToolRevisionRequest {
                parent,
                path: "mail.remind".into(),
                revision: 1,
            },
            &token,
            "reef",
        ))
        .await
        .expect("get the revision")
        .into_inner();
    assert_eq!(revision, current);
}
