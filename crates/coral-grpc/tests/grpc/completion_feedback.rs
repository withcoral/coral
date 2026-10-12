use super::{Fixture, change, management as wire, request};
use coral_workspace::AuthenticatedPrincipal;

#[tokio::test]
async fn completion_feedback_is_exact_task_detail_with_no_owner_exception_for_users() {
    let f = Fixture::new().await;
    let issued = f
        .workspaces
        .create_agent(
            &change(&f.owner),
            f.workspace.id(),
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = f
        .workspaces
        .authenticate_agent(f.workspace.id(), issued.credential.expose(), 1)
        .await
        .unwrap();
    let mut client = f.activity();
    for actor in [AuthenticatedPrincipal::user(f.member.clone()), agent] {
        let task = f.task(&actor).await;
        let name = format!("workspaces/{}/tasks/{}", f.workspace.id(), task.id());
        let token = if actor.authenticated_user().is_some() {
            f.token(&f.member, "reef")
        } else {
            f.token(&f.owner, "reef")
        };
        let inspect = || wire::InspectTaskRequest {
            name: name.clone(),
            ..Default::default()
        };
        assert_eq!(
            client
                .inspect_task(request(inspect(), &token, "reef"))
                .await
                .unwrap()
                .into_inner()
                .feedback,
            None
        );
        f.tasks
            .end(
                &actor,
                f.workspace.id(),
                task.id(),
                coral_task::TaskOutcome::Failure,
                Some("Private completion 🪸"),
                i64::MAX / 2,
            )
            .await
            .unwrap();
        let exact = client
            .inspect_task(request(inspect(), &token, "reef"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(exact.feedback.as_deref(), Some("Private completion 🪸"));
        assert!(exact.interactions.is_empty());
        let rows = client
            .browse_activity_roots(request(
                wire::BrowseActivityRootsRequest::default(),
                &token,
                "reef",
            ))
            .await
            .unwrap()
            .into_inner();
        assert!(
            !String::from_utf8_lossy(&prost::Message::encode_to_vec(&rows))
                .contains("Private completion")
        );
        if actor.authenticated_user().is_some() {
            assert_eq!(
                client
                    .inspect_task(request(inspect(), &f.token(&f.owner, "reef"), "reef"))
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::NotFound
            );
        } else {
            assert_eq!(
                client
                    .inspect_task(request(inspect(), &f.token(&f.member, "reef"), "reef"))
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::NotFound
            );
            assert_eq!(
                client
                    .inspect_task(request(inspect(), issued.credential.expose(), "reef"))
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::Unauthenticated
            );
        }
    }
}
