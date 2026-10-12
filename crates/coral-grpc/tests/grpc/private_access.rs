use super::{Fixture, change, internal, request};

#[tokio::test]
async fn duplicate_metadata_rejects_an_otherwise_valid_agent_in_both_authentication_modes() {
    for authenticated in [true, false] {
        let f = Fixture::with_session_auth(authenticated).await;
        let issued = f
            .workspaces
            .create_agent(
                &change(&f.owner),
                f.workspace.id(),
                coral_workspace::AgentDisplayName::parse("agent").unwrap(),
                None,
            )
            .await
            .unwrap();
        let principal = f
            .workspaces
            .authenticate_agent(f.workspace.id(), issued.credential.expose(), 1)
            .await
            .unwrap();
        let task = f
            .tasks
            .start(
                &principal,
                f.workspace.id(),
                coral_task::TaskIntent::parse("Private goal").unwrap(),
                1,
            )
            .await
            .unwrap();
        let message = internal::GetTaskRequest {
            name: format!("workspaces/{}/tasks/{}", f.workspace.id(), task.id()),
        };
        let mut client = f.internal_task_service();
        let valid = || request(message.clone(), issued.credential.expose(), "mcp");
        assert_eq!(
            client.get_task(valid()).await.unwrap_err().code(),
            tonic::Code::NotFound
        );
        for header in ["authorization", "coral-audience"] {
            let mut ambiguous = valid();
            let duplicate = ambiguous.metadata().get(header).unwrap().clone();
            ambiguous.metadata_mut().append(header, duplicate);
            assert_eq!(
                client.get_task(ambiguous).await.unwrap_err().code(),
                tonic::Code::Unauthenticated
            );
        }
    }
}

#[tokio::test]
async fn malformed_credentials_never_select_local_authority() {
    use coral_grpc::coral::management::v1::GetAgentRegistrationRequest;
    let f = Fixture::with_session_auth(false).await;
    let mut client = f.registrations();
    let mut request = tonic::Request::new(GetAgentRegistrationRequest {
        name: format!(
            "workspaces/{}/agents/absent/registration",
            uuid::Uuid::new_v4()
        ),
    });
    request
        .metadata_mut()
        .insert("coral-audience", "reef".parse().unwrap());
    request
        .metadata_mut()
        .insert("authorization", "garbage".parse().unwrap());
    assert_eq!(
        client
            .get_agent_registration(request)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
}
