use super::*;

type Client = management::sub_agent_model_service_client::SubAgentModelServiceClient<Channel>;

fn settings(base_url: &str, args: &str) -> management::SubAgentModelSettings {
    management::SubAgentModelSettings {
        protocol: management::SubAgentModelProtocol::OpenaiResponses.into(),
        base_url: base_url.to_owned(),
        model: "model-1".to_owned(),
        context_window: Some(200_000),
        request_args_json: args.to_owned(),
    }
}

fn failure(status: &tonic::Status) -> (tonic::Code, String) {
    (status.code(), error_reason(status))
}

async fn get(client: &mut Client, token: &str) -> Option<management::SubAgentModel> {
    let request = request(management::GetSubAgentModelRequest {}, token, "reef");
    let read = client.get_sub_agent_model(request).await.unwrap();
    read.into_inner().model
}

async fn create(
    client: &mut Client,
    token: &str,
    settings: management::SubAgentModelSettings,
    api_key: Option<&str>,
) -> Result<management::SubAgentModel, (tonic::Code, String)> {
    let message = management::CreateSubAgentModelRequest {
        settings: Some(settings),
        api_key: api_key.map(str::to_owned),
    };
    let created = client.create_sub_agent_model(request(message, token, "reef"));
    created
        .await
        .map(tonic::Response::into_inner)
        .map_err(|status| failure(&status))
}

async fn update(
    client: &mut Client,
    token: &str,
    etag: &str,
    settings: management::SubAgentModelSettings,
    api_key: Option<&str>,
) -> Result<management::SubAgentModel, (tonic::Code, String)> {
    let message = management::UpdateSubAgentModelRequest {
        settings: Some(settings),
        etag: etag.to_owned(),
        api_key: api_key.map(str::to_owned),
    };
    let updated = client.update_sub_agent_model(request(message, token, "reef"));
    updated
        .await
        .map(tonic::Response::into_inner)
        .map_err(|status| failure(&status))
}

/// coral-agent-model owns the rules. This checks that each RPC reaches them,
/// and projects the model without its key.
#[tokio::test]
async fn an_organization_owner_sets_changes_and_removes_the_sub_agent_model() {
    let f = Fixture::new().await;
    let mut client = Client::new(f.channel.clone());
    let token = f.token(&f.owner, "reef");
    assert_eq!(get(&mut client, &token).await, None);

    let args = r#"{"reasoning":{"effort":"high"}}"#;
    let entered = settings("https://a.example/v1", args);
    let created = create(&mut client, &token, entered.clone(), Some("key-1")).await;
    let created = created.unwrap();
    assert_eq!(created.settings.as_ref(), Some(&entered));
    assert!(created.has_api_key);
    assert_eq!(created.changed_by, format!("users/{}", f.owner.user_id()));
    assert!(created.change_time.is_some());
    assert_eq!(get(&mut client, &token).await.as_ref(), Some(&created));

    let again = create(&mut client, &token, entered.clone(), None).await;
    let already = "SUB_AGENT_MODEL_ALREADY_SET".to_owned();
    assert_eq!(again, Err((tonic::Code::AlreadyExists, already)));

    // An absent key keeps the stored one, and an empty one removes it.
    let etag = &created.etag;
    let kept = update(&mut client, &token, etag, entered.clone(), None).await;
    let kept = kept.unwrap();
    assert!(kept.has_api_key && kept.id == created.id);
    let cleared = update(&mut client, &token, &kept.etag, entered.clone(), Some("")).await;
    let cleared = cleared.unwrap();
    assert!(!cleared.has_api_key);

    let stale = update(&mut client, &token, etag, entered, Some("key-2")).await;
    let mismatch = "ETAG_MISMATCH".to_owned();
    assert_eq!(stale, Err((tonic::Code::Aborted, mismatch)));

    let moved = settings("https://b.example/v1", "");
    let moved = update(&mut client, &token, &cleared.etag, moved, None).await;
    let moved = moved.unwrap();
    assert_ne!(moved.id, created.id);

    let message = management::DeleteSubAgentModelRequest { etag: moved.etag };
    let delete = client.delete_sub_agent_model(request(message, &token, "reef"));
    delete.await.unwrap();
    assert_eq!(get(&mut client, &token).await, None);
}

/// Each setting has a reason of its own, which Reef shows at its field.
#[tokio::test]
async fn a_refused_setting_names_itself_to_an_organization_owner_alone() {
    let f = Fixture::new().await;
    let mut client = Client::new(f.channel.clone());
    let (owner, member) = (f.token(&f.owner, "reef"), f.token(&f.member, "reef"));
    let url = "https://a.example/v1";
    let with = |change: fn(&mut management::SubAgentModelSettings)| {
        let mut changed = settings(url, "");
        change(&mut changed);
        changed
    };
    for (entered, key, reason) in [
        (settings("a.example", ""), None, "INVALID_BASE_URL"),
        (
            settings("https://10.0.0.1/v1", ""),
            None,
            "DESTINATION_NOT_PERMITTED",
        ),
        (settings(url, "[]"), None, "INVALID_REQUEST_ARGS"),
        (
            settings(url, r#"{"store": true}"#),
            None,
            "REQUEST_ARG_NOT_ALLOWED",
        ),
        (with(|s| s.model.clear()), None, "INVALID_MODEL"),
        (
            with(|s| s.context_window = Some(1)),
            None,
            "INVALID_CONTEXT_WINDOW",
        ),
        (settings(url, ""), Some("a\nb"), "INVALID_API_KEY"),
        (with(|s| s.protocol = 0), None, "INVALID_ARGUMENT"),
    ] {
        let refused = create(&mut client, &owner, entered.clone(), key).await;
        let expected = (tonic::Code::InvalidArgument, reason.to_owned());
        assert_eq!(refused, Err(expected), "{entered:?}");
    }
    assert_eq!(get(&mut client, &owner).await, None);

    let denied = create(&mut client, &member, settings("a.example", ""), None).await;
    assert_eq!(denied, Err((DENIED.0, DENIED.1.to_owned())));
    let message = request(management::GetSubAgentModelRequest {}, &member, "reef");
    let denied = client.get_sub_agent_model(message).await.unwrap_err();
    assert_eq!(failure(&denied), (DENIED.0, DENIED.1.to_owned()));
}

/// coral-policy and coral-administration own the consent. This checks that a
/// Workspace Owner reaches it, with the stable reasons Reef acts on, and that
/// nobody else reads the model through a Workspace.
#[tokio::test]
async fn a_workspace_owner_sees_the_model_and_turns_the_sub_agent_on_for_it_alone() {
    let f = Fixture::new().await;
    let mut client = Client::new(f.channel.clone());
    let (owner, member) = (f.token(&f.owner, "reef"), f.token(&f.member, "reef"));
    let workspace = format!("workspaces/{}", f.workspace.id());
    let read = |token: &str| {
        let message = management::GetWorkspaceSubAgentModelRequest {
            workspace: workspace.clone(),
        };
        let request = request(message, token, "reef");
        let mut client = Client::new(f.channel.clone());
        async move {
            let read = client.get_workspace_sub_agent_model(request).await;
            read.map(tonic::Response::into_inner)
                .map_err(|status| failure(&status))
        }
    };
    let missing = read(&owner).await;
    assert_eq!(missing, Err((NOT_FOUND.0, NOT_FOUND.1.to_owned())));

    let entered = settings("https://a.example/v1", r#"{"speed":"fast"}"#);
    let model = create(&mut client, &owner, entered.clone(), Some("key-1")).await;
    let model = model.unwrap();
    let shown = read(&owner).await.unwrap();
    assert_eq!(
        (&shown.id, shown.settings.as_ref()),
        (&model.id, Some(&entered))
    );
    let denied = read(&member).await;
    assert_eq!(denied, Err((DENIED.0, DENIED.1.to_owned())));

    let turn_on = |token: &str, model_id: &str, state| {
        let message = management::TurnOnSubAgentRequest {
            workspace: workspace.clone(),
            model_id: model_id.to_owned(),
            access_state: state,
        };
        let request = request(message, token, "reef");
        let mut client = Client::new(f.channel.clone());
        async move {
            let on = client.turn_on_sub_agent(request).await;
            on.map(|response| response.into_inner().access_state)
                .map_err(|status| failure(&status))
        }
    };
    let enabled = i32::from(management::ToolAccessState::Enabled);
    let disabled = i32::from(management::ToolAccessState::Disabled);
    let invalid = Err((INVALID.0, INVALID.1.to_owned()));
    let other = uuid::Uuid::new_v4().to_string();
    let changed = (tonic::Code::Aborted, "SUB_AGENT_MODEL_CHANGED".to_owned());
    assert_eq!(turn_on(&owner, &other, enabled).await, Err(changed));
    assert_eq!(turn_on(&owner, "not-a-uuid", enabled).await, invalid);
    assert_eq!(turn_on(&owner, &model.id, disabled).await, invalid);
    let denied = turn_on(&member, &model.id, enabled).await;
    assert_eq!(denied, Err((DENIED.0, DENIED.1.to_owned())));

    // A model is set, so "no model" would be the wrong reason.
    let (context, any) = (change(&f.owner), IfMatch::any());
    let archived = f.workspaces.archive(&context, f.workspace.id(), &any);
    archived.await.unwrap();
    let archived = (
        tonic::Code::FailedPrecondition,
        "WORKSPACE_ARCHIVED".to_owned(),
    );
    assert_eq!(read(&owner).await, Err(archived));
}

/// Answers each test with the next of its results, and keeps the key it got.
struct Tester {
    results: std::sync::Mutex<Vec<Result<(), coral_agent_model::TestFailure>>>,
    keys: std::sync::Mutex<Vec<Option<String>>>,
}

#[tonic::async_trait]
impl coral_agent_model::ModelTester for Tester {
    async fn test(
        &self,
        _settings: &coral_agent_model::ModelSettings,
        key: Option<coral_credentials::Secret>,
    ) -> Result<(), coral_agent_model::TestFailure> {
        let key = key.map(|key| key.expose().to_owned());
        self.keys.lock().unwrap().push(key);
        self.results.lock().unwrap().remove(0)
    }
}

/// coral-agent owns what a test does. This checks the key that each request
/// means, each outcome on the wire, and who may ask.
#[tokio::test]
async fn an_organization_owner_tests_a_model_with_the_key_the_request_means() {
    use coral_agent_model::TestFailure;
    use management::SubAgentModelTestOutcome as Outcome;

    let f = Fixture::new().await;
    let (owner, member) = (f.token(&f.owner, "reef"), f.token(&f.member, "reef"));
    let entered = settings("https://a.example/v1", "");
    let mut client = Client::new(f.channel.clone());
    let saved = create(&mut client, &owner, entered.clone(), Some("saved")).await;
    saved.unwrap();
    let test = |token: &str, key: Option<&str>| {
        let message = management::TestSubAgentModelRequest {
            settings: Some(entered.clone()),
            api_key: key.map(str::to_owned),
        };
        let request = request(message, token, "reef");
        let mut client = Client::new(f.channel.clone());
        async move {
            let tested = client.test_sub_agent_model(request).await;
            let tested = tested.map(tonic::Response::into_inner);
            tested
                .map(|tested| (tested.outcome(), tested.http_status))
                .map_err(|status| failure(&status))
        }
    };
    let (code, _reason) = test(&owner, None).await.unwrap_err();
    assert_eq!(code, tonic::Code::Unimplemented, "no Sub-agent is composed");

    let tester = Arc::new(Tester {
        results: std::sync::Mutex::new(vec![
            Ok(()),
            Err(TestFailure::Refused(401)),
            Err(TestFailure::Unreachable),
            Err(TestFailure::Forbidden),
            Err(TestFailure::Unreadable),
        ]),
        keys: std::sync::Mutex::default(),
    });
    f.agent_models.set_tester(tester.clone());
    let denied = test(&member, None).await;
    assert_eq!(denied, Err((DENIED.0, DENIED.1.to_owned())));
    for (key, expected) in [
        (None, (Outcome::Answered, None)),
        (Some("typed"), (Outcome::Refused, Some(401))),
        (Some(""), (Outcome::Unreachable, None)),
        (None, (Outcome::Forbidden, None)),
        (None, (Outcome::Unreadable, None)),
    ] {
        assert_eq!(test(&owner, key).await, Ok(expected), "{key:?}");
    }
    let keys = tester.keys.lock().unwrap().clone();
    let [saved, typed] = ["saved", "typed"].map(|key| Some(key.to_owned()));
    assert_eq!(keys, [saved.clone(), typed, None, saved.clone(), saved]);
}
