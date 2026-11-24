use coral_credentials::{CredentialVault, Secret};
use coral_db::UnixNanos;
use coral_outbound::{DestinationPolicy, NetworkAccess};
use coral_test_support::{TestDatabase, provision_user};
use coral_workspace::{
    AuthenticatedUser, ChangeContext, IfMatch, OrganizationModule, OrganizationRole, TraceId,
};

use crate::agent_models::key_record;
use crate::{
    AgentModel, AgentModels, ApiKeyChange, EnteredSettings, Error, InvalidSetting, ModelProtocol,
    ModelSettings,
};

const URL_A: &str = "https://a.example/v1";
const URL_B: &str = "https://b.example/v1";

struct Fixture {
    db: TestDatabase,
    _vault_dir: tempfile::TempDir,
    vault: CredentialVault,
    models: AgentModels,
    owner: AuthenticatedUser,
    other_owner: AuthenticatedUser,
    member: AuthenticatedUser,
}

impl Fixture {
    async fn new() -> Self {
        let db = TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("test database");
        let owner = provision_user(db.database(), "owner", OrganizationRole::Owner).await;
        let other_owner = provision_user(db.database(), "other", OrganizationRole::Owner).await;
        let member = provision_user(db.database(), "member", OrganizationRole::Member).await;
        let vault_dir = tempfile::tempdir().expect("vault directory");
        let vault = CredentialVault::open(&vault_dir.path().join("key")).expect("vault");
        let models = AgentModels::new(db.database(), vault.clone(), DestinationPolicy::default());
        Self {
            db,
            _vault_dir: vault_dir,
            vault,
            models,
            owner,
            other_owner,
            member,
        }
    }

    async fn set(&self, base_url: &str, key: Option<&str>) -> AgentModel {
        self.models
            .set(&at(&self.owner, 1), &settings(base_url), key.map(secret))
            .await
            .expect("set a model")
    }

    async fn change(
        &self,
        read: &AgentModel,
        base_url: &str,
        key: ApiKeyChange,
    ) -> Result<AgentModel, Error> {
        self.models
            .change(&at(&self.owner, 2), &etag(read), &settings(base_url), key)
            .await
    }

    async fn read(&self) -> Option<AgentModel> {
        self.models.read(&self.owner).await.expect("read the model")
    }

    /// The stored key, opened for the model's own id and base URL.
    async fn stored_key(&self) -> Option<String> {
        let model = self.read().await.expect("a model is set");
        let sealed = self.sealed_key().await?;
        let record = key_record(model.id(), model.settings().base_url());
        let opened = self.vault.open_sealed(&record, &sealed);
        Some(opened.expect("the key opens").to_string())
    }

    async fn sealed_key(&self) -> Option<String> {
        let database = self.db.database();
        let mut tx = database.begin_read_snapshot().await.expect("snapshot");
        let stored = crate::repository::find(&mut tx, coral_db::RowLock::None).await;
        let stored = stored.expect("read the row").expect("a model is set");
        stored.sealed_api_key
    }
}

fn at(actor: &AuthenticatedUser, instant: i64) -> ChangeContext {
    ChangeContext::new(
        actor.clone(),
        UnixNanos::new(instant),
        TraceId::current_or_new(),
    )
}

fn settings(base_url: &str) -> EnteredSettings {
    EnteredSettings {
        protocol: ModelProtocol::Anthropic,
        base_url: base_url.to_owned(),
        model: "model-1".to_owned(),
        context_window: None,
        request_args_json: String::new(),
    }
}

fn secret(value: &str) -> Secret {
    Secret::new(value.to_owned()).expect("a secret")
}

fn etag(model: &AgentModel) -> IfMatch {
    IfMatch::parse(&model.etag().to_string()).expect("an etag")
}

// Catches: a Member, or an Owner who was disabled, who reads or changes the model, and a denial
// that changes it all the same.
#[tokio::test]
async fn only_an_enabled_organization_owner_reads_or_changes_the_model() {
    let f = Fixture::new().await;
    let model = f.set(URL_A, Some("key")).await;
    OrganizationModule::new(f.db.database())
        .set_enabled(
            &at(&f.owner, 1),
            f.other_owner.user_id(),
            &IfMatch::any(),
            false,
        )
        .await
        .expect("disable an Owner");

    let if_match = etag(&model);
    for denied in [&f.member, &f.other_owner] {
        let context = at(denied, 2);
        // A setting that is invalid is not reported to a caller who is denied.
        let settings = settings("not a URL");
        let change = f
            .models
            .change(&context, &if_match, &settings, ApiKeyChange::Clear);
        let outcomes = [
            f.models.read(denied).await.map(|_model| ()),
            f.models
                .set(&context, &settings, None)
                .await
                .map(|_model| ()),
            change.await.map(|_model| ()),
            f.models.remove(&context, &if_match).await,
        ];
        for outcome in outcomes {
            assert!(
                matches!(outcome, Err(Error::PermissionDenied)),
                "{denied:?}: {outcome:?}"
            );
        }
    }
    assert_eq!(f.read().await, Some(model));
    assert_eq!(f.stored_key().await.as_deref(), Some("key"));
}

// Catches: a new base URL that keeps the id or the key of the old one, a return to an earlier base
// URL that revives its id, a spelling of the same URL read as a new one, and any other change that
// issues a new id.
#[tokio::test]
async fn only_a_new_base_url_issues_a_new_id_and_drops_the_key() {
    let f = Fixture::new().await;
    let first = f.set(URL_A, Some("key")).await;

    let respelled = f.change(&first, "HTTPS://A.example:443/v1/", ApiKeyChange::Keep);
    let respelled = respelled.await.expect("respell the URL");
    assert_eq!(respelled.id(), first.id());
    assert_eq!(respelled.settings().base_url(), URL_A);
    assert_eq!(f.stored_key().await.as_deref(), Some("key"));

    let entered = EnteredSettings {
        protocol: ModelProtocol::OpenAiResponses,
        model: "model-2".to_owned(),
        context_window: Some(200_000),
        request_args_json: r#"{"top_p": 0.5}"#.to_owned(),
        ..settings(URL_A)
    };
    let context = at(&f.owner, 2);
    let renamed = f
        .models
        .change(&context, &etag(&respelled), &entered, ApiKeyChange::Keep)
        .await;
    let renamed = renamed.expect("change all but the URL");
    assert_eq!(renamed.id(), first.id());

    let moved = f.change(&renamed, URL_B, ApiKeyChange::Keep);
    let moved = moved.await.expect("move");
    assert_ne!(moved.id(), first.id());
    assert!(!moved.has_api_key());
    assert_eq!(f.sealed_key().await, None);

    let back = f.change(&moved, URL_A, ApiKeyChange::Keep);
    let back = back.await.expect("move back");
    assert!(![first.id(), moved.id()].contains(&back.id()));
    assert!(!back.has_api_key());
}

// Catches: a key kept, replaced or cleared when another was asked for, and a sealed key that opens
// for another model or another base URL.
#[tokio::test]
async fn a_change_keeps_replaces_or_clears_the_key_which_opens_for_its_destination_alone() {
    let f = Fixture::new().await;
    let model = f.set(URL_A, None).await;
    assert!(!model.has_api_key());

    let model = f.change(&model, URL_A, ApiKeyChange::Set(secret("one")));
    let model = model.await.expect("set a key");
    assert!(model.has_api_key());
    assert_eq!(f.stored_key().await.as_deref(), Some("one"));

    let model = f.change(&model, URL_A, ApiKeyChange::Keep);
    let model = model.await.expect("keep the key");
    assert_eq!(f.stored_key().await.as_deref(), Some("one"));

    let model = f.change(&model, URL_B, ApiKeyChange::Set(secret("two")));
    let model = model.await.expect("move with a key");
    assert_eq!(f.stored_key().await.as_deref(), Some("two"));

    let sealed = f.sealed_key().await.expect("a sealed key");
    for (id, base_url) in [(model.id(), URL_A), (uuid::Uuid::new_v4(), URL_B)] {
        let record = key_record(id, base_url);
        assert!(f.vault.open_sealed(&record, &sealed).is_err(), "{record}");
    }

    let model = f.change(&model, URL_B, ApiKeyChange::Clear);
    assert!(!model.await.expect("clear the key").has_api_key());
    assert_eq!(f.sealed_key().await, None);
}

// Catches: a save or a removal over a change its caller did not read, and an etag that outlives its
// model.
#[tokio::test]
async fn a_stale_etag_changes_nothing_even_after_the_model_was_removed_and_set_again() {
    let f = Fixture::new().await;
    let first = f.set(URL_A, None).await;
    let second = f.change(&first, URL_A, ApiKeyChange::Keep);
    let second = second.await.expect("change");

    let stale = f.change(&first, URL_B, ApiKeyChange::Keep).await;
    assert!(matches!(stale, Err(Error::EtagMismatch)), "{stale:?}");
    let stale = f.models.remove(&at(&f.owner, 3), &etag(&first)).await;
    assert!(matches!(stale, Err(Error::EtagMismatch)), "{stale:?}");
    assert_eq!(f.read().await.as_ref(), Some(&second));

    f.models
        .remove(&at(&f.owner, 3), &etag(&second))
        .await
        .expect("remove");
    assert_eq!(f.read().await, None);
    let gone = f.change(&second, URL_A, ApiKeyChange::Keep).await;
    assert!(matches!(gone, Err(Error::NotSet)), "{gone:?}");
    let gone = f.models.remove(&at(&f.owner, 3), &etag(&second)).await;
    assert!(matches!(gone, Err(Error::NotSet)), "{gone:?}");

    let again = f.set(URL_A, None).await;
    assert_ne!(again.id(), first.id());
    let stale = f.change(&first, URL_B, ApiKeyChange::Keep).await;
    assert!(matches!(stale, Err(Error::EtagMismatch)), "{stale:?}");
    assert_eq!(f.read().await, Some(again));
}

// Catches: two models, or a second set that replaces the first.
#[tokio::test]
async fn an_organization_has_one_model_and_the_first_set_stands() {
    let f = Fixture::new().await;
    let set = |actor: &AuthenticatedUser, base_url: &str| {
        let (models, context) = (f.models.clone(), at(actor, 1));
        let settings = settings(base_url);
        async move { models.set(&context, &settings, None).await }
    };
    let (one, two) = tokio::join!(set(&f.owner, URL_A), set(&f.other_owner, URL_B));
    let (stood, refused) = if one.is_ok() { (one, two) } else { (two, one) };
    assert!(matches!(refused, Err(Error::AlreadySet)), "{refused:?}");
    assert_eq!(f.read().await, stood.ok());
}

// Catches: a setting that is lost on its way to the row, and a change credited to the wrong Owner.
#[tokio::test]
async fn the_model_reads_back_as_saved_with_who_changed_it_and_when() {
    let f = Fixture::new().await;
    let model = f.set(URL_A, None).await;
    assert_eq!(
        (model.changed_by(), model.changed_at()),
        (f.owner.user_id(), UnixNanos::new(1))
    );

    let entered = EnteredSettings {
        protocol: ModelProtocol::ChatCompletions,
        model: " model-2 ".to_owned(),
        context_window: Some(crate::CONTEXT_WINDOW_MIN),
        request_args_json:
            r#"{"chat_template_kwargs": {"enable_thinking": true}, "temperature": 0.2}"#.to_owned(),
        ..settings(URL_A)
    };
    let context = at(&f.other_owner, 9);
    let changed = f
        .models
        .change(&context, &etag(&model), &entered, ApiKeyChange::Keep)
        .await;
    let changed = changed.expect("change");
    let saved = changed.settings();
    assert_eq!(saved.protocol(), entered.protocol);
    assert_eq!((saved.base_url(), saved.model()), (URL_A, "model-2"));
    assert_eq!(saved.context_window(), entered.context_window);
    let args = serde_json::Value::Object(saved.request_args().clone());
    assert_eq!(
        args,
        serde_json::from_str::<serde_json::Value>(&entered.request_args_json).expect("JSON")
    );
    assert_eq!(
        (changed.changed_by(), changed.changed_at()),
        (f.other_owner.user_id(), UnixNanos::new(9))
    );
    assert_eq!(f.read().await, Some(changed));
}

// Catches: a setting that a request could not use, or that would send data somewhere else, saved.
#[test]
fn settings_that_cannot_be_used_are_refused_by_the_setting_to_correct() {
    use InvalidSetting::{BaseUrl, ContextWindow, Destination, Model, RequestArgName, RequestArgs};

    let refused = |policy, base_url: &str, model: &str, context_window, args: &str| {
        let entered = EnteredSettings {
            protocol: ModelProtocol::ChatCompletions,
            base_url: base_url.to_owned(),
            model: model.to_owned(),
            context_window,
            request_args_json: args.to_owned(),
        };
        match ModelSettings::parse(policy, &entered) {
            Ok(_settings) => None,
            Err(Error::Invalid(setting)) => Some(setting),
            Err(other) => panic!("{entered:?}: {other:?}"),
        }
    };
    let closed = DestinationPolicy::default();
    let loopback = closed.with_loopback(NetworkAccess::HttpAndHttps);

    let long = format!("https://a.example/{}", "v".repeat(2_048));
    for (policy, base_url, expected) in [
        (closed, URL_A, None),
        (loopback, "http://localhost:8000/v1", None),
        (closed, "a.example/v1", Some(BaseUrl)),
        (closed, "ftp://a.example/v1", Some(BaseUrl)),
        (closed, "https://u:p@a.example/v1", Some(BaseUrl)),
        (closed, "https://a.example/v1?k=v", Some(BaseUrl)),
        (closed, "https://a.example/v1#f", Some(BaseUrl)),
        (closed, &long, Some(BaseUrl)),
        (closed, "http://a.example/v1", Some(Destination)),
        (closed, "http://localhost:8000/v1", Some(Destination)),
        (closed, "https://169.254.169.254/v1", Some(Destination)),
    ] {
        assert_eq!(
            refused(policy, base_url, "m", None, ""),
            expected,
            "{base_url}"
        );
    }

    let (longest, too_long) = ("m".repeat(200), "m".repeat(201));
    for (model, expected) in [
        (longest.as_str(), None),
        ("  ", Some(Model)),
        ("a\nb", Some(Model)),
        (&too_long, Some(Model)),
    ] {
        assert_eq!(
            refused(closed, URL_A, model, None, ""),
            expected,
            "{model:?}"
        );
    }

    let fewest = crate::CONTEXT_WINDOW_MIN;
    for (window, expected) in [(fewest, None), (fewest - 1, Some(ContextWindow))] {
        assert_eq!(
            refused(closed, URL_A, "m", Some(window), ""),
            expected,
            "{window}"
        );
    }

    let every = crate::REQUEST_ARG_NAMES.map(|name| format!("\"{name}\": null"));
    let every = format!("{{{}}}", every.join(","));
    let large = format!(r#"{{"thinking": "{}"}}"#, "t".repeat(16 * 1024));
    for (args, expected) in [
        (" ", None),
        ("{}", None),
        (every.as_str(), None),
        ("[]", Some(RequestArgs)),
        ("{", Some(RequestArgs)),
        (&large, Some(RequestArgs)),
        (r#"{"top_p": 1, "mcp_servers": []}"#, Some(RequestArgName)),
        (r#"{"model": "other"}"#, Some(RequestArgName)),
    ] {
        assert_eq!(refused(closed, URL_A, "m", None, args), expected, "{args}");
    }
}
