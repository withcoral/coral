use coral_edition::EnterpriseFeatureId;

use super::testing::TestModeFactory;
use super::*;

const SECRET: &str = "sk-live-0123456789abcdef";
const CORAL_URL: &str = "https://coral.example.test/";

struct Stub(&'static str, Option<EnterpriseFeature>);

impl SignInModeFactory for Stub {
    fn name(&self) -> ModeName {
        ModeName(self.0)
    }

    fn feature(&self) -> Option<EnterpriseFeature> {
        self.1.clone()
    }

    fn build(
        &self,
        _settings: &ModeSettings,
        _environment: &ModeEnvironment,
    ) -> Result<Arc<dyn SignInMode>, SettingError> {
        Err(SettingError::new("", "a stub builds nothing"))
    }
}

fn settings(raw: &str) -> (ModeSettings, ModeEnvironment) {
    let settings = ModeSettings::new(toml::from_str(raw).expect("TOML"));
    (
        settings,
        ModeEnvironment::new(Url::parse(CORAL_URL).expect("URL")),
    )
}

fn test_mode(raw: &str) -> Arc<dyn SignInMode> {
    let (settings, environment) = settings(raw);
    TestModeFactory
        .build(&settings, &environment)
        .unwrap_or_else(|error| panic!("{error}"))
}

/// A message lists each mode once, and the mode that serves is the last one
/// registered under its name.
#[test]
fn a_later_mode_replaces_one_of_the_same_name() {
    let feature = EnterpriseFeature::new(EnterpriseFeatureId::new("later"), "Later");
    let modes = SignInModes::default()
        .with(Arc::new(Stub("test", None)))
        .with(Arc::new(Stub("other", None)))
        .with(Arc::new(Stub("test", Some(feature.clone()))));
    assert_eq!(modes.names().collect::<Vec<_>>(), ["other", "test"]);
    assert_eq!(
        modes.get("test").and_then(|mode| mode.feature()),
        Some(feature)
    );
    assert!(modes.get("missing").is_none());
}

/// A mode's name is a segment of its URLs, and a refusal is sent only in the
/// form that Reef reads: `<code> <time>`.
#[test]
fn names_and_refusals_keep_their_forms() {
    for (name, valid) in [
        ("test", true),
        ("a-b_9", true),
        (&*"a".repeat(32), true),
        (&*"a".repeat(33), false),
        ("", false),
        ("Test", false),
        ("9test", false),
        ("a b", false),
        ("a/b", false),
    ] {
        assert_eq!(ModeName::is_name(name), valid, "{name}");
    }
    let long_code = format!("{} 2026-11-26T10:15:42Z", "a".repeat(64));
    let too_long_code = format!("{} 2026-11-26T10:15:42Z", "a".repeat(65));
    for (description, valid) in [
        ("sign_in_refused 2026-11-26T10:15:42Z", true),
        (&*long_code, true),
        (&*too_long_code, false),
        ("sign-in 2026-11-26T10:15:42Z", false),
        ("Refused 2026-11-26T10:15:42Z", false),
        ("1refused 2026-11-26T10:15:42Z", false),
        ("refused 2026-02-30T10:15:42Z", false),
        ("refused 2026-11-26T10:15:42", false),
        ("refused 2026-11-26 10:15:42Z", false),
        ("refused 2026-11-26T10:15:42+00:00", false),
        ("refused  2026-11-26T10:15:42Z", false),
        ("refused 2026-11-26T10:15:42Z extra", false),
        ("refused", false),
        ("", false),
    ] {
        let refusal = Refusal::from_description(description.to_owned());
        assert_eq!(refusal.checked().is_some(), valid, "{description}");
    }
    // The helper writes the form, to the second.
    let refusal = Refusal::new("sign_in_refused", UnixNanos::new(1_500_000_000));
    assert_eq!(
        refusal.checked(),
        Some("sign_in_refused 1970-01-01T00:00:01Z")
    );
}

#[test]
fn a_rejected_mode_setting_names_its_field_and_never_its_value() {
    for (settings, expected) in [
        (
            format!("sign_in_url = '{SECRET}'"),
            "sign_in_url: must be a URL",
        ),
        (
            "sign_in_url = 7".to_owned(),
            "sign_in_url: unsupported value",
        ),
        (
            "sign_in_url = 'http://idp.example.test/'".to_owned(),
            "sign_in_url: must be https",
        ),
        (String::new(), "missing field `sign_in_url`"),
    ] {
        let (built, environment) = super::tests::settings(&settings);
        let Err(error) = TestModeFactory.build(&built, &environment) else {
            panic!("{settings}: expected a refusal");
        };
        let error = error.to_string();
        assert!(error.starts_with(expected), "{settings}: {error}");
        assert!(!error.contains(SECRET), "{settings}: {error}");
    }
}

/// Settings, a request, a sign-in's value and a person can each hold a
/// secret or personal data, so their `Debug` prints none of it.
#[test]
fn debug_prints_no_value() {
    let request = ModeRequest {
        query: "code=SECRET",
        body: b"SECRET",
    };
    let (settings, _) = settings("client_secret = 'SECRET'");
    for (printed, expected) in [
        (format!("{request:?}"), "ModeRequest { .. }"),
        (format!("{settings:?}"), "ModeSettings { .. }"),
        (
            format!("{:?}", SignInValue::new("SECRET".to_owned())),
            "SignInValue(<redacted>)",
        ),
        (
            format!("{:?}", ProvenPerson::name_id("SECRET".to_owned())),
            "ProvenPerson { .. }",
        ),
    ] {
        assert_eq!(printed, expected);
    }
}

/// A mode outside `ee/` can serve the whole contract: the begin, the two
/// answers of a request, and a refusal.
#[tokio::test]
async fn the_test_mode_begins_names_and_checks() {
    let now = UnixNanos::new(0);
    let mode = test_mode("sign_in_url = 'https://idp.example.test/start'");
    let sign_in = SignInValue::new("value-1".to_owned());
    let begin = mode.begin(&sign_in, now).await.expect("begin");
    assert_eq!(
        begin.as_str(),
        "https://idp.example.test/start?sign_in=value-1&return_to=https%3A%2F%2Fcoral.example.test%2F"
    );

    let answer = ModeRequest {
        query: "sign_in=value-1&person=00u1",
        body: b"",
    };
    let Named::SignIn(named) = mode.names(&answer, now) else {
        panic!("the answer names its sign-in");
    };
    assert_eq!(named, sign_in);
    let person = mode.check(&answer, &sign_in, now).await.expect("a person");
    assert_eq!(person.identifier(), PersonIdentifier::NameId("00u1"));

    let unasked = ModeRequest {
        query: "person=00u1",
        body: b"",
    };
    assert!(matches!(mode.names(&unasked, now), Named::NoSignIn));
    let malformed = ModeRequest {
        query: "sign_in=a&sign_in=b",
        body: b"",
    };
    let Named::Refused(refusal) = mode.names(&malformed, now) else {
        panic!("two sign-ins are refused");
    };
    assert_eq!(
        refusal.checked(),
        Some("test_malformed 1970-01-01T00:00:00Z")
    );

    let other = SignInValue::new("value-2".to_owned());
    let refusal = mode
        .check(&answer, &other, now)
        .await
        .expect_err("mismatch");
    assert_eq!(
        refusal.checked(),
        Some("test_mismatch 1970-01-01T00:00:00Z")
    );
    let refusing =
        test_mode("sign_in_url = 'https://idp.example.test/start'\nrefuse = 'sign_in_refused'");
    let refusal = refusing
        .check(&answer, &sign_in, now)
        .await
        .expect_err("refused");
    assert_eq!(
        refusal.checked(),
        Some("sign_in_refused 1970-01-01T00:00:00Z")
    );
}
