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

/// A mode's name is a segment of its URLs.
#[test]
fn names_keep_their_form() {
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
}

const LONGEST_CODE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const TOO_LONG_CODE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

/// Each refusal sends its OAuth error and a text that is either fixed or in
/// the form `<code> <time>`, and a code only in the form of one.
#[test]
fn a_refusal_names_its_answer() {
    let now = UnixNanos::new(1_500_000_000);
    let form = "sign_in_refused 1970-01-01T00:00:01Z";
    let longest = format!("{LONGEST_CODE} 1970-01-01T00:00:01Z");
    for (refusal, error, description, checked, code) in [
        (
            Refusal::new("sign_in_refused", now),
            RefusalError::AccessDenied,
            form,
            Some(form),
            Some("sign_in_refused"),
        ),
        (
            Refusal::new(LONGEST_CODE, now),
            RefusalError::AccessDenied,
            &*longest,
            Some(&*longest),
            Some(LONGEST_CODE),
        ),
        (
            Refusal::denied("provider_denied"),
            RefusalError::AccessDenied,
            DENIED,
            None,
            Some("provider_denied"),
        ),
        (
            Refusal::failed("token_exchange"),
            RefusalError::ServerError,
            "authorization failed",
            None,
            Some("token_exchange"),
        ),
        (
            Refusal::invalid("callback_malformed", "callback request is malformed"),
            RefusalError::InvalidRequest,
            "callback request is malformed",
            None,
            Some("callback_malformed"),
        ),
    ] {
        assert_eq!(
            (
                refusal.error(),
                refusal.description(),
                refusal.checked(),
                refusal.code()
            ),
            (error, description, checked, code),
            "{refusal:?}"
        );
    }
    // A code of another form sends none of itself, to the client or the
    // log.
    for code in [
        "",
        "Refused",
        "1refused",
        "sign-in",
        "refused here",
        TOO_LONG_CODE,
    ] {
        let refusal = Refusal::new(code, now);
        assert_eq!(
            (refusal.description(), refusal.checked(), refusal.code()),
            (DENIED, None, None),
            "{code}"
        );
    }
    assert_eq!(
        [
            RefusalError::AccessDenied,
            RefusalError::InvalidRequest,
            RefusalError::ServerError
        ]
        .map(RefusalError::as_str),
        ["access_denied", "invalid_request", "server_error"]
    );
}

/// A proven person keeps every part of the profile a mode gives, and a
/// provider that says nothing of the email is not read as one that refused it.
#[test]
fn a_proven_person_keeps_its_profile() {
    let bare = ProvenPerson::subject("https://idp.example.test".to_owned(), "00u1".to_owned());
    assert_eq!(
        (
            bare.display_name(),
            bare.name(),
            bare.email(),
            bare.email_verified(),
            bare.hosted_domain(),
            bare.observed_at()
        ),
        (None, None, None, None, None, None)
    );
    for verified in [None, Some(false), Some(true)] {
        let person = bare
            .clone()
            .with_display_name(Some("Ada L".to_owned()))
            .with_profile(
                Some("Ada".to_owned()),
                Some("ada@example.test".to_owned()),
                verified,
                Some("example.test".to_owned()),
            )
            .with_observed_at(UnixNanos::new(7));
        assert_eq!(
            (
                person.identifier(),
                person.display_name(),
                person.name(),
                person.email(),
                person.email_verified(),
                person.hosted_domain(),
                person.observed_at()
            ),
            (
                PersonIdentifier::Subject {
                    issuer: "https://idp.example.test",
                    subject: "00u1"
                },
                Some("Ada L"),
                Some("Ada"),
                Some("ada@example.test"),
                verified,
                Some("example.test"),
                Some(UnixNanos::new(7))
            )
        );
    }
}

#[test]
fn a_rejected_mode_setting_names_its_field_and_never_its_value() {
    for (settings, field, reason) in [
        (
            format!("sign_in_url = '{SECRET}'"),
            "sign_in_url",
            "must be a URL",
        ),
        (
            "sign_in_url = 7".to_owned(),
            "sign_in_url",
            "unsupported value",
        ),
        (
            "sign_in_url = 'http://idp.example.test/'".to_owned(),
            "sign_in_url",
            "must be https",
        ),
        (String::new(), "", "missing field `sign_in_url`"),
    ] {
        let (built, environment) = super::tests::settings(&settings);
        let Err(error) = TestModeFactory.build(&built, &environment) else {
            panic!("{settings}: expected a refusal");
        };
        assert_eq!(
            (error.field(), error.reason()),
            (field, reason),
            "{settings}"
        );
        assert!(!error.is_unknown(), "{settings}");
    }

    // A key the mode does not read is marked, so core can name the mode.
    let (built, environment) = settings(&format!(
        "sign_in_url = 'https://idp.example.test/'\nissuer = '{SECRET}'"
    ));
    let Err(error) = TestModeFactory.build(&built, &environment) else {
        panic!("expected a refusal");
    };
    assert!(error.is_unknown());
    assert_eq!(
        (error.field(), error.reason()),
        ("issuer", "not a setting of this sign-in mode")
    );

    // An empty key names no field, so it is not marked.
    let (built, environment) = settings("sign_in_url = 'https://idp.example.test/'\n'' = 1");
    let Err(error) = TestModeFactory.build(&built, &environment) else {
        panic!("expected a refusal");
    };
    assert!(!error.is_unknown(), "{error}");
    assert_eq!(error.field(), "", "{error}");
    assert!(error.reason().starts_with("unknown field ``"), "{error}");
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

/// A mode's URLs and documents stay below `/auth/{name}/`, and each segment
/// answers each method once; a document is a GET.
#[test]
fn a_modes_urls_stay_below_its_name() {
    let name = ModeName::new("test").expect("name");
    assert_eq!(ModeUrl::get("answer").path(name), "/auth/test/answer");
    let document = |segment| ModeDocument::new(segment, "text/plain", String::new());
    assert_eq!(document("about").path(name), "/auth/test/about");
    for (urls, documents, checked) in [
        (vec![], vec![], Ok(())),
        (
            vec![ModeUrl::get("answer"), ModeUrl::post("answer", 1)],
            vec![],
            Ok(()),
        ),
        (
            vec![ModeUrl::post("a-b_9", 0), ModeUrl::get("metadata")],
            vec![],
            Ok(()),
        ),
        (
            vec![ModeUrl::post("answer", 1)],
            vec![document("answer"), document("about")],
            Ok(()),
        ),
        (vec![ModeUrl::get("")], vec![], Err(InvalidUrl::Segment)),
        (vec![ModeUrl::get("a/b")], vec![], Err(InvalidUrl::Segment)),
        (vec![ModeUrl::get("..")], vec![], Err(InvalidUrl::Segment)),
        (
            vec![ModeUrl::post("Answer", 1)],
            vec![],
            Err(InvalidUrl::Segment),
        ),
        (vec![], vec![document("a/b")], Err(InvalidUrl::Segment)),
        (
            vec![ModeUrl::get("answer"), ModeUrl::get("answer")],
            vec![],
            Err(InvalidUrl::Repeated),
        ),
        (
            vec![ModeUrl::post("answer", 1), ModeUrl::post("answer", 2)],
            vec![],
            Err(InvalidUrl::Repeated),
        ),
        (
            vec![ModeUrl::get("answer")],
            vec![document("answer")],
            Err(InvalidUrl::Repeated),
        ),
        (
            vec![],
            vec![document("about"), document("about")],
            Err(InvalidUrl::Repeated),
        ),
        (
            vec![],
            vec![ModeDocument::new("about", "", String::new())],
            Err(InvalidUrl::MediaType),
        ),
        (
            vec![],
            vec![ModeDocument::new("about", "text/plain\n", String::new())],
            Err(InvalidUrl::MediaType),
        ),
    ] {
        assert_eq!(
            ModeUrl::check_all(&urls, &documents),
            checked,
            "{urls:?} {documents:?}"
        );
    }
}

/// The test mode serves what core's tests need of it: a GET and a limited
/// POST, a cookie of either `SameSite`, and an answer in a form body.
#[tokio::test]
async fn the_test_mode_declares_its_urls_and_cookie() {
    let clock = At(UnixNanos::new(0));
    let lax = super::testing::mode("sign_in_url = 'https://idp.example.test/start'");
    assert_eq!(
        lax.urls(),
        [
            ModeUrl::get("answer"),
            ModeUrl::post("answer", super::testing::BODY_LIMIT)
        ]
    );
    assert_eq!(lax.same_site(), SameSite::Lax);
    assert_eq!(
        lax.documents(),
        [ModeDocument::new(
            "about",
            super::testing::ABOUT_MEDIA_TYPE,
            "the test sign-in mode of https://coral.example.test/".to_owned()
        )]
    );
    let none =
        super::testing::mode("sign_in_url = 'https://idp.example.test/start'\nsame_site = 'none'");
    assert_eq!(none.same_site(), SameSite::None);
    assert_eq!(
        [SameSite::Lax, SameSite::None].map(SameSite::as_str),
        ["Lax", "None"]
    );

    let posted = ModeRequest {
        query: "",
        body: b"sign_in=value-1&person=00u2",
    };
    let Named::SignIn(sign_in) = none.names(&posted, clock.0) else {
        panic!("a form body names its sign-in");
    };
    let person = none
        .check(&posted, &sign_in, &clock)
        .await
        .expect("a person");
    assert_eq!(person.identifier(), PersonIdentifier::NameId("00u2"));
}

/// A clock that always reads one instant.
#[derive(Debug)]
struct At(UnixNanos);

impl Clock for At {
    fn now(&self) -> UnixNanos {
        self.0
    }
}

/// A mode outside `ee/` can serve the whole contract: the begin, the two
/// answers of a request, a refusal and the first Owner. Its check dates a
/// refusal by the clock it is given.
#[tokio::test]
async fn the_test_mode_begins_names_and_checks() {
    let now = UnixNanos::new(0);
    let clock = At(UnixNanos::new(1_000_000_000));
    let mode = super::testing::mode("sign_in_url = 'https://idp.example.test/start'");
    assert_eq!(mode.first_owner(), None);
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
    let person = mode
        .check(&answer, &sign_in, &clock)
        .await
        .expect("a person");
    assert_eq!(person.identifier(), PersonIdentifier::NameId("00u1"));
    assert_eq!(person.observed_at(), Some(clock.0));

    let unasked = ModeRequest {
        query: "person=00u1",
        body: b"",
    };
    let Named::NoSignIn(refusal) = mode.names(&unasked, now) else {
        panic!("an answer with no sign-in names none");
    };
    assert_eq!(
        refusal.checked(),
        Some("test_not_requested 1970-01-01T00:00:00Z")
    );
    assert_eq!(
        mode.no_sign_in_cookie(now).checked(),
        Some("test_no_sign_in 1970-01-01T00:00:00Z")
    );
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
        .check(&answer, &other, &clock)
        .await
        .expect_err("mismatch");
    assert_eq!(
        refusal.checked(),
        Some("test_mismatch 1970-01-01T00:00:01Z")
    );
    let refusing = super::testing::mode(
        "sign_in_url = 'https://idp.example.test/start'\nrefuse = true\nfirst_owner = '00u9'",
    );
    let refusal = refusing
        .check(&answer, &sign_in, &clock)
        .await
        .expect_err("refused");
    assert_eq!(refusal.checked(), Some("test_refused 1970-01-01T00:00:01Z"));
    assert_eq!(
        refusing.first_owner(),
        Some(PersonIdentifier::NameId("00u9"))
    );
}
