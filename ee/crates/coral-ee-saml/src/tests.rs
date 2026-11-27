// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

use std::io::Read as _;
use std::sync::Arc;

use base64::Engine as _;
use coral_db::{Clock as _, UnixNanos};
use coral_edition::test_fixtures::SwitchedEdition;
use coral_edition::{Edition, EditionCheck};
use coral_sign_in::{
    ModeEnvironment, ModeSettings, PersonIdentifier, RefusalError, SameSite, SettingError,
    SignInMode, SignInModeFactory as _, SignInValue,
};
use coral_test_support::ManualClock;
use url::Url;

use super::SamlModeFactory;
use super::settings::Settings;

const CORAL_URL: &str = "https://coral.example.test/";
const SIGN_IN_URL: &str = "https://idp.example.test/app/coral/sso/saml";
/// 43 characters, as a sign-in's random value has.
const SIGN_IN: &str = "AbCdEfGhIjKlMnOpQrStUvWxYz0123456789-_abcde";

/// The start of the day `(year, month, day)` in UTC.
fn day_start(year: i32, month: u32, day: u32) -> UnixNanos {
    let at = chrono::NaiveDate::from_ymd_opt(year, month, day)
        .and_then(|day| day.and_hms_opt(0, 0, 0))
        .expect("a day")
        .and_utc();
    UnixNanos::new(at.timestamp_nanos_opt().expect("in range"))
}

/// The instant `2026-11-25T10:15:42.5Z`.
fn instant() -> UnixNanos {
    let at = chrono::NaiveDate::from_ymd_opt(2026, 11, 25)
        .and_then(|day| day.and_hms_milli_opt(10, 15, 42, 500))
        .expect("an instant")
        .and_utc();
    UnixNanos::new(at.timestamp_nanos_opt().expect("in range"))
}

/// A self-signed certificate, made for the test, whose last instant is the
/// start of the day `(year, month, day)` in UTC.
fn certificate_ending(year: i32, month: u8, day: u8) -> String {
    let key = rcgen::KeyPair::generate().expect("a key");
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("parameters");
    params.not_before = rcgen::date_time_ymd(2026, 1, 1);
    params.not_after = rcgen::date_time_ymd(year, month, day);
    params.self_signed(&key).expect("a certificate").pem()
}

/// Settings TOML with every required setting, the `certificates` given, and
/// `extra` after them.
fn settings_toml(certificates: &[String], extra: &str) -> String {
    let certificates = certificates
        .iter()
        .map(|pem| format!("'''\n{pem}'''"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "entity_id = 'http://idp.example.test/exk1'\n\
         sign_in_url = '{SIGN_IN_URL}'\n\
         certificates = [{certificates}]\n\
         {extra}\n"
    )
}

fn mode_settings(toml: &str) -> ModeSettings {
    ModeSettings::new(toml::from_str(toml).expect("TOML"))
}

fn environment() -> ModeEnvironment {
    ModeEnvironment::new(Url::parse(CORAL_URL).expect("Coral's URL"))
}

/// The mode that `toml` configures, under the Edition `edition`.
fn build(toml: &str, edition: EditionCheck) -> Result<Arc<dyn SignInMode>, SettingError> {
    let settings = mode_settings(toml);
    SamlModeFactory::new(edition).build(&settings, &environment())
}

/// Asserts that `refusal` is `access_denied`, described as `code` and the
/// time of [`instant`].
fn assert_refused(refusal: &coral_sign_in::Refusal, code: &str) {
    assert_eq!(refusal.error(), RefusalError::AccessDenied, "{code}");
    assert_eq!(
        refusal.description(),
        format!("{code} 2026-11-25T10:15:42Z")
    );
}

fn enterprise() -> EditionCheck {
    EditionCheck::new(Arc::new(Edition::Enterprise))
}

/// Each case in which Coral refuses to start names the setting and why,
/// never its value, and builds no mode.
#[test]
#[expect(clippy::too_many_lines, reason = "one row for each refusal at start")]
fn refuses_to_start() {
    let one = certificate_ending(2030, 1, 1);
    let good = settings_toml(std::slice::from_ref(&one), "");
    let key = rcgen::KeyPair::generate().expect("a key").serialize_pem();
    let not_der = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n".to_owned();
    let (_label, mut der) = x509_cert::der::pem::decode_vec(one.as_bytes()).expect("PEM");
    der.extend_from_slice(&[0, 0]);
    let trailing = x509_cert::der::pem::encode_string(
        "CERTIFICATE",
        x509_cert::der::pem::LineEnding::LF,
        &der,
    )
    .expect("PEM");
    let with = |extra: &str| settings_toml(std::slice::from_ref(&one), extra);
    let cases = [
        (
            good.replace("entity_id = 'http://idp.example.test/exk1'\n", ""),
            "missing field `entity_id`",
            false,
        ),
        (
            format!("{good}\nissuer = 'https://accounts.example.test'"),
            "issuer: not a setting of this sign-in mode",
            true,
        ),
        (
            with("client_secret = 'sk-live-0123456789abcdef'"),
            "client_secret: not a setting of this sign-in mode",
            true,
        ),
        (
            good.replace("'http://idp.example.test/exk1'", "' '"),
            "entity_id: must not be empty",
            false,
        ),
        (
            with("email_attribute = ''"),
            "email_attribute: must not be empty",
            false,
        ),
        (
            with("name_attribute = ''"),
            "name_attribute: must not be empty",
            false,
        ),
        (
            with("bootstrap_owner_name_id = ''"),
            "bootstrap_owner_name_id: must not be empty",
            false,
        ),
        (
            good.replace(SIGN_IN_URL, "http://idp.example.test/sso"),
            "sign_in_url: SAML sign-in needs https",
            false,
        ),
        (
            good.replace(SIGN_IN_URL, "http://localhost:8443/sso"),
            "sign_in_url: SAML sign-in needs https",
            false,
        ),
        (
            good.replace(SIGN_IN_URL, " "),
            "sign_in_url: must not be empty",
            false,
        ),
        (
            good.replace(SIGN_IN_URL, &format!("{SIGN_IN_URL}#start")),
            "sign_in_url: must have no fragment",
            false,
        ),
        (
            good.replace(SIGN_IN_URL, "https://okta:hunter2@idp.example.test/sso"),
            "sign_in_url: must hold no user name or password",
            false,
        ),
        (
            good.replace(SIGN_IN_URL, "idp.example.test/sso"),
            "sign_in_url: must be a URL",
            false,
        ),
        (
            settings_toml(&[], ""),
            "certificates: needs one or two certificates",
            false,
        ),
        (
            settings_toml(&[one.clone(), one.clone(), one.clone()], ""),
            "certificates: needs one or two certificates",
            false,
        ),
        (
            good.replace(&format!("[{}]", "'''\n".to_owned() + &one + "'''"), "'x'"),
            "certificates: unsupported value",
            false,
        ),
        (
            settings_toml(&["not a certificate\n".to_owned()], ""),
            "certificates[0]: must be one PEM certificate",
            false,
        ),
        (
            settings_toml(&[one.clone(), not_der], ""),
            "certificates[1]: must be one PEM certificate",
            false,
        ),
        (
            settings_toml(&[format!("{one}{one}")], ""),
            "certificates[0]: must be one PEM certificate",
            false,
        ),
        (
            settings_toml(&[key], ""),
            "certificates[0]: must be one PEM certificate",
            false,
        ),
        (
            settings_toml(&[format!("the Identity Provider's certificate\n{one}")], ""),
            "certificates[0]: must be one PEM certificate",
            false,
        ),
        (
            settings_toml(&[trailing], ""),
            "certificates[0]: must be one PEM certificate",
            false,
        ),
    ];
    for (toml, message, unknown) in cases {
        let Err(error) = build(&toml, enterprise()) else {
            panic!("{message}: expected a refusal");
        };
        assert_eq!(error.to_string(), message);
        assert_eq!(error.is_unknown(), unknown, "{message}");
    }
}

/// Settings with only the required keys build, with the two attributes'
/// defaults, and the mode names the first Owner only when they say so.
#[test]
fn builds_with_the_defaults() {
    let pems = [
        certificate_ending(2030, 1, 1),
        certificate_ending(2031, 1, 1),
    ];
    let settings = Settings::check(&mode_settings(&settings_toml(&pems, ""))).expect("settings");
    assert_eq!(settings.entity_id, "http://idp.example.test/exk1");
    assert_eq!(settings.email_attribute, "email");
    assert_eq!(settings.name_attribute, "name");
    assert_eq!(settings.certificates.len(), 2);

    let mode = build(&settings_toml(&pems, ""), enterprise()).expect("the mode");
    assert_eq!(mode.first_owner(), None);
    assert_eq!(mode.same_site(), SameSite::None);
    assert_eq!(mode.urls(), []);
    assert_refused(&mode.no_sign_in_cookie(instant()), "saml_no_sign_in");

    // Each value is kept without the whitespace around it.
    let owner = build(
        &settings_toml(&pems, "bootstrap_owner_name_id = ' 00u1abcd '"),
        enterprise(),
    )
    .expect("the mode");
    assert_eq!(
        owner.first_owner(),
        Some(PersonIdentifier::NameId("00u1abcd"))
    );
}

/// The begin sends the browser to `sign_in_url` with an unsigned
/// `AuthnRequest`, deflated and in base64, and with no `RelayState`. The
/// URL keeps its own query, and the request escapes what it repeats.
#[tokio::test]
async fn begin_sends_an_unsigned_request_by_redirect() {
    let clock = ManualClock::starting_at(instant());
    let pem = certificate_ending(2030, 1, 1);
    for (sign_in_url, own_query, destination) in [
        (SIGN_IN_URL, vec![], SIGN_IN_URL.to_owned()),
        (
            "https://idp.example.test/sso?tenant=a&x=1",
            vec![("tenant", "a"), ("x", "1")],
            "https://idp.example.test/sso?tenant=a&amp;x=1".to_owned(),
        ),
    ] {
        let toml = settings_toml(std::slice::from_ref(&pem), "").replace(SIGN_IN_URL, sign_in_url);
        let mode = build(&toml, enterprise()).expect("the mode");
        let url = mode
            .begin(&SignInValue::new(SIGN_IN.to_owned()), clock.now())
            .await
            .expect("a redirect");
        assert_eq!(
            url.as_str().split('?').next(),
            sign_in_url.split('?').next(),
            "{url}"
        );
        let pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        let (request_pairs, rest): (Vec<_>, Vec<_>) =
            pairs.into_iter().partition(|(key, _)| key == "SAMLRequest");
        assert_eq!(
            rest.iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect::<Vec<_>>(),
            own_query,
            "only the URL's own query beside the request: no RelayState, SigAlg or Signature"
        );
        let [(_, request)] = request_pairs.as_slice() else {
            panic!("one SAMLRequest: {url}");
        };
        let deflated = base64::engine::general_purpose::STANDARD
            .decode(request)
            .expect("base64");
        let mut xml = String::new();
        flate2::read::DeflateDecoder::new(deflated.as_slice())
            .read_to_string(&mut xml)
            .expect("raw DEFLATE");
        assert_eq!(
            xml,
            format!(
                "<samlp:AuthnRequest xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" \
                 xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\" \
                 ID=\"_{SIGN_IN}\" Version=\"2.0\" IssueInstant=\"2026-11-25T10:15:42Z\" \
                 Destination=\"{destination}\" \
                 ProtocolBinding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\" \
                 AssertionConsumerServiceURL=\"https://coral.example.test/auth/saml/acs\">\
                 <saml:Issuer>https://coral.example.test/auth/saml/metadata</saml:Issuer>\
                 </samlp:AuthnRequest>"
            )
        );
    }
}

/// While the Edition is Community, each sign-in ends before the browser
/// leaves Coral with `saml_needs_enterprise`. The mode asks at each use,
/// so a License makes it work, and its loss stops it again. In either
/// Edition it accepts no response: it reads none yet.
#[tokio::test]
async fn refuses_each_sign_in_while_community() {
    let clock = ManualClock::starting_at(instant());
    let source = Arc::new(SwitchedEdition::default());
    let mode = build(
        &settings_toml(&[certificate_ending(2030, 1, 1)], ""),
        EditionCheck::new(source.clone()),
    )
    .expect("the mode starts while the Edition is Community");
    let sign_in = SignInValue::new(SIGN_IN.to_owned());
    let answer = coral_sign_in::ModeRequest {
        query: "",
        body: b"",
    };
    for (edition, refused) in [
        (Edition::Community, true),
        (Edition::Enterprise, false),
        (Edition::Community, true),
    ] {
        source.set(edition);
        let begun = mode.begin(&sign_in, clock.now()).await;
        let Err(checked) = mode.check(&answer, &sign_in, &clock).await else {
            panic!("{edition:?}: the mode proves no person");
        };
        let coral_sign_in::Named::Refused(named) = mode.names(&answer, clock.now()) else {
            panic!("{edition:?}: the mode names no sign-in");
        };
        assert_refused(&named, "saml_malformed");
        match begun {
            Err(refusal) => {
                assert!(refused, "{edition:?}");
                assert_refused(&refusal, "saml_needs_enterprise");
                assert_refused(&checked, "saml_needs_enterprise");
            }
            Ok(url) => {
                assert!(!refused, "{edition:?}: {url}");
                assert_refused(&checked, "saml_malformed");
            }
        }
    }
}

/// The metadata document names Coral's entity ID and the URL that receives
/// the response, at the URL that is the entity ID, in either Edition.
#[test]
fn serves_the_metadata_document() {
    for edition in [Edition::Community, Edition::Enterprise] {
        let mode = build(
            &settings_toml(&[certificate_ending(2030, 1, 1)], ""),
            EditionCheck::new(Arc::new(edition)),
        )
        .expect("the mode");
        let documents = mode.documents();
        let [document] = documents.as_slice() else {
            panic!("{edition:?}: one document");
        };
        assert_eq!(document.path(super::NAME), "/auth/saml/metadata");
        assert_eq!(document.media_type(), "application/samlmetadata+xml");
        assert_eq!(
            document.body(),
            "<md:EntityDescriptor xmlns:md=\"urn:oasis:names:tc:SAML:2.0:metadata\" \
             entityID=\"https://coral.example.test/auth/saml/metadata\">\
             <md:SPSSODescriptor \
             protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\" \
             AuthnRequestsSigned=\"false\" WantAssertionsSigned=\"true\">\
             <md:AssertionConsumerService \
             Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\" \
             Location=\"https://coral.example.test/auth/saml/acs\" index=\"0\" isDefault=\"true\"/>\
             </md:SPSSODescriptor>\
             </md:EntityDescriptor>"
        );
    }
}

/// What a `fmt` layer writes: the host's own log, with the fields that
/// never leave it.
#[derive(Clone, Default)]
struct HostLog(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for HostLog {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .map_err(|_poisoned| std::io::Error::other("the host log is poisoned"))?
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// At startup the mode warns once for each certificate past its last
/// instant and for each that ends within 30 days, naming the entry on the
/// host only, and for no other. A warning never stops the start: `started`
/// returns nothing to stop it with.
#[test]
fn warns_at_start_by_last_day() {
    use tracing_subscriber::layer::SubscriberExt as _;

    const PAST: &str = "a SAML signing certificate is past its last day";
    const ENDING: &str = "a SAML signing certificate ends within 30 days";
    let pem = certificate_ending(2030, 6, 15);
    let later = certificate_ending(2031, 6, 15);
    let last = day_start(2030, 6, 15);
    let thirty_days = std::time::Duration::from_hours(24 * 30);
    let nanosecond = std::time::Duration::from_nanos(1);
    let before = |instant: UnixNanos, span| instant.checked_sub(span).expect("in range");
    let after = |instant: UnixNanos, span| instant.checked_add(span).expect("in range");
    for (now, certificates, warnings) in [
        (
            before(before(last, thirty_days), nanosecond),
            vec![pem.clone()],
            vec![],
        ),
        (
            before(last, thirty_days),
            vec![pem.clone()],
            vec![(ENDING, 0)],
        ),
        (last, vec![pem.clone()], vec![(ENDING, 0)]),
        (after(last, nanosecond), vec![pem.clone()], vec![(PAST, 0)]),
        (
            after(last, nanosecond),
            vec![pem.clone(), later.clone()],
            vec![(PAST, 0)],
        ),
        (
            before(day_start(2031, 6, 15), thirty_days),
            vec![pem.clone(), later.clone()],
            vec![(PAST, 0), (ENDING, 1)],
        ),
    ] {
        let clock = ManualClock::starting_at(now);
        let mode = build(&settings_toml(&certificates, ""), enterprise()).expect("the mode");
        let exported = coral_telemetry::testing::ExportCapture::default();
        let host = HostLog::default();
        {
            let writer = host.clone();
            let _subscriber = tracing::subscriber::set_default(
                tracing_subscriber::registry()
                    .with(exported.log_layer())
                    .with(
                        tracing_subscriber::fmt::layer()
                            .with_ansi(false)
                            .with_writer(move || writer.clone()),
                    ),
            );
            mode.started(clock.now());
        }
        let host = String::from_utf8(host.0.lock().expect("the host log").clone()).expect("text");
        let lines: Vec<&str> = host.lines().collect();
        assert_eq!(lines.len(), warnings.len(), "{now:?}: {host}");
        for (message, entry) in &warnings {
            let entry = format!("coral.local.certificate=certificates[{entry}]");
            assert_eq!(
                lines
                    .iter()
                    .filter(|line| line.contains(message) && line.contains(&entry))
                    .count(),
                1,
                "{now:?}: {message} {entry}: {host}"
            );
        }
        let records = exported.log_records();
        assert_eq!(records.len(), warnings.len(), "{now:?}: {records:?}");
        assert!(
            records
                .iter()
                .all(|record| !record.contains("certificates[")),
            "the entry never leaves the host: {records:?}"
        );
    }
}
