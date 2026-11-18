// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

use chrono::NaiveDate;
use coral_edition::{Edition, EditionSource, NoticeLevel};
use pasetors::keys::{AsymmetricKeyPair, Generate};
use pasetors::paserk::{FormatAsPaserk, Id};
use pasetors::version4::{PublicToken, V4};
use serde_json::{Value, json};

use super::{Build, Keys, LicensedEdition, Malformed, Refusal};

/// A throwaway signing key, as a test generates it.
struct Signer {
    pair: AsymmetricKeyPair<V4>,
}

impl Signer {
    fn new() -> Self {
        Self {
            pair: AsymmetricKeyPair::<V4>::generate().expect("key pair"),
        }
    }

    /// The public key as the crate keeps it: a PASERK `k4.public` string.
    fn public(&self) -> String {
        let mut paserk = String::new();
        self.pair.public.fmt(&mut paserk).expect("format");
        paserk
    }

    /// The key's PASERK id, `k4.pid.…`.
    fn kid(&self) -> String {
        let mut paserk = String::new();
        Id::from(&self.pair.public)
            .fmt(&mut paserk)
            .expect("format");
        paserk
    }

    /// A License with `payload`, as Coral signs one.
    fn sign(&self, payload: &Value) -> String {
        self.sign_as(payload, &self.kid(), b"coral-license")
    }

    /// A token with `payload`, a footer naming `kid`, under `assertion`.
    fn sign_as(&self, payload: &Value, kid: &str, assertion: &[u8]) -> String {
        PublicToken::sign(
            &self.pair.secret,
            payload.to_string().as_bytes(),
            Some(json!({ "kid": kid }).to_string().as_bytes()),
            Some(assertion),
        )
        .expect("sign")
    }
}

/// The License in a test License file: its one line that is not a `#` comment.
fn license_line(file: &str) -> &str {
    file.lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .unwrap_or_default()
}

/// A Commercial License payload that expires on `exp_date`.
fn payload(exp_date: &str) -> Value {
    payload_of("commercial", exp_date)
}

/// A License payload of `kind` that expires on `exp_date`.
fn payload_of(kind: &str, exp_date: &str) -> Value {
    json!({
        "v": 1,
        "kind": kind,
        "lid": "lic_01",
        "customer": "Acme Corp",
        "iat": "2026-01-02T03:04:05Z",
        "exp_date": exp_date,
    })
}

/// The customer each License a test signs names.
const CUSTOMER: &str = "Acme Corp";

fn date(text: &str) -> NaiveDate {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").expect(text)
}

/// A released build trusting `trusted` and refusing `revoked`, released on
/// `release_date`.
fn build(trusted: &[&Signer], revoked: &[&Signer], release_date: &str) -> LicensedEdition {
    let trusted: Vec<String> = trusted.iter().map(|signer| signer.public()).collect();
    let revoked: Vec<String> = revoked.iter().map(|signer| signer.public()).collect();
    LicensedEdition::new(
        Keys::new(
            &trusted.iter().map(String::as_str).collect::<Vec<_>>(),
            &revoked.iter().map(String::as_str).collect::<Vec<_>>(),
        )
        .expect("keys"),
        Build::Released(date(release_date)),
    )
}

/// What a startup logs: whether any line warns, and whether some line
/// says each of `says`.
fn assert_notices(
    notices: &[coral_edition::LicenseNotice],
    warns: bool,
    says: &[&str],
    case: &str,
) {
    assert_eq!(
        notices
            .iter()
            .any(|notice| notice.level == NoticeLevel::Warning),
        warns,
        "{case}: {notices:?}"
    );
    for text in says {
        assert!(
            notices.iter().any(|notice| notice.message.contains(text)),
            "{case}: no line says {text:?} in {notices:?}"
        );
    }
}

// Catches: the Edition decided by the clock instead of the Release Date, an
// off-by-one at the boundary, or a startup that does not say what to do.
#[test]
fn a_license_covers_builds_released_on_or_before_its_expiry() {
    let key = Signer::new();
    for (case, release_date, today, expected, warns, says) in [
        (
            "released before the expiry",
            "2027-06-29",
            "2026-11-18",
            Edition::Enterprise,
            false,
            &[
                "Acme Corp",
                "expires on 2027-06-30",
                "updates are covered until 2027-06-30",
            ][..],
        ),
        (
            "released on the expiry",
            "2027-06-30",
            "2026-11-18",
            Edition::Enterprise,
            false,
            &["Acme Corp"][..],
        ),
        (
            "a covered build run long after the expiry",
            "2027-06-30",
            "2031-01-01",
            Edition::Enterprise,
            true,
            &["Builds released after 2027-06-30"][..],
        ),
        (
            "60 days before the expiry",
            "2027-01-01",
            "2027-05-01",
            Edition::Enterprise,
            true,
            &["Builds released after 2027-06-30"][..],
        ),
        (
            "61 days before the expiry",
            "2027-01-01",
            "2027-04-30",
            Edition::Enterprise,
            false,
            &[][..],
        ),
    ] {
        let edition = build(&[&key], &[], release_date);
        let notices = edition
            .apply(Some(&key.sign(&payload("2027-06-30"))), date(today))
            .expect(case);
        assert_eq!(edition.edition(), expected, "{case}");
        assert_notices(&notices, warns, says, case);
    }
}

#[test]
fn a_build_without_a_license_runs_as_community_and_says_nothing() {
    let key = Signer::new();
    let edition = build(&[&key], &[], "2027-01-01");
    edition
        .apply(Some(&key.sign(&payload("2028-01-01"))), date("2026-11-18"))
        .expect("covered");
    let notices = edition.apply(None, date("2026-11-18")).expect("no License");
    assert_eq!(edition.edition(), Edition::Community);
    assert!(notices.is_empty(), "{notices:?}");
}

// Catches: a build a Commercial License does not cover that starts as
// Community, so a rolling deploy retires the covered build (ADR 0038).
#[test]
fn a_build_released_after_a_commercial_license_expires_refuses_to_start() {
    let key = Signer::new();
    let edition = build(&[&key], &[], "2027-07-01");
    let refusal = edition
        .apply(Some(&key.sign(&payload("2027-06-30"))), date("2026-11-18"))
        .expect_err("a build released the day after the expiry");
    assert_eq!(
        refusal,
        Refusal::Expired {
            customer: CUSTOMER.to_owned(),
            expiry: date("2027-06-30"),
            release_date: date("2027-07-01"),
        }
    );
    assert_eq!(edition.edition(), Edition::Community);
}

// Catches: a trial that starts after its expiry, on a later build or on the
// build it ran, or a trial that ends a day early.
#[test]
fn an_evaluation_license_starts_only_until_its_expiry_on_the_clock() {
    let key = Signer::new();
    let license = key.sign(&payload_of("evaluation", "2026-12-18"));
    for (case, release_date, today) in [
        ("during the trial", "2026-11-01", "2026-11-18"),
        ("on the last day", "2026-11-01", "2026-12-18"),
    ] {
        let edition = build(&[&key], &[], release_date);
        let notices = edition.apply(Some(&license), date(today)).expect(case);
        assert_eq!(edition.edition(), Edition::Enterprise, "{case}");
        assert_notices(
            &notices,
            true,
            &["Acme Corp", "until 2026-12-18", "stops after 2026-12-18"],
            case,
        );
    }
    for (case, release_date, today, expected) in [
        (
            "the day after, on the build it ran",
            "2026-11-01",
            "2026-12-19",
            Refusal::EvaluationEnded {
                customer: CUSTOMER.to_owned(),
                expiry: date("2026-12-18"),
            },
        ),
        (
            "a build released after the expiry, with the clock set back",
            "2027-01-01",
            "2026-11-18",
            Refusal::EvaluationTooOld {
                customer: CUSTOMER.to_owned(),
                expiry: date("2026-12-18"),
                release_date: date("2027-01-01"),
            },
        ),
    ] {
        let edition = build(&[&key], &[], release_date);
        let refusal = edition.apply(Some(&license), date(today)).expect_err(case);
        assert_eq!(refusal, expected, "{case}");
        assert_eq!(edition.edition(), Edition::Community, "{case}");
    }
}

// Catches: a trial that runs on past its expiry, one that switches
// Enterprise Features off before Coral stops, or a Commercial License that
// stops Coral on the clock.
#[test]
fn only_an_evaluation_license_stops_coral_once_it_expires() {
    let key = Signer::new();
    for (kind, stops) in [("evaluation", true), ("commercial", false)] {
        let edition = build(&[&key], &[], "2026-11-01");
        edition
            .apply(
                Some(&key.sign(&payload_of(kind, "2026-12-18"))),
                date("2026-11-18"),
            )
            .expect(kind);
        edition.recheck(date("2026-12-18")).expect("the last day");
        let expected = if stops {
            Err(Refusal::EvaluationStopped {
                customer: CUSTOMER.to_owned(),
                expiry: date("2026-12-18"),
            })
        } else {
            Ok(())
        };
        assert_eq!(edition.recheck(date("2026-12-19")), expected, "{kind}");
        assert_eq!(edition.edition(), Edition::Enterprise, "{kind}");
    }
    let edition = build(&[&key], &[], "2026-11-01");
    edition
        .recheck(date("2099-01-01"))
        .expect("no License never stops Coral");
}

// Catches: a License Coral must not start with, let through or refused for
// the wrong reason.
#[test]
fn coral_refuses_to_start_with_a_license_it_cannot_trust() {
    let key = Signer::new();
    let stranger = Signer::new();
    let leaked = Signer::new();
    let edition = build(&[&key], &[&leaked], "2027-01-01");
    let unknown_version = json!({
        "v": 2,
        "kind": "commercial",
        "lid": "lic_01",
        "customer": "Acme Corp",
        "iat": "2026-01-02T03:04:05Z",
        "exp_date": "2028-01-01",
    });
    for (case, license, expected) in [
        (
            "malformed",
            "not a license".to_owned(),
            Refusal::Malformed(Malformed::NotAToken),
        ),
        (
            "a JWT",
            "eyJhbGciOiJFZERTQSJ9.e30.c2ln".to_owned(),
            Refusal::Malformed(Malformed::NotAToken),
        ),
        (
            "no footer",
            PublicToken::sign(
                &key.pair.secret,
                payload("2028-01-01").to_string().as_bytes(),
                None,
                Some(b"coral-license"),
            )
            .expect("sign"),
            Refusal::Malformed(Malformed::NoSigningKey),
        ),
        (
            "unknown kid",
            stranger.sign(&payload("2028-01-01")),
            Refusal::Untrusted {
                kid: stranger.kid(),
            },
        ),
        (
            "bad signature",
            stranger.sign_as(&payload("2028-01-01"), &key.kid(), b"coral-license"),
            Refusal::BadSignature,
        ),
        (
            "wrong implicit assertion",
            key.sign_as(&payload("2028-01-01"), &key.kid(), b"another-token"),
            Refusal::BadSignature,
        ),
        (
            "unknown format version",
            key.sign(&unknown_version),
            Refusal::UnknownVersion(2),
        ),
        (
            "missing exp_date",
            key.sign(&json!({ "v": 1, "kind": "commercial", "lid": "lic_01", "customer": "Acme Corp", "iat": "2026-01-02T03:04:05Z" })),
            Refusal::Malformed(Malformed::NotALicense),
        ),
        (
            "missing lid",
            key.sign(&json!({ "v": 1, "kind": "commercial", "customer": "Acme Corp", "iat": "2026-01-02T03:04:05Z", "exp_date": "2028-01-01" })),
            Refusal::Malformed(Malformed::NotALicense),
        ),
        (
            "missing iat",
            key.sign(&json!({ "v": 1, "kind": "commercial", "lid": "lic_01", "customer": "Acme Corp", "exp_date": "2028-01-01" })),
            Refusal::Malformed(Malformed::NotALicense),
        ),
        (
            "missing kind",
            key.sign(&json!({ "v": 1, "lid": "lic_01", "customer": "Acme Corp", "iat": "2026-01-02T03:04:05Z", "exp_date": "2028-01-01" })),
            Refusal::Malformed(Malformed::NoKind),
        ),
        (
            "unknown kind",
            key.sign(&payload_of("perpetual", "2028-01-01")),
            Refusal::UnknownKind("perpetual".to_owned()),
        ),
        (
            "revoked key",
            leaked.sign(&payload("2028-01-01")),
            Refusal::Revoked { kid: leaked.kid() },
        ),
    ] {
        let refusal = edition
            .apply(Some(&license), date("2026-11-18"))
            .expect_err(case);
        assert_eq!(refusal, expected, "{case}");
        assert_eq!(edition.edition(), Edition::Community, "{case}");
    }
}

// Catches: a refusal that does not tell the operator what to do (ADR 0038).
#[test]
fn a_refusal_says_what_to_do() {
    for (refusal, says) in [
        (
            Refusal::Revoked {
                kid: "k4.pid.leaked".to_owned(),
            },
            "The configured License is signed with key k4.pid.leaked, which Coral has revoked. \
             Ask Coral for a new License.",
        ),
        (
            Refusal::Expired {
                customer: CUSTOMER.to_owned(),
                expiry: date("2027-06-30"),
                release_date: date("2027-07-01"),
            },
            "The License for Acme Corp expired on 2027-06-30, and this build was released on \
             2027-07-01, so Coral does not start. Run a build released on or before 2027-06-30, \
             or renew the License. To run as the Community Edition, remove the License from \
             config.toml.",
        ),
        (
            Refusal::EvaluationEnded {
                customer: CUSTOMER.to_owned(),
                expiry: date("2026-12-18"),
            },
            "The Evaluation License for Acme Corp ended on 2026-12-18, so Coral does not start. \
             Add a Commercial License to config.toml. To run as the Community Edition, remove \
             the License from config.toml.",
        ),
        (
            Refusal::EvaluationTooOld {
                customer: CUSTOMER.to_owned(),
                expiry: date("2026-12-18"),
                release_date: date("2027-01-01"),
            },
            "The Evaluation License for Acme Corp covers builds released on or before \
             2026-12-18, and this build was released on 2027-01-01, so Coral does not start. \
             Run a build released on or before 2026-12-18, or add a Commercial License to \
             config.toml. To run as the Community Edition, remove the License from config.toml.",
        ),
        (
            Refusal::EvaluationStopped {
                customer: CUSTOMER.to_owned(),
                expiry: date("2026-12-18"),
            },
            "The Evaluation License for Acme Corp ended on 2026-12-18, so Coral stops. Add a \
             Commercial License to config.toml. To run as the Community Edition, remove the \
             License from config.toml.",
        ),
    ] {
        assert_eq!(refusal.to_string(), says);
    }
}

// Catches: a published binary that accepts a test License, or a development
// build that trusts a production signing key.
#[test]
fn a_build_trusts_the_test_key_only_when_it_has_no_release_date() {
    let covering = license_line(include_str!(
        "../../../../crates/coral-test-support/test-licenses/covering.license"
    ));
    let lapsed = license_line(include_str!(
        "../../../../crates/coral-test-support/test-licenses/lapsed.license"
    ));
    let evaluation = license_line(include_str!(
        "../../../../crates/coral-test-support/test-licenses/evaluation.license"
    ));
    let development = Build::Development(date("2026-11-18"));
    let released = Build::Released(date("2026-11-18"));

    let edition = LicensedEdition::new(Keys::for_build(development), development);
    for (case, license, warns, says) in [
        ("covering", covering, false, &[][..]),
        (
            "evaluation",
            evaluation,
            true,
            &["Evaluation License", "9999-12-31"][..],
        ),
    ] {
        let notices = edition
            .apply(Some(license), date("2026-11-18"))
            .expect(case);
        assert_eq!(edition.edition(), Edition::Enterprise, "{case}");
        assert_notices(&notices, warns, says, case);
    }
    assert_eq!(
        edition.apply(Some(lapsed), date("2026-11-18")),
        Err(Refusal::Expired {
            customer: "Coral development (test key)".to_owned(),
            expiry: date("2000-01-01"),
            release_date: date("2026-11-18"),
        })
    );

    let test_kid = "k4.pid.6QBZRKbcbaaDgOpU7uN8FmoacXm3GV74aLGp3KbQ5E27";
    let edition = LicensedEdition::new(Keys::for_build(released), released);
    for license in [covering, lapsed, evaluation] {
        assert_eq!(
            edition.apply(Some(license), date("2026-11-18")),
            Err(Refusal::Untrusted {
                kid: test_kid.to_owned(),
            }),
            "a released build refuses the test signing key"
        );
    }

    // A development build trusts the test signing key alone, so it refuses every
    // production-signed License.
    assert_eq!(Keys::for_build(development).trusted_ids(), [test_kid]);
    assert!(
        !Keys::for_build(released)
            .trusted_ids()
            .iter()
            .any(|id| id == test_kid)
    );
}
