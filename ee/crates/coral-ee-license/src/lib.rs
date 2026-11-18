// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

//! Sets the Edition from a License (ADR 0038).
//!
//! A License is one line: a PASETO `v4.public` token signed under the
//! implicit assertion `coral-license`, whose footer names the signing key by
//! its PASERK id. It covers this build when the build's Release Date is on or
//! before the License's `exp_date`; a later build refuses to start with it.
//! A Commercial License never consults the clock: the clock only dates a
//! warning that it expires soon. An Evaluation License also needs the clock
//! on or before `exp_date`: Coral refuses to start after that, and a running
//! Coral stops when the periodic recheck finds it passed.

use std::sync::{PoisonError, RwLock};

use chrono::NaiveDate;
use coral_edition::{Edition, EditionSource, LicenseNotice, LicenseRefusal};
use pasetors::keys::AsymmetricPublicKey;
use pasetors::paserk::{FormatAsPaserk, Id};
use pasetors::token::UntrustedToken;
use pasetors::version4::{PublicToken, V4};
use serde::Deserialize;

/// The implicit assertion every License is signed under, so that no other
/// token signed with the same key passes as one.
const IMPLICIT_ASSERTION: &[u8] = b"coral-license";

/// How long before a License's expiry startup starts to warn about it.
const EXPIRY_WARNING: chrono::Days = chrono::Days::new(60);

/// The License format this build reads.
const FORMAT_VERSION: u64 = 1;

/// The production public keys, as PASERK `k4.public` strings with a comment
/// naming each. A build with a Release Date trusts these alone.
///
/// There are none until Coral issues its first License. Until then, a
/// published build runs as Community without a License, and refuses to start
/// with one, since it trusts no key.
///
/// To revoke a leaked key, move its literal from here to [`REVOKED_KEYS`].
/// Builds released after that refuse its Licenses by name; builds released
/// before still trust it (ADR 0038).
const PRODUCTION_KEYS: &[&str] = &[];

/// Production public keys that leaked, as PASERK `k4.public` strings moved
/// here from [`PRODUCTION_KEYS`]. Builds released before the leak still trust
/// them.
const REVOKED_KEYS: &[&str] = &[];

/// The test public key, which every build without a Release Date trusts
/// alone. Its private half is in the Coral team's password vault, never in
/// this repository or CI.
const TEST_KEY: &str = "k4.public.PrMXB8YYnuvNy5ZIx80g62X6m-3wodHtb7Lp65X-Wyk";

/// This build, as its build script dated it.
#[must_use]
pub fn this_build() -> Build {
    let date = NaiveDate::parse_from_str(env!("CORAL_LICENSE_RELEASE_DATE"), "%Y-%m-%d")
        .unwrap_or_default();
    match env!("CORAL_LICENSE_BUILD_KIND") {
        "released" => Build::Released(date),
        _ => Build::Development(date),
    }
}

/// The Edition a License sets. Startup decides it once, and an Evaluation
/// License ends it at its expiry.
pub struct LicensedEdition {
    keys: Keys,
    build: Build,
    grant: RwLock<Grant>,
}

/// What the applied License grants.
enum Grant {
    /// No License, or one that does not set Enterprise on this build.
    Community,
    /// A Commercial License that covers this build.
    Commercial,
    /// An Evaluation License, which stops Coral once the clock passes
    /// `expiry`. Until Coral stops, the Edition stays Enterprise, so no
    /// Enterprise Feature breaks while Coral still serves.
    Evaluation { customer: String, expiry: NaiveDate },
}

impl LicensedEdition {
    /// The source for this build: it trusts the keys [`Keys::for_build`]
    /// gives it.
    #[must_use]
    pub fn for_this_build() -> Self {
        let build = this_build();
        Self::new(Keys::for_build(build), build)
    }

    /// A source for `build` that trusts `keys`. Until startup applies a
    /// License, it answers Community.
    #[must_use]
    pub fn new(keys: Keys, build: Build) -> Self {
        Self {
            keys,
            build,
            grant: RwLock::new(Grant::Community),
        }
    }

    /// Decides what `license` grants this build.
    fn decide(
        &self,
        license: &str,
        today: NaiveDate,
    ) -> Result<(Grant, Vec<LicenseNotice>), LicenseRefusal> {
        let license = self.keys.verify(license)?;
        match license.kind {
            Kind::Commercial => self.decide_commercial(&license, today),
            Kind::Evaluation => self.decide_evaluation(license, today),
        }
    }

    /// A Commercial License covers every build released on or before its
    /// expiry, whatever the clock says. A later build refuses to start with
    /// it, so that a rolling deploy keeps the covered build running.
    fn decide_commercial(
        &self,
        license: &License,
        today: NaiveDate,
    ) -> Result<(Grant, Vec<LicenseNotice>), LicenseRefusal> {
        let release_date = self.build.release_date();
        let expiry = license.exp_date;
        let customer = &license.customer;
        if release_date > expiry {
            return Err(LicenseRefusal::new(format!(
                "The License for {customer} expired on {expiry}, and this build was released on \
                 {release_date}, so Coral does not start. Run a build released on or before \
                 {expiry}, or renew the License. To run as the Community Edition, remove the \
                 License from config.toml."
            )));
        }
        let mut notices = vec![LicenseNotice::info(format!(
            "The License for {customer} sets the Enterprise Edition. It expires on {expiry}: \
             updates are covered until {expiry}."
        ))];
        if today >= expiry - EXPIRY_WARNING {
            notices.push(LicenseNotice::warning(format!(
                "Builds released after {expiry} will not start with this License. Renew the \
                 License before you upgrade to one."
            )));
        }
        Ok((Grant::Commercial, notices))
    }

    /// An Evaluation License sets Enterprise only while the clock is on or
    /// before its expiry, and only on builds released by then. After that,
    /// Coral does not start with it.
    fn decide_evaluation(
        &self,
        license: License,
        today: NaiveDate,
    ) -> Result<(Grant, Vec<LicenseNotice>), LicenseRefusal> {
        let expiry = license.exp_date;
        let customer = license.customer;
        if today > expiry || self.build.release_date() > expiry {
            return Err(evaluation_ended(&customer, expiry, "does not start"));
        }
        let notices = vec![
            LicenseNotice::info(format!(
                "The Evaluation License for {customer} sets the Enterprise Edition until \
                 {expiry}."
            )),
            LicenseNotice::warning(format!(
                "Coral stops after {expiry}, and does not start again with this License. Ask \
                 Coral for a Commercial License before then."
            )),
        ];
        Ok((Grant::Evaluation { customer, expiry }, notices))
    }
}

/// Why Coral stops, or does not start, with an Evaluation License for
/// `customer` that ended on `expiry`.
fn evaluation_ended(customer: &str, expiry: NaiveDate, outcome: &str) -> LicenseRefusal {
    LicenseRefusal::new(format!(
        "The Evaluation License for {customer} ended on {expiry}, so Coral {outcome}. Add a \
         Commercial License to config.toml. To run as the Community Edition, remove the License \
         from config.toml."
    ))
}

impl EditionSource for LicensedEdition {
    fn edition(&self) -> Edition {
        match &*self.grant.read().unwrap_or_else(PoisonError::into_inner) {
            Grant::Community => Edition::Community,
            Grant::Commercial | Grant::Evaluation { .. } => Edition::Enterprise,
        }
    }

    fn apply_license(
        &self,
        license: Option<&str>,
        today: NaiveDate,
    ) -> Result<Vec<LicenseNotice>, LicenseRefusal> {
        let (grant, notices) = match license {
            None => (Grant::Community, Vec::new()),
            Some(license) => self.decide(license, today)?,
        };
        *self.grant.write().unwrap_or_else(PoisonError::into_inner) = grant;
        Ok(notices)
    }

    fn recheck_license(&self, today: NaiveDate) -> Result<(), LicenseRefusal> {
        match &*self.grant.read().unwrap_or_else(PoisonError::into_inner) {
            Grant::Evaluation { customer, expiry } if today > *expiry => {
                Err(evaluation_ended(customer, *expiry, "stops"))
            }
            Grant::Community | Grant::Commercial | Grant::Evaluation { .. } => Ok(()),
        }
    }
}

/// What kind of build this is, and when it counts as released.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Build {
    /// A build the release workflow published, with its Release Date.
    Released(NaiveDate),
    /// A source or development build, dated by its own commit.
    Development(NaiveDate),
}

impl Build {
    fn release_date(self) -> NaiveDate {
        match self {
            Self::Released(date) | Self::Development(date) => date,
        }
    }
}

/// The public keys a build trusts, and the keys it refuses by name because
/// they were revoked.
pub struct Keys {
    /// Each trusted key, beside its PASERK id.
    trusted: Vec<(String, AsymmetricPublicKey<V4>)>,
    /// The PASERK id of each revoked key, which a License's footer names.
    revoked: Vec<String>,
}

impl Keys {
    /// Trusts each PASERK `k4.public` key in `trusted`, and refuses each one
    /// in `revoked`.
    ///
    /// # Errors
    ///
    /// Returns what is wrong with a key that does not parse.
    pub fn new(trusted: &[&str], revoked: &[&str]) -> Result<Self, String> {
        let parse = |paserk: &&str| {
            let key = AsymmetricPublicKey::<V4>::try_from(paserk.trim())
                .map_err(|error| format!("public key `{paserk}`: {error:?}"))?;
            Ok((paserk_id(&key), key))
        };
        Ok(Self {
            trusted: trusted.iter().map(parse).collect::<Result<_, String>>()?,
            revoked: revoked
                .iter()
                .map(|paserk| parse(paserk).map(|(id, _)| id))
                .collect::<Result<_, String>>()?,
        })
    }

    /// The keys `build` trusts: the production keys for a build with a
    /// Release Date, the test key for any other.
    ///
    /// # Panics
    ///
    /// Panics if a key checked into this crate does not parse, which a test
    /// rules out.
    #[must_use]
    pub fn for_build(build: Build) -> Self {
        let keys = match build {
            Build::Released(_) => Self::new(PRODUCTION_KEYS, REVOKED_KEYS),
            Build::Development(_) => Self::new(&[TEST_KEY], &[]),
        };
        keys.expect("the keys checked into coral-ee-license parse")
    }

    /// The PASERK id of each key trusted.
    #[must_use]
    pub fn trusted_ids(&self) -> Vec<String> {
        self.trusted.iter().map(|(id, _)| id.clone()).collect()
    }

    /// Checks `license`'s signature and reads its payload.
    fn verify(&self, license: &str) -> Result<License, LicenseRefusal> {
        let token = UntrustedToken::<pasetors::token::Public, V4>::try_from(license.trim())
            .map_err(|_not_a_token| not_a_license("it is not a PASETO v4.public token"))?;
        let footer: Footer = serde_json::from_slice(token.untrusted_footer())
            .map_err(|_no_kid| not_a_license("its footer does not name a signing key"))?;
        if self.revoked.contains(&footer.kid) {
            return Err(LicenseRefusal::new(format!(
                "The configured License is signed with key {}, which Coral has revoked. \
                 Ask Coral for a new License.",
                footer.kid
            )));
        }
        let Some((_, key)) = self.trusted.iter().find(|(id, _)| *id == footer.kid) else {
            return Err(LicenseRefusal::new(format!(
                "The configured License is signed with key {}, which this build does not trust.",
                footer.kid
            )));
        };
        let trusted = PublicToken::verify(key, &token, None, Some(IMPLICIT_ASSERTION)).map_err(
            |_forged| LicenseRefusal::new("The configured License's signature does not check out."),
        )?;
        let payload: serde_json::Value = serde_json::from_str(trusted.payload())
            .map_err(|_not_json| not_a_license("its payload is not JSON"))?;
        match payload.get("v").and_then(serde_json::Value::as_u64) {
            Some(FORMAT_VERSION) => {}
            Some(version) => {
                return Err(LicenseRefusal::new(format!(
                    "The configured License has format version {version}, which this build \
                     does not read. Run a newer build, or ask Coral for a License this build reads."
                )));
            }
            None => return Err(not_a_license("its payload has no format version")),
        }
        match payload.get("kind").and_then(serde_json::Value::as_str) {
            Some("commercial" | "evaluation") => {}
            Some(kind) => {
                return Err(LicenseRefusal::new(format!(
                    "The configured License is of kind `{kind}`, which this build does not \
                     read. Run a newer build, or ask Coral for a License this build reads."
                )));
            }
            None => return Err(not_a_license("its payload has no kind")),
        }
        serde_json::from_value(payload)
            .map_err(|_not_a_license| not_a_license("its payload is not a License"))
    }
}

fn not_a_license(problem: &str) -> LicenseRefusal {
    LicenseRefusal::new(format!(
        "The configured License is not a Coral License: {problem}."
    ))
}

/// A key's PASERK id, `k4.pid.…`.
fn paserk_id(key: &AsymmetricPublicKey<V4>) -> String {
    let mut id = String::new();
    Id::from(key)
        .fmt(&mut id)
        .expect("writing to a String does not fail");
    id
}

#[derive(Deserialize)]
struct Footer {
    kid: String,
}

/// A License's payload.
#[derive(Deserialize)]
struct License {
    kind: Kind,
    customer: String,
    exp_date: NaiveDate,
}

/// Whether a License was paid for or is a trial (ADR 0038).
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Kind {
    /// Covers builds released on or before its expiry, after it passes.
    Commercial,
    /// Stops Coral when the clock passes its expiry.
    Evaluation,
}

#[cfg(test)]
mod tests;
