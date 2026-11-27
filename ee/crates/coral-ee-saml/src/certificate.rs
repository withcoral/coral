// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

//! A certificate that the operator pasted, which signs the Identity
//! Provider's responses.

use std::time::Duration;

use coral_db::UnixNanos;
use x509_cert::der::Decode as _;

/// How a PEM certificate begins.
const BEGIN: &str = "-----BEGIN CERTIFICATE-----";

/// One X.509 certificate, parsed.
pub(crate) struct Certificate {
    /// The last instant the certificate is valid at, its `notAfter`.
    last_instant: UnixNanos,
}

impl Certificate {
    /// The certificate in `pem`, which must hold exactly one PEM
    /// `CERTIFICATE` block, with only whitespace around it, and in it one
    /// certificate and nothing after it.
    pub(crate) fn parse(pem: &str) -> Option<Self> {
        let pem = pem.trim();
        // The decoder skips text before the block, and reads a second
        // block as part of the first.
        if !pem.starts_with(BEGIN) || pem.matches("-----BEGIN ").count() != 1 {
            return None;
        }
        let (label, der) = x509_cert::der::pem::decode_vec(pem.as_bytes()).ok()?;
        if label != "CERTIFICATE" {
            return None;
        }
        // Unlike `from_pem`, `from_der` refuses bytes after the certificate.
        let certificate = x509_cert::Certificate::from_der(&der).ok()?;
        let since_epoch = certificate
            .tbs_certificate()
            .validity()
            .not_after
            .to_unix_duration();
        // A `notAfter` past 2262, such as RFC 5280's 9999-12-31 for no end,
        // is the last instant `UnixNanos` holds.
        Some(Self {
            last_instant: UnixNanos::new(0).saturating_add(since_epoch),
        })
    }
}

/// How soon before its last instant a certificate is said to end.
const ENDS_SOON: Duration = Duration::from_hours(24 * 30);

/// Logs one warning at startup, at `now`, for each of `certificates` that is
/// past its last instant or ends within 30 days. Neither stops the start: a
/// response signed with a certificate past its last instant is refused when
/// it arrives. Each message names the entry itself, since no server log
/// layer shows a `coral.local.*` field; the settings hold one or two. The
/// four messages are literal text, by the rule the telemetry source scan
/// checks outside `ee/`: an exported event is named by its message.
pub(crate) fn warn_at_start(certificates: &[Certificate], now: UnixNanos) {
    for (index, certificate) in certificates.iter().enumerate() {
        let ended = now > certificate.last_instant;
        let ends_soon = certificate.last_instant.saturating_duration_since(now) <= ENDS_SOON;
        match (index == 0, ended, ends_soon) {
            (true, true, _) => tracing::warn!(
                "the first SAML signing certificate in auth.provider.certificates is past its last day, so each response it signs is refused"
            ),
            (true, false, true) => tracing::warn!(
                "the first SAML signing certificate in auth.provider.certificates ends within 30 days"
            ),
            (false, true, _) => tracing::warn!(
                "the second SAML signing certificate in auth.provider.certificates is past its last day, so each response it signs is refused"
            ),
            (false, false, true) => tracing::warn!(
                "the second SAML signing certificate in auth.provider.certificates ends within 30 days"
            ),
            (_, false, false) => {}
        }
    }
}
