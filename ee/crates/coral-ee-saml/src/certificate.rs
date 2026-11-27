// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

//! A certificate that the operator pasted, which signs the Identity
//! Provider's responses.

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

    /// The last instant the certificate is valid at.
    #[cfg_attr(not(test), expect(dead_code, reason = "the startup warnings read it"))]
    pub(crate) const fn last_instant(&self) -> UnixNanos {
        self.last_instant
    }
}
