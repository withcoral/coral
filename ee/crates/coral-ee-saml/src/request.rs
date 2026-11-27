// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

//! The request that begins a sign-in: an unsigned `AuthnRequest`, sent by
//! the HTTP-Redirect binding with no `RelayState`.

use std::io::Write as _;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use coral_db::UnixNanos;
use coral_sign_in::SignInValue;
use flate2::Compression;
use flate2::write::DeflateEncoder;
use url::Url;

use crate::xml;

/// Where a request sends the browser, and what it names.
pub(crate) struct RequestTarget<'a> {
    /// The Identity Provider's `sign_in_url`.
    pub(crate) sign_in_url: &'a Url,
    /// Coral's URL that receives the response.
    pub(crate) acs_url: &'a str,
    /// Coral's entity ID.
    pub(crate) coral_entity_id: &'a str,
}

impl RequestTarget<'_> {
    /// The URL that sends the browser to the Identity Provider with the
    /// request of the sign-in `sign_in`, issued at `now`: `sign_in_url` with
    /// `SAMLRequest`, the request deflated (RFC 1951) and in base64, added to
    /// its query.
    ///
    /// # Errors
    /// Returns an error when the request cannot be deflated, which writing
    /// to memory never causes.
    pub(crate) fn redirect(&self, sign_in: &SignInValue, now: UnixNanos) -> std::io::Result<Url> {
        let mut deflater = DeflateEncoder::new(Vec::new(), Compression::default());
        deflater.write_all(self.authn_request(sign_in, now).as_bytes())?;
        let request = STANDARD.encode(deflater.finish()?);
        let mut url = self.sign_in_url.clone();
        url.query_pairs_mut().append_pair("SAMLRequest", &request);
        Ok(url)
    }

    /// The `AuthnRequest` of the sign-in `sign_in` at `now`. Its ID is `_`
    /// and the sign-in's value, which the response returns as
    /// `InResponseTo`. It asks for no `NameID` format and no way of signing
    /// in: the Identity Provider's setup decides both.
    fn authn_request(&self, sign_in: &SignInValue, now: UnixNanos) -> String {
        // UTC, to the second.
        let instant = chrono::DateTime::from_timestamp_nanos(now.get())
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        format!(
            "<samlp:AuthnRequest xmlns:samlp=\"urn:oasis:names:tc:SAML:2.0:protocol\" \
             xmlns:saml=\"urn:oasis:names:tc:SAML:2.0:assertion\" \
             ID=\"_{id}\" Version=\"2.0\" IssueInstant=\"{instant}\" \
             Destination=\"{destination}\" \
             ProtocolBinding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\" \
             AssertionConsumerServiceURL=\"{acs}\">\
             <saml:Issuer>{issuer}</saml:Issuer>\
             </samlp:AuthnRequest>",
            id = xml::escape(sign_in.as_str()),
            destination = xml::escape(self.sign_in_url.as_str()),
            acs = xml::escape(self.acs_url),
            issuer = xml::escape(self.coral_entity_id),
        )
    }
}
