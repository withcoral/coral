//! Responses shared by the authorization-server handlers.
//!
//! Every response here carries an authorization code, an error a client acts
//! on, or a redirect to a browser, so all three share one set of security
//! headers and one way of building a redirect. Keeping them here means a
//! handler cannot ship with a weaker header set than its siblings — the state
//! this module was extracted to end, where one handler set `referrer-policy`
//! on its direct errors and the other did not.
//!
//! # Trust
//!
//! Only two kinds of value reach a response body or a `Location` here: a
//! `&'static str` chosen by the calling handler, and values the client itself
//! supplied and Coral stored. Nothing an upstream provider sends is passed in,
//! so a provider cannot steer a redirect or place text in an error.

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use url::Url;

/// A redirect target already checked against a client's registration.
///
/// The URL is not re-validated here. It reaches this type having passed
/// [`BrowserRedirect`](crate::outbound_url_policy::BrowserRedirect) at the
/// point it was matched against the registry, which is the only place with the
/// registration to match against.
///
/// # Known gap
///
/// A registered redirect URI may itself carry a `code`, `state`, or
/// `error` query parameter, and [`Self::redirect`] appends rather than
/// replaces — so such a URI yields two of that parameter, and a client reading
/// the first occurrence reads the registered one instead of ours.
/// `BrowserRedirect` permits any query, so nothing rejects this today. It is
/// unreachable while the client registry is empty, and rejecting reserved
/// parameters belongs at registration, where the URI is first accepted; the
/// client-metadata-document work is where that check goes.
pub(super) struct TrustedRedirect {
    url: Url,
    client_state: Option<String>,
}

impl TrustedRedirect {
    /// Binds a checked callback to the `state` its client sent, if any.
    pub(super) fn new(url: Url, client_state: Option<String>) -> Self {
        Self { url, client_state }
    }

    /// Redirects the browser back to the client with an authorization code.
    pub(super) fn success(&self, code: &str) -> Response {
        self.redirect("code", code, None)
    }

    /// Redirects the browser back to the client with a fixed OAuth error.
    ///
    /// Both strings are `&'static str` so an error can only ever say what this
    /// crate wrote, never what a provider or a store reported.
    pub(super) fn error(&self, error: &'static str, description: &'static str) -> Response {
        self.redirect("error", error, Some(description))
    }

    fn redirect(&self, key: &str, value: &str, description: Option<&str>) -> Response {
        let mut url = self.url.clone();
        let mut query = url.query_pairs_mut();
        query.append_pair(key, value);
        if let Some(description) = description {
            query.append_pair("error_description", description);
        }
        if let Some(state) = &self.client_state {
            query.append_pair("state", state);
        }
        drop(query);
        redirect(url.as_str())
    }
}

/// Reports an OAuth error directly, for a request with no callback to trust.
///
/// A request that failed before its redirect URI was checked has nowhere safe
/// to be sent, so the error is rendered here rather than redirected — a
/// redirect to an unvalidated URI would make Coral the open redirector.
pub(super) fn direct_error(error: &'static str, description: &'static str) -> Response {
    let body = serde_json::json!({
        "error": error,
        "error_description": description,
    })
    .to_string();
    (
        StatusCode::BAD_REQUEST,
        security_headers(),
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

/// Sends the browser to `location` with no cached copy and no referrer.
pub(super) fn redirect(location: &str) -> Response {
    (
        StatusCode::FOUND,
        security_headers(),
        [(header::LOCATION, location)],
        "",
    )
        .into_response()
}

/// Headers every authorization-server response carries.
///
/// Authorization codes travel in URLs, so `no-store`/`no-cache` keep one out of
/// a shared cache and `no-referrer` keeps one out of the `Referer` a client's
/// page sends onward.
fn security_headers() -> [(header::HeaderName, &'static str); 3] {
    [
        (header::CACHE_CONTROL, "no-store"),
        (header::PRAGMA, "no-cache"),
        (header::REFERRER_POLICY, "no-referrer"),
    ]
}
