//! User confirmation page and submission parsing for OAuth authorization.

use axum::body::to_bytes;
use axum::extract::Request;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::rand::{SecureRandom as _, SystemRandom};
use url::{Host, Url, form_urlencoded};
use zeroize::Zeroizing;

use super::super::super::state_store::{
    OAuthAuthorizationApprovalBrowserBinding, OAuthAuthorizationApprovalTicket,
};

const MAX_FORM_BYTES: usize = 256;
const TICKET_LENGTH: usize = 43;
const BROWSER_BINDING_LENGTH: usize = 43;
const APPROVAL_COOKIE_MAX_AGE_SECONDS: u64 = 5 * 60;
const SECURE_APPROVAL_COOKIE_NAME: &str = "__Host-coral_oauth_approval";
const LOOPBACK_APPROVAL_COOKIE_NAME: &str = "coral_oauth_approval";
const CONTENT_SECURITY_POLICY: &str =
    "default-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'";

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ApprovalDecision {
    Continue,
    Cancel,
}

pub(super) fn new_ticket() -> Result<OAuthAuthorizationApprovalTicket, ()> {
    OAuthAuthorizationApprovalTicket::generate(&SystemRandom::new()).map_err(|_error| ())
}

pub(super) fn browser_binding_for_page(
    headers: &HeaderMap,
    secure_cookie: bool,
) -> Result<OAuthAuthorizationApprovalBrowserBinding, ()> {
    if let Some(binding) = browser_binding_from_headers(headers, secure_cookie) {
        return Ok(binding);
    }
    let mut binding = Zeroizing::new([0_u8; 32]);
    SystemRandom::new()
        .fill(&mut *binding)
        .map_err(|_error| ())?;
    Ok(OAuthAuthorizationApprovalBrowserBinding::from_bytes(
        *binding,
    ))
}

pub(super) async fn parse_submission(
    request: Request,
    expected_origin: &str,
    secure_cookie: bool,
) -> Result<
    (
        OAuthAuthorizationApprovalTicket,
        OAuthAuthorizationApprovalBrowserBinding,
        ApprovalDecision,
    ),
    (),
> {
    let (parts, body) = request.into_parts();
    if !has_exact_origin(&parts.headers, expected_origin) {
        return Err(());
    }
    let browser_binding = browser_binding_from_headers(&parts.headers, secure_cookie).ok_or(())?;
    let content_type = parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    if content_type != Some("application/x-www-form-urlencoded") {
        return Err(());
    }
    let body = to_bytes(body, MAX_FORM_BYTES).await.map_err(|_error| ())?;
    let mut ticket = None;
    let mut decision = None;
    for (index, (name, value)) in form_urlencoded::parse(&body).enumerate() {
        if index >= 2 {
            return Err(());
        }
        match name.as_ref() {
            "ticket" if ticket.is_none() => ticket = Some(value.into_owned()),
            "decision" if decision.is_none() => {
                decision = Some(match value.as_ref() {
                    "continue" => ApprovalDecision::Continue,
                    "cancel" => ApprovalDecision::Cancel,
                    _ => return Err(()),
                });
            }
            _ => return Err(()),
        }
    }
    let encoded = ticket
        .filter(|ticket| ticket.len() == TICKET_LENGTH)
        .ok_or(())?;
    let mut bytes = Zeroizing::new([0_u8; 32]);
    let decoded = URL_SAFE_NO_PAD
        .decode_slice(encoded.as_bytes(), &mut *bytes)
        .map_err(|_error| ())?;
    if decoded != bytes.len() {
        return Err(());
    }
    let ticket = OAuthAuthorizationApprovalTicket::from_bytes(*bytes);
    Ok((ticket, browser_binding, decision.ok_or(())?))
}

pub(super) fn response(
    ticket: &OAuthAuthorizationApprovalTicket,
    client_name: &str,
    client_id: &str,
    redirect_uri: &Url,
    browser_binding: &OAuthAuthorizationApprovalBrowserBinding,
    secure_cookie: bool,
) -> Option<Response> {
    let client_host = Url::parse(client_id)
        .ok()?
        .host()
        .as_ref()
        .map(display_host)?;
    let redirect_host = redirect_uri.host().as_ref().map(display_host)?;
    let redirect_destination = format!("{redirect_host}:{}", redirect_uri.port_or_known_default()?);
    let warning = is_loopback(redirect_uri).then(|| {
        format!(
            "<aside role=\"alert\"><h2>Local redirect warning</h2><p>This redirect targets <code>{}</code> on this device. Continue only if you started this sign-in from a trusted local Coral client.</p></aside>",
            escape_html(&redirect_destination)
        )
    });
    let encoded_ticket = Zeroizing::new(URL_SAFE_NO_PAD.encode(ticket.as_bytes()));
    let body = format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>Approve Coral access</title></head><body><main><h1>Approve access?</h1><p><bdi>{}</bdi> is requesting access to Coral.</p><dl><dt>Client ID hostname</dt><dd><code>{}</code></dd><dt>Redirect host and port</dt><dd><code>{}</code></dd></dl>{}<form method=\"post\" action=\"/oauth/authorize\" autocomplete=\"off\"><input type=\"hidden\" name=\"ticket\" value=\"{}\"><button type=\"submit\" name=\"decision\" value=\"continue\">Continue</button><button type=\"submit\" name=\"decision\" value=\"cancel\">Cancel</button></form></main></body></html>",
        escape_html(client_name),
        escape_html(&client_host),
        escape_html(&redirect_destination),
        warning.unwrap_or_default(),
        escape_html(&encoded_ticket),
    );
    let mut response = (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
            (header::PRAGMA, "no-cache"),
            (header::CONTENT_SECURITY_POLICY, CONTENT_SECURITY_POLICY),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (header::REFERRER_POLICY, "no-referrer"),
            (header::X_FRAME_OPTIONS, "DENY"),
        ],
        body,
    )
        .into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        approval_cookie_header(browser_binding, secure_cookie)?,
    );
    Some(response)
}

fn approval_cookie_header(
    browser_binding: &OAuthAuthorizationApprovalBrowserBinding,
    secure: bool,
) -> Option<HeaderValue> {
    let name = approval_cookie_name(secure);
    let encoded_binding = Zeroizing::new(URL_SAFE_NO_PAD.encode(browser_binding.as_bytes()));
    let secure_attribute = if secure { "; Secure" } else { "" };
    HeaderValue::from_str(&format!(
        "{name}={}; Path=/; Max-Age={APPROVAL_COOKIE_MAX_AGE_SECONDS}; \
         HttpOnly{secure_attribute}; SameSite=Lax",
        encoded_binding.as_str()
    ))
    .ok()
}

fn browser_binding_from_headers(
    headers: &HeaderMap,
    secure: bool,
) -> Option<OAuthAuthorizationApprovalBrowserBinding> {
    let expected_name = approval_cookie_name(secure);
    let mut encoded = None;
    for header_value in headers.get_all(header::COOKIE) {
        let header_value = header_value.to_str().ok()?;
        for pair in header_value
            .split(';')
            .filter_map(|pair| pair.trim().split_once('='))
        {
            if pair.0 != expected_name {
                continue;
            }
            if encoded.is_some() {
                return None;
            }
            encoded = Some(Zeroizing::new(pair.1.to_string()));
        }
    }
    let encoded = encoded.filter(|value| value.len() == BROWSER_BINDING_LENGTH)?;
    let mut bytes = Zeroizing::new([0_u8; 32]);
    let decoded = URL_SAFE_NO_PAD
        .decode_slice(encoded.as_bytes(), &mut *bytes)
        .ok()?;
    (decoded == bytes.len()).then(|| OAuthAuthorizationApprovalBrowserBinding::from_bytes(*bytes))
}

fn has_exact_origin(headers: &HeaderMap, expected_origin: &str) -> bool {
    let mut origins = headers.get_all(header::ORIGIN).iter();
    let Some(origin) = origins.next() else {
        return false;
    };
    origins.next().is_none()
        && origin
            .to_str()
            .is_ok_and(|origin| origin == expected_origin)
}

fn approval_cookie_name(secure: bool) -> &'static str {
    if secure {
        SECURE_APPROVAL_COOKIE_NAME
    } else {
        LOOPBACK_APPROVAL_COOKIE_NAME
    }
}

fn display_host(host: &Host<&str>) -> String {
    match host {
        Host::Domain(host) => host.to_string(),
        Host::Ipv4(host) => host.to_string(),
        Host::Ipv6(host) => format!("[{host}]"),
    }
}

fn is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(host)) => {
            let host = host.trim_end_matches('.').to_ascii_lowercase();
            host == "localhost" || host.ends_with(".localhost")
        }
        Some(Host::Ipv4(host)) => host.is_loopback(),
        Some(Host::Ipv6(host)) => {
            host.is_loopback() || host.to_ipv4_mapped().is_some_and(|host| host.is_loopback())
        }
        None => false,
    }
}

fn escape_html(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        escaped.push_str(match character {
            '&' => "&amp;",
            '<' => "&lt;",
            '>' => "&gt;",
            '"' => "&quot;",
            '\'' => "&#39;",
            _ => {
                escaped.push(character);
                continue;
            }
        });
    }
    escaped
}
