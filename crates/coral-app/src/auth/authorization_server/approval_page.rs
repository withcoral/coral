//! The approval page every sign-in shows, and parsing its submission.
//!
//! A Workspace URL shows it before sign-in, from `/oauth/authorize`; the
//! Organization MCP Endpoint shows it after, from the callback, where the
//! person also picks the Workspace. This module renders every byte of it and
//! escapes every value it is handed.

use std::fmt::Write as _;
use std::sync::LazyLock;

use axum::body::to_bytes;
use axum::extract::Request;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use coral_outbound::{BrowserRedirect, EndpointUrl};
use ring::rand::{SecureRandom as _, SystemRandom};
use sha2::{Digest as _, Sha256};
use url::{Host, Url, form_urlencoded};
use zeroize::Zeroize as _;
use zeroize::Zeroizing;

use super::cookies;
use super::response::approval_page_security_headers;
use super::workspace_choice::WORKSPACE_CHOICE_PATH;

/// Room for a sealed approval: the app inputs, a client name from a metadata
/// document of at most 5 KiB, and the resource, base64url encoded.
const MAX_FORM_BYTES: usize = 16 * 1024;
const BROWSER_BINDING_LENGTH: usize = 43;
/// How long both the approval cookie and a sealed approval last.
pub(super) const APPROVAL_MAX_AGE_SECONDS: u64 = 5 * 60;
const SECURE_APPROVAL_COOKIE_NAME: &str = "__Host-coral_oauth_approval";
const LOOPBACK_APPROVAL_COOKIE_NAME: &str = "coral_oauth_approval";
/// The approval page's policy, which deliberately omits `form-action`.
///
/// Submitting this page's form starts a sign-in that navigates through every
/// origin the flow touches, and a browser applies `form-action` to all of them,
/// redirects included. A single observed sign-in went here, to the provider, to
/// two more provider URLs, back to this server's callback, and finally to the
/// client's redirect URI — five origins, only some of them knowable when this
/// page is rendered. An enumerated list is worse than none: a provider that
/// federates to an enterprise identity provider, adds an MFA or bot-check hop,
/// or simply changes its own redirect path silently breaks every login, and the browser
/// blames this page's same-origin form action rather than the hop it blocked.
/// The POST is delivered before the block, so the user sees nothing happen.
///
/// What the directive would defend against is an injected form exfiltrating
/// data. This page renders no script, takes no user input beyond a hidden
/// ticket, escapes every interpolated value, and already denies every other
/// external fetch through `default-src 'none'`, so the exposure is small and
/// bounded — where a wrong `form-action` breaks the feature outright.
/// Hash the embedded stylesheet once, so editing CSS needs no export step or
/// manually synchronized policy. Fonts come from the device, not the network.
static CONTENT_SECURITY_POLICY: LazyLock<String> = LazyLock::new(|| {
    let hash = STANDARD.encode(Sha256::digest(APPROVAL_PAGE_STYLES.as_bytes()));
    format!(
        "default-src 'none'; base-uri 'none'; frame-ancestors 'none'; style-src 'sha256-{hash}'"
    )
});

pub(super) const APPROVAL_PAGE_STYLES: &str = include_str!("approval_page/approval.css");
const APPROVAL_PAGE: &str = include_str!("approval_page/approval.html");
const AUTHORIZE_PATH: &str = "/oauth/authorize";

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ApprovalDecision {
    Continue,
    Cancel,
}

/// The secret in the browser's approval cookie that sealed approvals are bound to.
pub(super) struct BrowserBinding([u8; 32]);

impl BrowserBinding {
    pub(super) fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(super) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Drop for BrowserBinding {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

pub(super) fn browser_binding_for_page(
    headers: &HeaderMap,
    secure_cookie: bool,
) -> Result<BrowserBinding, ()> {
    if let Some(binding) = browser_binding_from_headers(headers, secure_cookie) {
        return Ok(binding);
    }
    let mut binding = Zeroizing::new([0_u8; 32]);
    SystemRandom::new()
        .fill(&mut *binding)
        .map_err(|_error| ())?;
    Ok(BrowserBinding::from_bytes(*binding))
}

/// The fields of an approval page's submission.
pub(super) struct Submission {
    pub(super) ticket: Zeroizing<String>,
    pub(super) browser_binding: BrowserBinding,
    pub(super) decision: ApprovalDecision,
    /// The Workspace picked at the Organization MCP Endpoint. Only that page
    /// sends one, and only when a row was picked.
    pub(super) workspace: Option<String>,
}

/// Reads an approval page's form from the browser that holds an approval
/// cookie, submitted from this server's exact origin.
pub(super) async fn parse_submission(
    request: Request,
    expected_origin: &str,
    secure_cookie: bool,
) -> Result<Submission, ()> {
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
    let mut workspace = None;
    for (index, (name, value)) in form_urlencoded::parse(&body).enumerate() {
        if index >= 3 {
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
            "workspace" if workspace.is_none() => workspace = Some(value.into_owned()),
            _ => return Err(()),
        }
    }
    // A sealed approval is base64url; anything else cannot open, so it is
    // rejected here with the rest of the malformed form.
    let ticket = Zeroizing::new(
        ticket
            .filter(|ticket| {
                !ticket.is_empty()
                    && ticket
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            })
            .ok_or(())?,
    );
    Ok(Submission {
        ticket,
        browser_binding,
        decision: decision.ok_or(())?,
        workspace,
    })
}

/// What an approval page asks the person to allow.
pub(super) enum ApprovalScope<'a> {
    /// A Workspace URL names its one Workspace, so the page shows before sign-in.
    Workspace,
    /// At the Organization MCP Endpoint the signed-in person also picks one
    /// of `choices`. `signed_in_as` names the account they signed in with,
    /// when the identity provider gave one.
    Choice {
        choices: &'a [Choice<'a>],
        signed_in_as: Option<&'a str>,
    },
    /// The signed-in person has no Workspace to pick. The page says how to
    /// get one, links to Coral at `coral_url` when it is known, and offers
    /// only the way back to the client.
    NoChoice {
        signed_in_as: Option<&'a str>,
        coral_url: Option<&'a str>,
    },
}

/// One Workspace row on the picker, as plain text.
pub(super) struct Choice<'a> {
    /// The value the row submits.
    pub(super) workspace_id: String,
    pub(super) name: &'a str,
    /// Shown after the name, each set apart: a role, the Providers.
    pub(super) details: Vec<String>,
}

/// The slots whose content depends on the [`ApprovalScope`].
struct ScopeSlots {
    decision_title: &'static str,
    form_action: &'static str,
    access_scope: &'static str,
    account: String,
    choices: String,
    cancel_label: String,
    primary_action: String,
}

impl ScopeSlots {
    fn new(scope: &ApprovalScope<'_>, client_name: &str) -> Self {
        const CONNECT: &str = "\n          <button class=\"connect\" type=\"submit\" \
                               name=\"decision\" value=\"continue\">Connect</button>";
        let account = |signed_in_as: Option<&str>| {
            signed_in_as.map_or_else(String::new, |account| {
                format!(
                    "\n        <p class=\"account\"><span>Signed in as <bdi>{}</bdi></span></p>",
                    escape_html(account)
                )
            })
        };
        match scope {
            ApprovalScope::Workspace => Self {
                decision_title: "Allow access?",
                form_action: AUTHORIZE_PATH,
                access_scope: "this workspace",
                account: String::new(),
                choices: String::new(),
                cancel_label: "Cancel".to_owned(),
                primary_action: CONNECT.to_owned(),
            },
            ApprovalScope::Choice {
                choices,
                signed_in_as,
            } => Self {
                decision_title: "Choose a workspace",
                form_action: WORKSPACE_CHOICE_PATH,
                access_scope: "the workspace you choose",
                account: account(*signed_in_as),
                choices: choice_rows(choices),
                cancel_label: "Cancel".to_owned(),
                primary_action: CONNECT.to_owned(),
            },
            // With nothing to connect, Cancel is the way back to the client,
            // and Coral takes Connect's place when its URL is known.
            ApprovalScope::NoChoice {
                signed_in_as,
                coral_url,
            } => Self {
                decision_title: "No workspace to choose",
                form_action: WORKSPACE_CHOICE_PATH,
                access_scope: "the workspace you choose",
                account: account(*signed_in_as),
                choices: NO_CHOICE.to_owned(),
                cancel_label: format!("Back to <bdi>{}</bdi>", escape_html(client_name)),
                primary_action: coral_url.map_or_else(String::new, |url| {
                    format!(
                        "\n          <a class=\"connect\" href=\"{}\">Open Coral</a>",
                        escape_html(url)
                    )
                }),
            },
        }
    }
}

/// One labelled radio group with a row per choice. Nothing is preselected,
/// so a quick Connect cannot pick a Workspace the person never looked at;
/// `required` makes the browser ask for one.
fn choice_rows(choices: &[Choice<'_>]) -> String {
    let mut rows = String::from(
        "\n        <div class=\"choices\" role=\"radiogroup\" aria-labelledby=\"decision-title\">",
    );
    for choice in choices {
        let details = choice
            .details
            .iter()
            .map(|detail| escape_html(detail))
            .collect::<Vec<_>>()
            .join(" · ");
        let _infallible = write!(
            rows,
            "\n          <label class=\"choice\"><input type=\"radio\" name=\"workspace\" \
             value=\"{}\" required><span class=\"choice-text\"><span class=\"choice-name\">\
             <bdi>{}</bdi></span><span class=\"choice-meta\">{details}</span></span></label>",
            escape_html(&choice.workspace_id),
            escape_html(choice.name),
        );
    }
    rows.push_str("\n        </div>");
    rows
}

/// Renders the approval page for one pending authorization.
///
/// `Cancel` comes before `Allow` in the form because the first submit button
/// is the one a browser presses for a bare Enter key, and the safe decision is
/// the better default for a keystroke nobody aimed at either button. Source
/// order keeps `Cancel` as the submit default. The fixed, hash-pinned
/// stylesheet stacks that safe decision last on a narrow viewport.
pub(super) fn response(
    ticket: &str,
    client_name: &str,
    client_id: &str,
    redirect_uri: &EndpointUrl<BrowserRedirect>,
    scope: &ApprovalScope<'_>,
    browser_binding: &BrowserBinding,
    secure_cookie: bool,
) -> Option<Response> {
    let scoped = ScopeSlots::new(scope, client_name);
    // Resolution already validated the identity. A URL has a metadata host;
    // an opaque registration has no verified origin or branding to display.
    // The client names itself, so its host stands alone, without that name.
    let client_identity = Url::parse(client_id)
        .ok()
        .and_then(|url| url.host().as_ref().map(display_host))
        .unwrap_or_else(|| client_id.to_owned());
    // An app-scheme callback names the app the operating system opens, not a
    // host and port.
    let (redirect_label, redirect_destination) =
        if let Some(scheme) = redirect_uri.private_use_scheme() {
            ("Opens app", scheme.to_owned())
        } else {
            let redirect_uri = redirect_uri.as_url();
            let redirect_host = redirect_uri.host().as_ref().map(display_host)?;
            (
                "Returns to",
                format!("{redirect_host}:{}", redirect_uri.port_or_known_default()?),
            )
        };
    // Read slots only from the trusted template. Never rescan substituted
    // client text: a client named "@@ticket@@" must remain literal, not become
    // the ticket. An unknown slot fails the whole page.
    let mut body = String::with_capacity(APPROVAL_PAGE.len() + APPROVAL_PAGE_STYLES.len());
    for (index, segment) in APPROVAL_PAGE.split("@@").enumerate() {
        if index % 2 == 0 {
            body.push_str(segment);
            continue;
        }
        match segment {
            "styles" => body.push_str(APPROVAL_PAGE_STYLES),
            "client_name" => body.push_str(&escape_html(client_name)),
            "client_identity" => body.push_str(&escape_html(&client_identity)),
            "redirect_label" => body.push_str(redirect_label),
            "redirect_destination" => body.push_str(&escape_html(&redirect_destination)),
            "ticket" => body.push_str(&escape_html(ticket)),
            "access_scope" => body.push_str(scoped.access_scope),
            "decision_title" => body.push_str(scoped.decision_title),
            "form_action" => body.push_str(scoped.form_action),
            "account" => body.push_str(&scoped.account),
            "choices" => body.push_str(&scoped.choices),
            "cancel_label" => body.push_str(&scoped.cancel_label),
            "primary_action" => body.push_str(&scoped.primary_action),
            _ => return None,
        }
    }
    let mut response = (
        StatusCode::OK,
        approval_page_security_headers(),
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (
                header::CONTENT_SECURITY_POLICY,
                CONTENT_SECURITY_POLICY.as_str(),
            ),
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

/// What a person with no Workspace reads in place of the choices.
const NO_CHOICE: &str = "\n        <div class=\"empty\"><p>You are not a member of a workspace.</p>\
                         <p>Create a workspace in Coral. Then sign in again.</p></div>";

fn approval_cookie_header(browser_binding: &BrowserBinding, secure: bool) -> Option<HeaderValue> {
    let name = approval_cookie_name(secure);
    let encoded_binding = Zeroizing::new(URL_SAFE_NO_PAD.encode(browser_binding.as_bytes()));
    let secure_attribute = if secure { "; Secure" } else { "" };
    HeaderValue::from_str(&format!(
        "{name}={}; Path=/; Max-Age={APPROVAL_MAX_AGE_SECONDS}; \
         HttpOnly{secure_attribute}; SameSite=Lax",
        encoded_binding.as_str()
    ))
    .ok()
}

/// Recovers the binding this browser was given, from the request's cookies.
///
/// A cookie this server cannot read leaves the binding unreadable in the worst
/// direction: the page handler treats `None` as "no binding yet" and mints a
/// fresh one, while the submission handler treats it as a rejected request, so
/// the user loops through the page with no decision ever taking.
/// [`cookies::single_value`] reads past cookies it cannot interpret and refuses
/// a repeated name, which is why it is the only reader used here.
fn browser_binding_from_headers(headers: &HeaderMap, secure: bool) -> Option<BrowserBinding> {
    let encoded = cookies::single_value(headers, approval_cookie_name(secure))
        .filter(|value| value.len() == BROWSER_BINDING_LENGTH)?;
    let mut bytes = Zeroizing::new([0_u8; 32]);
    let decoded = URL_SAFE_NO_PAD.decode_slice(&**encoded, &mut *bytes).ok()?;
    (decoded == bytes.len()).then(|| BrowserBinding::from_bytes(*bytes))
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
