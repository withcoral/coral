//! User confirmation page and submission parsing for OAuth authorization.

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

use super::super::cookies;
use super::super::response::approval_page_security_headers;
use super::super::workspace_choice::WORKSPACE_CHOICE_PATH;

/// Room for a sealed approval: the app inputs, a client name from a metadata
/// document of at most 5 KiB, and the resource, base64url encoded.
const MAX_FORM_BYTES: usize = 16 * 1024;
const BROWSER_BINDING_LENGTH: usize = 43;
/// How long both the approval cookie and a sealed approval last.
pub(in crate::auth::authorization_server) const APPROVAL_MAX_AGE_SECONDS: u64 = 5 * 60;
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

pub(super) const APPROVAL_PAGE_STYLES: &str = include_str!("approval.css");
const APPROVAL_PAGE: &str = include_str!("approval.html");
const AUTHORIZE_PATH: &str = "/oauth/authorize";

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::auth::authorization_server) enum ApprovalDecision {
    Continue,
    Cancel,
}

/// The secret in the browser's approval cookie that sealed approvals are bound to.
pub(crate) struct BrowserBinding([u8; 32]);

impl BrowserBinding {
    pub(super) fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(in crate::auth::authorization_server) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Drop for BrowserBinding {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

pub(in crate::auth::authorization_server) fn browser_binding_for_page(
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

pub(super) async fn parse_submission(
    request: Request,
    expected_origin: &str,
    secure_cookie: bool,
) -> Result<(Zeroizing<String>, BrowserBinding, ApprovalDecision), ()> {
    let (browser_binding, form) = read_form(request, expected_origin, secure_cookie, 2).await?;
    if form.workspace.is_some() {
        return Err(());
    }
    Ok((form.ticket, browser_binding, form.decision))
}

/// The fields of the Workspace picker's submission.
pub(in crate::auth::authorization_server) struct WorkspaceSubmission {
    pub(in crate::auth::authorization_server) ticket: Zeroizing<String>,
    pub(in crate::auth::authorization_server) browser_binding: BrowserBinding,
    pub(in crate::auth::authorization_server) decision: ApprovalDecision,
    /// Absent only when the person cancels without a choice.
    pub(in crate::auth::authorization_server) workspace: Option<String>,
}

/// Parses the Workspace picker's form with the same origin, cookie and
/// content checks as the approval form.
pub(in crate::auth::authorization_server) async fn parse_workspace_submission(
    request: Request,
    expected_origin: &str,
    secure_cookie: bool,
) -> Result<WorkspaceSubmission, ()> {
    let (browser_binding, form) = read_form(request, expected_origin, secure_cookie, 3).await?;
    if form.decision == ApprovalDecision::Continue && form.workspace.is_none() {
        return Err(());
    }
    Ok(WorkspaceSubmission {
        ticket: form.ticket,
        browser_binding,
        decision: form.decision,
        workspace: form.workspace,
    })
}

struct Form {
    ticket: Zeroizing<String>,
    decision: ApprovalDecision,
    workspace: Option<String>,
}

/// Reads at most `max_fields` form fields from the browser that holds an
/// approval cookie, submitted from this server's exact origin.
async fn read_form(
    request: Request,
    expected_origin: &str,
    secure_cookie: bool,
    max_fields: usize,
) -> Result<(BrowserBinding, Form), ()> {
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
        if index >= max_fields {
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
    Ok((
        browser_binding,
        Form {
            ticket,
            decision: decision.ok_or(())?,
            workspace,
        },
    ))
}

/// What an approval page asks the person to allow.
pub(in crate::auth::authorization_server) enum ApprovalScope<'a> {
    /// A Workspace URL names its one Workspace, so the page shows before sign-in.
    Workspace,
    /// At the Organization MCP Endpoint the signed-in person also picks the
    /// Workspace, from `options` that trusted code rendered from escaped names:
    /// one radio row per Workspace.
    /// `signed_in_as` names the account they signed in with, when the
    /// identity provider gave one.
    Choice {
        options: &'a str,
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

/// Renders the approval page for one pending authorization.
///
/// `Cancel` comes before `Allow` in the form because the first submit button
/// is the one a browser presses for a bare Enter key, and the safe decision is
/// the better default for a keystroke nobody aimed at either button. Source
/// order keeps `Cancel` as the submit default. The fixed, hash-pinned
/// stylesheet stacks that safe decision last on a narrow viewport.
pub(in crate::auth::authorization_server) fn response(
    ticket: &str,
    client_name: &str,
    client_id: &str,
    redirect_uri: &EndpointUrl<BrowserRedirect>,
    scope: &ApprovalScope<'_>,
    browser_binding: &BrowserBinding,
    secure_cookie: bool,
) -> Option<Response> {
    let account = |signed_in_as: Option<&str>| {
        signed_in_as.map_or_else(String::new, |account| {
            format!(
                "\n        <p class=\"account\"><span>Signed in as <bdi>{}</bdi></span></p>",
                escape_html(account)
            )
        })
    };
    let (decision_title, form_action, account, choices) = match scope {
        ApprovalScope::Workspace => (
            "Allow access?",
            AUTHORIZE_PATH,
            String::new(),
            String::new(),
        ),
        ApprovalScope::Choice {
            options,
            signed_in_as,
        } => (
            "Choose a workspace",
            WORKSPACE_CHOICE_PATH,
            account(*signed_in_as),
            format!(
                "\n        <div class=\"choices\" role=\"radiogroup\" \
                 aria-labelledby=\"decision-title\">{options}\n        </div>"
            ),
        ),
        ApprovalScope::NoChoice { signed_in_as, .. } => (
            "No workspace to choose",
            WORKSPACE_CHOICE_PATH,
            account(*signed_in_as),
            NO_CHOICE.to_owned(),
        ),
    };
    let access_scope = match scope {
        ApprovalScope::Workspace => "this workspace",
        ApprovalScope::Choice { .. } | ApprovalScope::NoChoice { .. } => "the workspace you choose",
    };
    // With nothing to connect, Cancel is the way back to the client, and
    // Coral takes Connect's place when its URL is known.
    let (cancel_label, primary_action) = match scope {
        ApprovalScope::NoChoice { coral_url, .. } => (
            format!("Back to <bdi>{}</bdi>", escape_html(client_name)),
            coral_url.map_or_else(String::new, |url| {
                format!(
                    "\n          <a class=\"connect\" href=\"{}\">Open Coral</a>",
                    escape_html(url)
                )
            }),
        ),
        ApprovalScope::Workspace | ApprovalScope::Choice { .. } => (
            "Cancel".to_owned(),
            "\n          <button class=\"connect\" type=\"submit\" name=\"decision\" \
             value=\"continue\">Connect</button>"
                .to_owned(),
        ),
    };
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
    let body = render(APPROVAL_PAGE, |slot| {
        Some(match slot {
            "client_name" => escape_html(client_name),
            "client_identity" => escape_html(&client_identity),
            "redirect_label" => redirect_label.to_owned(),
            "redirect_destination" => escape_html(&redirect_destination),
            "ticket" => escape_html(ticket),
            "access_scope" => access_scope.to_owned(),
            "decision_title" => decision_title.to_owned(),
            "form_action" => form_action.to_owned(),
            "account" => account.clone(),
            "choices" => choices.clone(),
            "cancel_label" => cancel_label.clone(),
            "primary_action" => primary_action.clone(),
            _ => return None,
        })
    })?;
    page(body, Some(browser_binding), secure_cookie)
}

/// What a person with no Workspace reads in place of the choices.
const NO_CHOICE: &str = "\n        <div class=\"empty\"><p>You are not a member of a workspace.</p>\
                         <p>Create a workspace in Coral. Then sign in again.</p></div>";

/// Fills a trusted template's `@@slot@@`s, and its `styles`, in one pass.
///
/// Read slots only from the trusted template. Never rescan substituted client
/// text: a client named "@@ticket@@" must remain literal, not become the
/// ticket. An unknown slot fails the whole page.
pub(in crate::auth::authorization_server) fn render(
    template: &str,
    slot: impl Fn(&str) -> Option<String>,
) -> Option<String> {
    let mut body = String::with_capacity(template.len() + APPROVAL_PAGE_STYLES.len());
    for (index, segment) in template.split("@@").enumerate() {
        if index % 2 == 0 {
            body.push_str(segment);
        } else if segment == "styles" {
            body.push_str(APPROVAL_PAGE_STYLES);
        } else {
            body.push_str(&slot(segment)?);
        }
    }
    Some(body)
}

/// Serves a rendered sign-in page under the approval page's policy. A page
/// that carries a sealed ticket also sets the approval cookie it is bound to.
pub(in crate::auth::authorization_server) fn page(
    body: String,
    browser_binding: Option<&BrowserBinding>,
    secure_cookie: bool,
) -> Option<Response> {
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
    if let Some(browser_binding) = browser_binding {
        response.headers_mut().insert(
            header::SET_COOKIE,
            approval_cookie_header(browser_binding, secure_cookie)?,
        );
    }
    Some(response)
}

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

pub(in crate::auth::authorization_server) fn escape_html(value: &str) -> String {
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
