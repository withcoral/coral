//! The approval page that ends a sign-in at the Organization MCP Endpoint.
//!
//! The identity provider has answered and Coral knows the User, but no code
//! exists yet. The person approves the client and picks one of their active
//! Workspaces on one page, and the code its submission issues carries that
//! Workspace (ADR 0025). Nothing is stored while the page waits: the finished
//! sign-in travels sealed in the form, bound to the browser's approval cookie,
//! like the approval page a Workspace URL shows before sign-in.

use std::fmt::Write as _;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, header};
use axum::response::Response;
use coral_auth::authorization_codes::AuthorizationGrant;
use coral_outbound::{BrowserRedirect, EndpointUrl};
use coral_workspace::{UserId, WorkspaceChoice, WorkspaceRole};

use super::super::sealed_handshake::{PendingWorkspaceChoice, SignInSession};
use super::authorize::confirmation::{self, ApprovalDecision, ApprovalScope, escape_html};
use super::callback::{self, CodeOutcome};
use super::response::{TrustedRedirect, direct_error};
use super::{AuthorizationServerHttpState, cookies, sign_in_cookie};

/// Where the picker form posts the person's choice.
pub(super) const WORKSPACE_CHOICE_PATH: &str = "/oauth/workspace";
const NO_WORKSPACE_PAGE: &str = include_str!("authorize/no_workspace.html");
/// The browser's last choice. It is a preference for the picker only: it
/// selects nothing the User cannot pick anyway, and it grants nothing.
const SECURE_PREFERENCE_COOKIE: &str = "__Host-coral_oauth_workspace";
const LOOPBACK_PREFERENCE_COOKIE: &str = "coral_oauth_workspace";
const PREFERENCE_MAX_AGE_SECONDS: u64 = 30 * 24 * 60 * 60;
/// Shown when a client registered no name, as registration does.
const UNNAMED_CLIENT: &str = "Unnamed OAuth client";

/// Asks the person who finished `session` to allow the client and pick a
/// Workspace, or says how to get one. No code is issued here.
pub(super) async fn offer(
    state: &AuthorizationServerHttpState,
    headers: &HeaderMap,
    trusted: &TrustedRedirect,
    oidc_state: &str,
    user_id: String,
    session: SignInSession,
) -> Response {
    let choices = match choices(state, &user_id).await {
        Ok(choices) => choices,
        Err(failure) => return failure.into_error(trusted),
    };
    let rendered = if choices.is_empty() {
        no_workspace_page(state)
    } else {
        let pending = PendingWorkspaceChoice {
            user_id,
            oidc_state: oidc_state.to_owned(),
            client_id: session.client_id,
            redirect_uri: session.redirect_uri,
            client_state: session.client_state,
            code_challenge: session.code_challenge,
            resource: session.resource,
        };
        let client_name = session.client_name.as_deref().unwrap_or(UNNAMED_CLIENT);
        picker_page(state, headers, &pending, client_name, &choices)
    };
    rendered.unwrap_or_else(|| trusted.error("server_error", "authorization failed"))
}

/// Issues the code for the Workspace the person picked, after checking again
/// that they can still pick it.
pub(super) async fn oauth_workspace_post(
    State(state): State<AuthorizationServerHttpState>,
    request: Request,
) -> Response {
    let secure = state.secure_cookies();
    let Ok(submission) = confirmation::parse_workspace_submission(
        request,
        state.settings.authorization_server().issuer(),
        secure,
    )
    .await
    else {
        return choice_error();
    };
    let Some(pending) = sign_in_cookie::now_unix_secs().and_then(|now| {
        PendingWorkspaceChoice::open(
            state.settings.handshake_keys(),
            submission.browser_binding.as_bytes(),
            submission.ticket.as_bytes(),
            now,
        )
    }) else {
        return choice_error();
    };
    let Some(trusted) = TrustedRedirect::parse(&pending.redirect_uri, pending.client_state.clone())
    else {
        return direct_error("server_error", "authorization failed");
    };
    let (ApprovalDecision::Continue, Some(workspace)) = (submission.decision, submission.workspace)
    else {
        return trusted.error("access_denied", "authorization was denied");
    };
    // The page may have waited minutes; the choice counts only if the User
    // could still pick it now.
    match choices(&state, &pending.user_id).await {
        Ok(choices)
            if choices
                .iter()
                .any(|choice| choice.workspace().id().to_string() == workspace) => {}
        Ok(_unavailable) => {
            return trusted.error("access_denied", "the workspace is not available");
        }
        Err(failure) => return failure.into_error(&trusted),
    }
    let grant = AuthorizationGrant {
        user_id: pending.user_id,
        client_id: pending.client_id,
        redirect_uri: pending.redirect_uri,
        code_challenge: pending.code_challenge,
        resource: pending.resource,
        workspace_id: Some(workspace.clone()),
    };
    match callback::issue_code(&state, &trusted, &pending.oidc_state, &grant).await {
        CodeOutcome::Issued(mut issued) => {
            if let Some(cookie) = preference_cookie(&workspace, secure) {
                issued.headers_mut().append(header::SET_COOKIE, cookie);
            }
            issued
        }
        CodeOutcome::Refused(refused) => refused,
    }
}

enum ChoiceFailure {
    /// The User can no longer sign in, so they can pick nothing.
    Denied,
    Unavailable,
}

impl ChoiceFailure {
    fn into_error(self, trusted: &TrustedRedirect) -> Response {
        match self {
            Self::Denied => trusted.error("access_denied", "access is disabled"),
            Self::Unavailable => trusted.error("server_error", "authorization failed"),
        }
    }
}

async fn choices(
    state: &AuthorizationServerHttpState,
    user_id: &str,
) -> Result<Vec<WorkspaceChoice>, ChoiceFailure> {
    let Some(workspaces) = state.workspaces.as_ref() else {
        tracing::warn!("Workspace picker has no database to list Workspaces from");
        return Err(ChoiceFailure::Unavailable);
    };
    let user_id = UserId::parse(user_id).map_err(|_error| ChoiceFailure::Unavailable)?;
    workspaces
        .list_sign_in_choices(&user_id)
        .await
        .map_err(|error| {
            if matches!(error, coral_workspace::Error::PermissionDenied(_)) {
                ChoiceFailure::Denied
            } else {
                tracing::warn!("Workspace picker could not list the User's Workspaces");
                ChoiceFailure::Unavailable
            }
        })
}

fn picker_page(
    state: &AuthorizationServerHttpState,
    headers: &HeaderMap,
    pending: &PendingWorkspaceChoice,
    client_name: &str,
    choices: &[WorkspaceChoice],
) -> Option<Response> {
    let secure = state.secure_cookies();
    let browser_binding = confirmation::browser_binding_for_page(headers, secure).ok()?;
    let expires_at = sign_in_cookie::now_unix_secs()? + confirmation::APPROVAL_MAX_AGE_SECONDS;
    let ticket = pending
        .seal(
            state.settings.handshake_keys(),
            browser_binding.as_bytes(),
            expires_at,
        )
        .ok()?;
    let preferred = cookies::single_value(headers, preference_cookie_name(secure))
        .and_then(|value| String::from_utf8(value.to_vec()).ok())
        .filter(|preferred| {
            choices
                .iter()
                .any(|choice| choice.workspace().id().to_string() == *preferred)
        });
    let mut options = String::new();
    for (index, choice) in choices.iter().enumerate() {
        let id = choice.workspace().id().to_string();
        let checked = preferred
            .as_deref()
            .map_or(index == 0, |preferred| preferred == id);
        write!(
            options,
            "<label class=\"choice\"><input type=\"radio\" name=\"workspace\" \
             value=\"{id}\"{}><span class=\"choice-name\"><bdi>{}</bdi></span>\
             <span class=\"choice-role\">{}</span></label>",
            if checked { " checked" } else { "" },
            escape_html(choice.workspace().display_name().as_str()),
            match choice.role() {
                WorkspaceRole::Owner => "Owner",
                WorkspaceRole::Member => "Member",
            },
        )
        .ok()?;
    }
    let redirect_uri = EndpointUrl::<BrowserRedirect>::parse(&pending.redirect_uri).ok()?;
    confirmation::response(
        &ticket,
        client_name,
        &pending.client_id,
        &redirect_uri,
        &ApprovalScope::Choice { options: &options },
        &browser_binding,
        secure,
    )
}

fn no_workspace_page(state: &AuthorizationServerHttpState) -> Option<Response> {
    let link = state
        .settings
        .authorization_server()
        .web_public_url()
        .map(|url| {
            format!(
                "<p class=\"actions\"><a href=\"{}\">Open Coral</a></p>",
                escape_html(url)
            )
        })
        .unwrap_or_default();
    let body = confirmation::render(NO_WORKSPACE_PAGE, |slot| {
        (slot == "coral_link").then(|| link.clone())
    })?;
    confirmation::page(body, None, state.secure_cookies())
}

fn preference_cookie_name(secure: bool) -> &'static str {
    if secure {
        SECURE_PREFERENCE_COOKIE
    } else {
        LOOPBACK_PREFERENCE_COOKIE
    }
}

fn preference_cookie(workspace: &str, secure: bool) -> Option<HeaderValue> {
    let secure_attribute = if secure { "; Secure" } else { "" };
    HeaderValue::from_str(&format!(
        "{}={workspace}; Path=/; Max-Age={PREFERENCE_MAX_AGE_SECONDS}; \
         HttpOnly{secure_attribute}; SameSite=Lax",
        preference_cookie_name(secure)
    ))
    .ok()
}

fn choice_error() -> Response {
    direct_error("invalid_request", "workspace choice is invalid or expired")
}
