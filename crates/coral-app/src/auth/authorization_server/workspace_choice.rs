//! The approval page that ends a sign-in at the Organization MCP Endpoint.
//!
//! The identity provider has answered and Coral knows the User, but no code
//! exists yet. The person approves the client and picks one of their active
//! Workspaces on one page, and the code its submission issues carries that
//! Workspace. Nothing is stored while the page waits: the finished
//! sign-in travels sealed in the form, bound to the browser's approval cookie,
//! like the approval page a Workspace URL shows before sign-in.

use axum::extract::{Request, State};
use axum::http::HeaderMap;
use axum::response::Response;
use coral_outbound::{BrowserRedirect, EndpointUrl};
use coral_workspace::{UserId, WorkspaceChoice, WorkspaceId, WorkspaceRole};

use super::super::sealed_handshake::{PendingWorkspaceChoice, SignInSession};
use super::approval_page::{self, ApprovalDecision, ApprovalScope, Choice};
use super::callback;
use super::response::{TrustedRedirect, direct_error};
use super::{AuthorizationServerHttpState, sign_in_cookie};

/// Where the picker form posts the person's choice.
pub(super) const WORKSPACE_CHOICE_PATH: &str = "/oauth/workspace";
/// Shown when a client registered no name, as registration does.
const UNNAMED_CLIENT: &str = "Unnamed OAuth client";

/// Asks the person who finished `session` to allow the client and pick a
/// Workspace, or says how to get one. No code is issued here.
/// `signed_in_as` is the account the identity provider named, shown so the
/// person can tell why a Workspace is missing.
pub(super) async fn offer(
    state: &AuthorizationServerHttpState,
    headers: &HeaderMap,
    trusted: &TrustedRedirect,
    oidc_state: &str,
    user_id: UserId,
    signed_in_as: Option<&str>,
    session: SignInSession,
) -> Response {
    let choices = match state.workspaces.list_sign_in_choices(&user_id).await {
        Ok(choices) => choices,
        Err(error) => return ChoiceFailure::from_domain(&error).into_error(trusted),
    };
    let providers = provider_summaries(state, &user_id, &choices).await;
    let pending = PendingWorkspaceChoice {
        user_id,
        oidc_state: oidc_state.to_owned(),
        request: session.request,
    };
    let client_name = session.client_name.as_deref().unwrap_or(UNNAMED_CLIENT);
    picker_page(
        state,
        headers,
        &pending,
        client_name,
        signed_in_as,
        &choices,
        &providers,
    )
    .unwrap_or_else(|| trusted.error("server_error", "authorization failed"))
}

/// Issues the code for the Workspace the person picked, after checking again
/// that they can still pick it.
pub(super) async fn oauth_workspace_post(
    State(state): State<AuthorizationServerHttpState>,
    request: Request,
) -> Response {
    let Ok(submission) = approval_page::parse_submission(
        request,
        state.settings.authorization_server().issuer(),
        state.secure_cookies(),
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
    let Some(trusted) = TrustedRedirect::parse(
        &pending.request.redirect_uri,
        pending.request.client_state.clone(),
    ) else {
        return direct_error("server_error", "authorization failed");
    };
    let workspace = match (submission.decision, submission.workspace) {
        (ApprovalDecision::Continue, Some(workspace)) => workspace,
        // Connect without a pick is a form the page never sends.
        (ApprovalDecision::Continue, None) => return choice_error(),
        (ApprovalDecision::Cancel, _) => {
            return trusted.error("access_denied", "authorization was denied");
        }
    };
    // The page may have waited minutes; the choice counts only if the User
    // could still pick it now.
    let Ok(workspace) = WorkspaceId::parse(&workspace) else {
        return ChoiceFailure::NotAvailable.into_error(&trusted);
    };
    let workspace = match state
        .workspaces
        .sign_in_choice(&pending.user_id, &workspace)
        .await
    {
        Ok(choice) => choice.workspace().id().clone(),
        Err(error) => return ChoiceFailure::from_domain(&error).into_error(&trusted),
    };
    let grant = pending.request.grant(&pending.user_id, Some(workspace));
    callback::issue_code(&state, &trusted, &pending.oidc_state, &grant).await
}

enum ChoiceFailure {
    /// The User can no longer sign in, so they can pick nothing.
    Denied,
    /// The User cannot pick that Workspace now.
    NotAvailable,
    Unavailable,
}

impl ChoiceFailure {
    fn into_error(self, trusted: &TrustedRedirect) -> Response {
        match self {
            Self::Denied => trusted.error("access_denied", "access is disabled"),
            Self::NotAvailable => trusted.error("access_denied", "the workspace is not available"),
            Self::Unavailable => trusted.error("server_error", "authorization failed"),
        }
    }

    fn from_domain(error: &coral_workspace::Error) -> Self {
        match error {
            coral_workspace::Error::PermissionDenied(_) => Self::Denied,
            coral_workspace::Error::WorkspaceNotFound => Self::NotAvailable,
            _ => {
                tracing::warn!("Workspace picker could not read the User's Workspaces");
                Self::Unavailable
            }
        }
    }
}

/// Each choice's usable Providers, named for display: "GitHub, Linear, Slack". `None` where they cannot be read;
/// the page then shows the Workspace without them rather than failing. One query names every choice's Providers.
async fn provider_summaries(
    state: &AuthorizationServerHttpState,
    user_id: &UserId,
    choices: &[WorkspaceChoice],
) -> Vec<Option<String>> {
    let unread = || vec![None; choices.len()];
    let Some(providers) = state.providers.as_ref() else {
        return unread();
    };
    let Ok(actor) = state.organization.resolve_user_authority(user_id).await else {
        return unread();
    };
    let workspaces = choices
        .iter()
        .map(|choice| choice.workspace().id().clone())
        .collect::<Vec<_>>();
    match providers.active_provider_names(&actor, &workspaces).await {
        Ok(names) => workspaces
            .iter()
            .map(|workspace| {
                names
                    .get(workspace)
                    .and_then(|names| provider_summary(names))
            })
            .collect(),
        Err(_error) => {
            tracing::warn!("Workspace choice could not list the Workspaces' Providers");
            unread()
        }
    }
}

fn provider_summary(names: &[String]) -> Option<String> {
    const SHOWN: usize = 3;
    if names.is_empty() {
        return None;
    }
    let shown = names
        .iter()
        .take(SHOWN)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    Some(match names.len().saturating_sub(SHOWN) {
        0 => shown,
        rest => format!("{shown} +{rest}"),
    })
}

fn picker_page(
    state: &AuthorizationServerHttpState,
    headers: &HeaderMap,
    pending: &PendingWorkspaceChoice,
    client_name: &str,
    signed_in_as: Option<&str>,
    choices: &[WorkspaceChoice],
    providers: &[Option<String>],
) -> Option<Response> {
    let secure = state.secure_cookies();
    let browser_binding = approval_page::browser_binding_for_page(headers, secure).ok()?;
    let expires_at = sign_in_cookie::now_unix_secs()? + approval_page::APPROVAL_MAX_AGE_SECONDS;
    let ticket = pending
        .seal(
            state.settings.handshake_keys(),
            browser_binding.as_bytes(),
            expires_at,
        )
        .ok()?;
    // A role tells the rows apart only when they differ.
    let first_role = choices.first().map(WorkspaceChoice::role);
    let roles_differ = choices
        .iter()
        .any(|choice| Some(choice.role()) != first_role);
    let rows = choices
        .iter()
        .zip(providers)
        .map(|(choice, providers)| {
            let role = match choice.role() {
                _ if !roles_differ => None,
                WorkspaceRole::Owner => Some("Owner".to_owned()),
                WorkspaceRole::Member => Some("Member".to_owned()),
            };
            Choice {
                workspace_id: choice.workspace().id().to_string(),
                name: choice.workspace().display_name().as_str(),
                details: role.into_iter().chain(providers.clone()).collect(),
            }
        })
        .collect::<Vec<_>>();
    let redirect_uri = EndpointUrl::<BrowserRedirect>::parse(&pending.request.redirect_uri).ok()?;
    let scope = if rows.is_empty() {
        ApprovalScope::NoChoice {
            signed_in_as,
            coral_url: state.settings.authorization_server().web_public_url(),
        }
    } else {
        ApprovalScope::Choice {
            choices: &rows,
            signed_in_as,
        }
    };
    approval_page::response(
        &ticket,
        client_name,
        &pending.request.client_id,
        &redirect_uri,
        &scope,
        &browser_binding,
        secure,
    )
}

fn choice_error() -> Response {
    direct_error("invalid_request", "workspace choice is invalid or expired")
}

#[cfg(test)]
mod tests {
    use super::provider_summary;

    #[test]
    fn a_provider_summary_names_the_first_few_and_counts_the_rest() {
        for (names, summary) in [
            (&[][..], None),
            (&["GitHub"][..], Some("GitHub")),
            (
                &["GitHub", "Linear", "Slack"][..],
                Some("GitHub, Linear, Slack"),
            ),
            (
                &["GitHub", "Linear", "Slack", "Stripe", "Zendesk"][..],
                Some("GitHub, Linear, Slack +2"),
            ),
        ] {
            let names = names.iter().map(ToString::to_string).collect::<Vec<_>>();
            assert_eq!(provider_summary(&names).as_deref(), summary);
        }
    }
}
