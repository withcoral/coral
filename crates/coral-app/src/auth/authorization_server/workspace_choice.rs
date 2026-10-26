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
use axum::http::HeaderMap;
use axum::response::Response;
use coral_auth::authorization_codes::AuthorizationGrant;
use coral_outbound::{BrowserRedirect, EndpointUrl};
use coral_provider::{ProviderCallRoot, ProviderState};
use coral_workspace::{UserId, WorkspaceChoice, WorkspaceId, WorkspaceModule, WorkspaceRole};

use super::super::sealed_handshake::{PendingWorkspaceChoice, SignInSession};
use super::authorize::confirmation::{self, ApprovalDecision, ApprovalScope, escape_html};
use super::callback::{self, CodeOutcome};
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
    user_id: String,
    signed_in_as: Option<&str>,
    session: SignInSession,
) -> Response {
    let choices = match choices(state, &user_id).await {
        Ok(choices) => choices,
        Err(failure) => return failure.into_error(trusted),
    };
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
    let providers = provider_summaries(state, &pending.user_id, &choices).await;
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
    let Ok(submission) = confirmation::parse_workspace_submission(
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
    let workspace = match pick(&state, &pending.user_id, &workspace).await {
        Ok(choice) => choice.workspace().id().to_string(),
        Err(failure) => return failure.into_error(&trusted),
    };
    let grant = AuthorizationGrant {
        user_id: pending.user_id,
        client_id: pending.client_id,
        redirect_uri: pending.redirect_uri,
        code_challenge: pending.code_challenge,
        resource: pending.resource,
        workspace_id: Some(workspace),
    };
    match callback::issue_code(&state, &trusted, &pending.oidc_state, &grant).await {
        CodeOutcome::Issued(response) | CodeOutcome::Refused(response) => response,
    }
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

fn workspaces(state: &AuthorizationServerHttpState) -> Result<&WorkspaceModule, ChoiceFailure> {
    state.workspaces.as_ref().ok_or_else(|| {
        tracing::warn!("Workspace picker has no database to list Workspaces from");
        ChoiceFailure::Unavailable
    })
}

async fn choices(
    state: &AuthorizationServerHttpState,
    user_id: &str,
) -> Result<Vec<WorkspaceChoice>, ChoiceFailure> {
    let user_id = UserId::parse(user_id).map_err(|_error| ChoiceFailure::Unavailable)?;
    workspaces(state)?
        .list_sign_in_choices(&user_id)
        .await
        .map_err(|error| ChoiceFailure::from_domain(&error))
}

async fn pick(
    state: &AuthorizationServerHttpState,
    user_id: &str,
    workspace: &str,
) -> Result<WorkspaceChoice, ChoiceFailure> {
    let user_id = UserId::parse(user_id).map_err(|_error| ChoiceFailure::Unavailable)?;
    let workspace = WorkspaceId::parse(workspace).map_err(|_error| ChoiceFailure::NotAvailable)?;
    workspaces(state)?
        .sign_in_choice(&user_id, &workspace)
        .await
        .map_err(|error| ChoiceFailure::from_domain(&error))
}

/// Each choice's usable Providers, named for display: "GitHub, Linear, Slack". `None` where they cannot be read;
/// the page then shows the Workspace without them rather than failing.
async fn provider_summaries(
    state: &AuthorizationServerHttpState,
    user_id: &str,
    choices: &[WorkspaceChoice],
) -> Vec<Option<String>> {
    let mut summaries = vec![None; choices.len()];
    let Some(lister) = state.providers.as_ref() else {
        return summaries;
    };
    let Ok(user_id) = UserId::parse(user_id) else {
        return summaries;
    };
    let Ok(actor) = lister.organization.resolve_user_authority(&user_id).await else {
        return summaries;
    };
    for (summary, choice) in summaries.iter_mut().zip(choices) {
        match lister
            .providers
            .inspect_providers(&actor, choice.workspace().id())
            .await
        {
            Ok(providers) => {
                // The Built-in Provider is in every Workspace, and a Disabled
                // one runs nothing, so neither tells the choices apart.
                let mut names = providers
                    .iter()
                    .filter(|provider| {
                        provider.state() == ProviderState::Active
                            && !matches!(provider.call_root(), ProviderCallRoot::Builtin)
                    })
                    .map(|provider| provider.display_name().to_owned())
                    .collect::<Vec<_>>();
                names.sort_by_key(|name| name.to_lowercase());
                *summary = provider_summary(&names);
            }
            Err(_error) => {
                tracing::warn!("Workspace choice could not list a Workspace's Providers");
            }
        }
    }
    summaries
}

/// The first few Provider names, then how many more: "GitHub, Linear, Slack +2".
/// A Workspace without Providers shows nothing, so the ones with them stand out.
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
    let browser_binding = confirmation::browser_binding_for_page(headers, secure).ok()?;
    let expires_at = sign_in_cookie::now_unix_secs()? + confirmation::APPROVAL_MAX_AGE_SECONDS;
    let ticket = pending
        .seal(
            state.settings.handshake_keys(),
            browser_binding.as_bytes(),
            expires_at,
        )
        .ok()?;
    // Nothing is preselected, so a quick Allow cannot pick a Workspace the
    // person never looked at; `required` makes the browser ask for one. A
    // role tells the rows apart only when they differ.
    let first_role = choices.first().map(WorkspaceChoice::role);
    let roles_differ = choices
        .iter()
        .any(|choice| Some(choice.role()) != first_role);
    let mut options = String::new();
    for (choice, providers) in choices.iter().zip(providers) {
        let role = match choice.role() {
            _ if !roles_differ => None,
            WorkspaceRole::Owner => Some("Owner"),
            WorkspaceRole::Member => Some("Member"),
        };
        let meta = role
            .map(str::to_owned)
            .into_iter()
            .chain(providers.as_deref().map(escape_html))
            .collect::<Vec<_>>()
            .join(" · ");
        let name = escape_html(choice.workspace().display_name().as_str());
        write!(
            options,
            "\n          <label class=\"choice\"><input type=\"radio\" name=\"workspace\" \
             value=\"{}\" required><span class=\"choice-text\"><span class=\"choice-name\">\
             <bdi>{name}</bdi></span><span class=\"choice-meta\">{meta}</span></span></label>",
            choice.workspace().id(),
        )
        .ok()?;
    }
    let redirect_uri = EndpointUrl::<BrowserRedirect>::parse(&pending.redirect_uri).ok()?;
    let scope = if choices.is_empty() {
        ApprovalScope::NoChoice {
            signed_in_as,
            coral_url: state.settings.authorization_server().web_public_url(),
        }
    } else {
        ApprovalScope::Choice {
            options: &options,
            signed_in_as,
        }
    };
    confirmation::response(
        &ticket,
        client_name,
        &pending.client_id,
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
