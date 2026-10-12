//! Shared OAuth callback validation and durable workflow selection.
use super::{AccountModule, AuthorizationId};
use crate::{Error, ProviderSetupId};
use coral_credentials::Secret;
use coral_workspace::ChangeContext;
use coral_workspace::WorkspaceId;

/// Safe reference to the workflow that accepted the callback, including replays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallbackCompletion {
    /// Provider configuration and its setup-scoped account authorization.
    Setup {
        /// Owning scope of the Setup.
        owner: WorkspaceId,
        /// Exact Setup identified by the callback state.
        setup_id: ProviderSetupId,
    },
    /// Account authorization for an already published Provider.
    Account {
        /// Owning scope of the authorization.
        owner: WorkspaceId,
        /// Exact authorization identified by the callback state.
        authorization_id: AuthorizationId,
    },
}

pub(super) enum CallbackResponse {
    Code(Secret),
    Error(&'static str),
}

impl CallbackResponse {
    pub(super) fn fingerprint_input(&self) -> (&str, &str) {
        match self {
            Self::Code(code) => ("code", code.expose()),
            Self::Error(error) => ("error", error),
        }
    }
}

impl AccountModule {
    /// Completes an OAuth callback from its encoded query, without a leading `?`.
    /// Both browser and loopback adapters retain duplicate parameters for validation.
    /// The durable workflow verifies the initiating actor and exchanges the code once.
    /// # Errors
    /// Rejects malformed input and callbacks the actor cannot complete.
    pub async fn complete_callback(
        &self,
        context: &ChangeContext,
        query: &str,
    ) -> Result<CallbackCompletion, Error> {
        if query.len() > 16 * 1024 {
            return Err(Error::AccountInput);
        }
        let mut fields = std::collections::BTreeMap::new();
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            if fields.insert(key, value).is_some() {
                return Err(Error::AccountInput);
            }
        }
        let state = fields
            .get("state")
            .filter(|state| !state.is_empty() && state.len() <= 1024)
            .ok_or(Error::AccountInput)?;
        let response = match (fields.get("code"), fields.get("error")) {
            (Some(code), None) if !code.is_empty() && code.len() <= 4096 => {
                CallbackResponse::Code(Secret::new(code.to_string())?)
            }
            (None, Some(error)) if !error.is_empty() && error.len() <= 1024 => {
                // Persist only bounded host-owned codes, never upstream descriptions.
                CallbackResponse::Error(match error.as_ref() {
                    "access_denied" => "access_denied",
                    _ => "authorization_failed",
                })
            }
            _ => return Err(Error::AccountInput),
        };
        if state.starts_with("account.") {
            let (owner, authorization) = self
                .complete_account_callback(context, state, response)
                .await?;
            Ok(CallbackCompletion::Account {
                owner,
                authorization_id: authorization.id,
            })
        } else {
            let completion = self
                .complete_setup_callback(context, None, state, response)
                .await?;
            Ok(CallbackCompletion::Setup {
                owner: completion.owner,
                setup_id: completion.setup_id,
            })
        }
    }
}
