//! Client Prompts (ADR 0029): asking the invoking User in the MCP client's own
//! dialog, through MCP elicitation in protocol revision 2026-07-28.
//!
//! `wait` asks; nothing else does. A client answers a prompt by sending the
//! same call again, and a repeated `wait` only observes the Run again, where a
//! repeated `exec` would run its Program twice.
//!
//! Coral keeps nothing between requests, so the Required Actions already asked
//! travel with the client: in the request state of the call it repeats, and in
//! the cursor every Run Update returns. Neither grants anything. A client that
//! edits them can only be asked again, or not at all, and every observation is
//! authorized as before.

use std::collections::BTreeSet;

use base64::Engine as _;
use coral_run::{
    RequiredAction, RequiredActionRequest, RequiredActor, RunCursor, RunId, RunUpdate,
};
use rmcp::model::{
    ClientCapabilities, ElicitRequest, ElicitRequestParams, InputRequest, InputRequests,
    InputRequiredResult,
};
use serde::{Deserialize, Serialize};

use crate::Elicitation;

const ENCODING: base64::engine::GeneralPurpose = base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// Separates the Run's own cursor from the Required Actions already asked. A
/// Run cursor is URL-safe base64, which never holds it.
const CURSOR_SEPARATOR: char = '.';

/// The Client Prompts one request may be shown: what its client declares, as
/// far as the deployment allows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Ask {
    /// A prompt that opens the Required Action's page.
    Link,
}

impl Ask {
    /// What `elicitation` allows of what `capabilities` declares. None for a
    /// request that declares no elicitation, which every request before
    /// 2026-07-28 is: it carries no capabilities of its own.
    pub(crate) fn of(
        elicitation: Elicitation,
        capabilities: Option<&ClientCapabilities>,
    ) -> Option<Self> {
        let declared = capabilities?.elicitation.as_ref()?;
        match elicitation {
            Elicitation::Off => None,
            Elicitation::Form | Elicitation::Url => declared.url.is_some().then_some(Self::Link),
        }
    }
}

/// The Required Actions of one Run that a Client Prompt already asked about.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Shown(BTreeSet<String>);

impl Shown {
    fn of(action: &RequiredAction) -> String {
        action.request.to_string()
    }

    fn contains(&self, action: &RequiredAction) -> bool {
        self.0.contains(&Self::of(action))
    }

    fn with(mut self, action: &RequiredAction) -> Self {
        self.0.insert(Self::of(action));
        self
    }

    /// Both sets.
    #[must_use]
    pub(crate) fn union(mut self, other: Self) -> Self {
        self.0.extend(other.0);
        self
    }

    /// Only the requests still in `actions`, so the set never outgrows them.
    fn current_in(&self, actions: &[RequiredAction]) -> Self {
        Self(
            actions
                .iter()
                .map(Self::of)
                .filter(|request| self.0.contains(request))
                .collect(),
        )
    }

    fn encode(&self) -> String {
        let joined = self.0.iter().cloned().collect::<Vec<_>>().join("\n");
        ENCODING.encode(joined)
    }

    fn decode(value: &str) -> Option<Self> {
        let joined = String::from_utf8(ENCODING.decode(value).ok()?).ok()?;
        Some(Self(
            joined
                .split('\n')
                .filter(|request| !request.is_empty())
                .map(str::to_owned)
                .collect(),
        ))
    }
}

/// The cursor `wait` and `cancel` take and every Run Update returns: the Run's
/// own cursor, and the Required Actions a Client Prompt already asked about,
/// so each is asked once while the assistant passes the latest cursor on.
pub(crate) struct WaitCursor {
    pub(crate) run: RunCursor,
    pub(crate) shown: Shown,
}

impl WaitCursor {
    /// Reads a cursor that a Run Update returned.
    pub(crate) fn parse(value: &str) -> Result<Self, coral_run::Error> {
        let (run, shown) = match value.split_once(CURSOR_SEPARATOR) {
            Some((run, shown)) => (run, Shown::decode(shown)),
            None => (value, Some(Shown::default())),
        };
        // A cursor with an unreadable tail is as invalid as one with an
        // unreadable Run cursor, and the Run's error says so.
        let run = RunCursor::parse(run)?;
        let shown = shown
            .ok_or_else(|| coral_run::Error::InvalidInput("Run cursor is invalid".to_owned()))?;
        Ok(Self { run, shown })
    }

    /// The cursor a Run Update returns: `run`, with whatever of `shown` is
    /// still among `actions`. Without any, it is the Run's own cursor.
    pub(crate) fn format(run: &RunCursor, shown: &Shown, actions: &[RequiredAction]) -> String {
        let shown = shown.current_in(actions);
        if shown.0.is_empty() {
            run.to_string()
        } else {
            format!("{run}{CURSOR_SEPARATOR}{}", shown.encode())
        }
    }
}

/// What a repeated call carries back: the Run it observed and the Required
/// Actions it asked about.
#[derive(Serialize, Deserialize)]
struct RequestState {
    run: String,
    shown: String,
}

/// The Required Actions that `state`, from the call a client repeats, asked
/// about in `run_id`. None for a state that is not Coral's, or names another
/// Run: the call then observes as if it carried none.
pub(crate) fn shown_in_state(state: &str, run_id: &RunId) -> Option<Shown> {
    let bytes = ENCODING.decode(state).ok()?;
    let state = serde_json::from_slice::<RequestState>(&bytes).ok()?;
    (state.run == run_id.to_string())
        .then(|| Shown::decode(&state.shown))
        .flatten()
}

/// The first Required Action in `update` that names the invoking User and no
/// Client Prompt asked about yet, asked as `ask`. Only the invoking User
/// observes a Run whose request names them, so the prompt reaches that User.
pub(crate) fn prompt(
    update: &RunUpdate,
    shown: &Shown,
    ask: Ask,
    human_interface: &crate::HumanInterfaceUrl,
) -> Option<InputRequiredResult> {
    let action = update.required_actions().iter().find(|action| {
        action.required_actor == RequiredActor::InvokingUser && !shown.contains(action)
    })?;
    let url = human_interface.page_url(&action.action_path);
    let request = match ask {
        Ask::Link => ElicitRequest::new(ElicitRequestParams::UrlElicitationParams {
            meta: None,
            // The client shows the link too, but one that opens it on another
            // host, as over SSH, leaves the person this text to copy.
            message: format!("{} Open {url}", asked(action)),
            url: url.clone(),
            elicitation_id: Shown::of(action),
        }),
    };
    let state = RequestState {
        run: update.run_id().to_string(),
        shown: shown.clone().with(action).encode(),
    };
    let state = serde_json::to_vec(&state).ok()?;
    Some(InputRequiredResult::new(
        Some(InputRequests::from([(
            Shown::of(action),
            InputRequest::Elicitation(request),
        )])),
        Some(ENCODING.encode(state)),
    ))
}

/// What `action` asks of the person, in one sentence.
fn asked(action: &RequiredAction) -> String {
    let provider = action
        .provider_label
        .as_deref()
        .map_or_else(String::new, |label| format!(" ({label})"));
    match (&action.request, action.call_path.as_deref()) {
        (RequiredActionRequest::InvocationApproval(_), Some(tool)) => {
            format!("Coral needs your approval to call {tool}{provider}.")
        }
        (RequiredActionRequest::InvocationApproval(_), None) => {
            format!("Coral needs your approval for a tool call{provider}.")
        }
        (RequiredActionRequest::AccountConnection(_), _) => match &action.provider_label {
            Some(label) => format!("Coral needs you to connect your {label} account."),
            None => "Coral needs you to connect an account.".to_owned(),
        },
    }
}

#[cfg(test)]
mod tests;
