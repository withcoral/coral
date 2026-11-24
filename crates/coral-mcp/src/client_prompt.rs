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
//! authorized as before. The request state also names the approval its prompt
//! asked as a form: only an answer for that one decides, so a link prompt never
//! does.

use std::collections::{BTreeMap, BTreeSet};

use base64::Engine as _;
use coral_run::approval::{ApprovalDecision, ApprovalId};
use coral_run::{
    RequiredAction, RequiredActionRequest, RequiredActor, RunCursor, RunId, RunUpdate,
};
use rmcp::model::{
    ClientCapabilities, ElicitRequest, ElicitRequestParams, ElicitationSchema, InputRequest,
    InputRequests, InputRequiredResult, InputResponses,
};
use serde::{Deserialize, Serialize};

use crate::Elicitation;

const ENCODING: base64::engine::GeneralPurpose = base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// Separates the Run's own cursor from the Required Actions already asked. A
/// Run cursor is URL-safe base64, which never holds it.
const CURSOR_SEPARATOR: char = '.';

/// The longest argument text a form shows. An approval whose arguments are
/// longer is asked as a link, so the person reviews them on its page.
const FORM_ARGUMENTS_CHARS: usize = 2_000;

/// The comment a decision made in a form records, so the Approvals Page
/// shows where it was made.
pub(crate) const FORM_COMMENT: &str = "Decided in the AI app's prompt.";

/// The Client Prompts one request may be shown: what its client declares, as
/// far as the deployment allows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Ask {
    /// An Approval Request may be asked as a form that decides it.
    form: bool,
    /// A Required Action may be asked as a link to its page.
    link: bool,
}

/// How one Required Action is asked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    /// A form whose Approve or Decline decides an Approval Request.
    Form,
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
        let ask = match elicitation {
            Elicitation::Off => return None,
            Elicitation::Form => Self {
                form: declared.form.is_some(),
                link: declared.url.is_some(),
            },
            Elicitation::Url => Self {
                form: false,
                link: declared.url.is_some(),
            },
        };
        (ask.form || ask.link).then_some(ask)
    }

    /// How `action` is asked, if at all. A connection happens at the
    /// Provider's sign-in, so only a link asks for one.
    const fn mode(self, action: &RequiredAction) -> Option<Mode> {
        match action.request {
            RequiredActionRequest::InvocationApproval(_) if self.form => Some(Mode::Form),
            _ if self.link => Some(Mode::Link),
            _ => None,
        }
    }

    /// `mode`, or a link when a form cannot show what it would decide.
    pub(crate) const fn instead_of_form(self) -> Option<Mode> {
        if self.link { Some(Mode::Link) } else { None }
    }

    /// The decision that `responses` make: an Approve or a Decline of the
    /// one Approval Request that `prompted` asked as a form. A link prompt
    /// decides nothing, even for an approval, and neither does a cancelled
    /// form or an answer for a request this prompt did not ask.
    pub(crate) fn decision(
        self,
        responses: &InputResponses,
        prompted: &Prompted,
    ) -> Option<(ApprovalId, ApprovalDecision)> {
        if !self.form {
            return None;
        }
        let id = prompted.form.as_ref()?;
        let decision = match responses.get(&Shown::key(id))?.get("action")?.as_str()? {
            "accept" => ApprovalDecision::Approve,
            "decline" => ApprovalDecision::Decline,
            _ => return None,
        };
        Some((id.clone(), decision))
    }
}

/// The Required Actions of one Run that a Client Prompt already asked about.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Shown(BTreeSet<String>);

impl Shown {
    fn of(action: &RequiredAction) -> String {
        action.request.to_string()
    }

    /// The key an approval is asked and answered under.
    fn key(approval: &ApprovalId) -> String {
        RequiredActionRequest::InvocationApproval(approval.clone()).to_string()
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

/// What a repeated call carries back: the Run it observed, the Required
/// Actions it asked about, and the approval it asked as a form, if any.
#[derive(Serialize, Deserialize)]
struct RequestState {
    run: String,
    shown: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    form: Option<String>,
}

/// What the prompt of the call a client repeats asked.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Prompted {
    /// The Required Actions asked about so far.
    pub(crate) shown: Shown,
    /// The Approval Request asked as a form: the one an answer decides.
    form: Option<ApprovalId>,
}

/// What `state`, from the call a client repeats, asked in `run_id`. None for
/// a state that is not Coral's, or names another Run: the call then observes
/// as if it carried none.
pub(crate) fn prompted_in_state(state: &str, run_id: &RunId) -> Option<Prompted> {
    let bytes = ENCODING.decode(state).ok()?;
    let state = serde_json::from_slice::<RequestState>(&bytes).ok()?;
    if state.run != run_id.to_string() {
        return None;
    }
    let form = match state.form {
        Some(form) => Some(ApprovalId::parse(&form).ok()?),
        None => None,
    };
    Some(Prompted {
        shown: Shown::decode(&state.shown)?,
        form,
    })
}

/// The first Required Action in `update` that names the invoking User, that
/// no Client Prompt asked about yet, and that `ask` can ask, with how. Only
/// the invoking User observes a Run whose request names them, so the prompt
/// reaches that User.
pub(crate) fn next<'a>(
    update: &'a RunUpdate,
    shown: &Shown,
    ask: Ask,
) -> Option<(&'a RequiredAction, Mode)> {
    update.required_actions().iter().find_map(|action| {
        (action.required_actor == RequiredActor::InvokingUser && !shown.contains(action))
            .then(|| ask.mode(action))
            .flatten()
            .map(|mode| (action, mode))
    })
}

/// The approval's `arguments` as a form shows them, or None when they are too
/// long to show whole.
pub(crate) fn form_arguments(arguments: &serde_json::Value) -> Option<String> {
    let text = serde_json::to_string_pretty(arguments).ok()?;
    (text.chars().count() <= FORM_ARGUMENTS_CHARS).then_some(text)
}

/// Asks about `action` of `run_id` as `mode`, and counts it as asked. A form
/// shows `arguments`, the exact arguments it decides on.
pub(crate) fn prompt(
    run_id: &RunId,
    action: &RequiredAction,
    mode: Mode,
    arguments: Option<&str>,
    shown: &Shown,
    human_interface: &crate::HumanInterfaceUrl,
) -> Option<InputRequiredResult> {
    let url = human_interface.page_url(&action.action_path);
    let request = match (mode, arguments) {
        (Mode::Form, Some(arguments)) => {
            ElicitRequest::new(ElicitRequestParams::FormElicitationParams {
                meta: None,
                message: format!(
                    "{}\n\nArguments:\n{arguments}\n\nApprove or decline it here, or review it at {url}",
                    asked(action)
                ),
                // Nothing to fill in: accept approves, decline declines.
                requested_schema: ElicitationSchema::new(BTreeMap::new()),
            })
        }
        // A form that cannot show its arguments decides nothing blind.
        (Mode::Form, None) => return None,
        (Mode::Link, _) => ElicitRequest::new(ElicitRequestParams::UrlElicitationParams {
            meta: None,
            // The client shows the link too, but one that opens it on another
            // host, as over SSH, leaves the person this text to copy.
            message: format!("{} Open {url}", asked(action)),
            url: url.clone(),
            elicitation_id: Shown::of(action),
        }),
    };
    let form = match (mode, &action.request) {
        (Mode::Form, RequiredActionRequest::InvocationApproval(id)) => Some(id.to_string()),
        _ => None,
    };
    let state = RequestState {
        run: run_id.to_string(),
        shown: shown.clone().with(action).encode(),
        form,
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
