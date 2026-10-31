//! Elicitation of the invoking User's current human actions.
//!
//! The Run domain decides who is prompted and what an answer does. This Adapter
//! only negotiates how to ask. An approval is a form; an account connection is a
//! URL to Reef's connect page, asked only of clients that declare URL mode. A
//! 2026-07-28 client receives an `input_required` result and retries the same
//! call with its answers and the echoed request state. Coral keeps no MCP
//! session, so a pre-2026-07-28 client is never asked in-band: its Run Update
//! names the required actions and the Run Status Page, where the User acts. One
//! tool call asks about each action at most once, across retry rounds; a later
//! call asks again while the action is still pending.

use std::collections::{BTreeMap, BTreeSet};

use coral_run::{ActionPrompt, PromptAnswer, RequiredActionRequest, RunId};
use rmcp::model::{
    ElicitRequest, ElicitRequestParams, ElicitResult, ElicitationAction, ElicitationCapability,
    ElicitationSchema, InputRequest, InputRequiredResult, InputResponses, ProtocolVersion,
};
use rmcp::service::{RequestContext, RoleServer};
use serde::{Deserialize, Serialize};

use crate::HumanInterfaceUrl;

/// The elicitation modes a client declared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Modes {
    form: bool,
    url: bool,
}

impl Modes {
    /// The modes a tool call can ask its client in, fixed when the call
    /// arrives. Before 2026-07-28 the only way to ask is a server-initiated
    /// request, which needs a session Coral does not keep, so those requests
    /// are never asked.
    pub(crate) fn negotiated(context: &RequestContext<RoleServer>) -> Option<Self> {
        if context
            .protocol_version()
            .is_none_or(|version| version < ProtocolVersion::V_2026_07_28)
        {
            return None;
        }
        Self::declared(
            context
                .client_capabilities()
                .and_then(|capabilities| capabilities.elicitation)
                .as_ref(),
        )
    }

    /// `None` when the client declared no elicitation.
    fn declared(elicitation: Option<&ElicitationCapability>) -> Option<Self> {
        // An empty elicitation capability means form mode.
        let modes = Self {
            form: elicitation?.form.is_some() || elicitation?.url.is_none(),
            url: elicitation?.url.is_some(),
        };
        (modes.form || modes.url).then_some(modes)
    }

    /// Approvals need a form. A connection needs a URL and a page to open.
    fn asks(self, prompt: &ActionPrompt) -> bool {
        match prompt.request {
            RequiredActionRequest::InvocationApproval(_) => self.form,
            RequiredActionRequest::AccountConnection(_) => {
                self.url && prompt.connect_path.is_some()
            }
        }
    }
}

/// Echoed `requestState`. It is untrusted client input and grants no authority:
/// the Run domain authorizes every observation and answer for the authenticated
/// Principal. It only names the Run this call observes and what it has asked.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestState {
    run_id: String,
    asked: BTreeSet<String>,
}

/// One tool call's prompting state, including answers carried by a retry.
pub(crate) struct Prompting {
    modes: Option<Modes>,
    continued: Option<RunId>,
    asked: BTreeSet<String>,
    answers: Vec<(RequiredActionRequest, PromptAnswer)>,
}

impl Prompting {
    /// Reads a retry's request state and answers.
    ///
    /// # Errors
    /// Returns a safe invalid-params message for state without a negotiated
    /// retry, malformed state, answers without state, or answers to requests
    /// this call never asked.
    pub(crate) fn from_request(
        modes: Option<Modes>,
        request_state: Option<String>,
        input_responses: Option<InputResponses>,
    ) -> Result<Self, String> {
        let Some(state) = request_state else {
            if input_responses.is_some() {
                return Err("inputResponses require requestState".into());
            }
            return Ok(Self {
                modes,
                continued: None,
                asked: BTreeSet::new(),
                answers: Vec::new(),
            });
        };
        if modes.is_none() {
            return Err("requestState requires a negotiated input_required retry".into());
        }
        let invalid_state = || "invalid requestState".to_owned();
        let state: RequestState = serde_json::from_str(&state).map_err(|_error| invalid_state())?;
        let continued = RunId::parse(&state.run_id).map_err(|_error| invalid_state())?;
        let mut answers = Vec::new();
        for (key, response) in input_responses.unwrap_or_default() {
            if !state.asked.contains(&key) {
                return Err("inputResponses answer a request that was not asked".into());
            }
            let request = RequiredActionRequest::parse(&key).map_err(|_error| invalid_state())?;
            let result: ElicitResult = serde_json::from_value(response)
                .map_err(|_error| "invalid elicitation response".to_owned())?;
            answers.push((request, answer(&result)));
        }
        Ok(Self {
            modes,
            continued: Some(continued),
            asked: state.asked,
            answers,
        })
    }

    /// The Run a retried call continues observing instead of starting work.
    pub(crate) const fn continued_run(&self) -> Option<&RunId> {
        self.continued.as_ref()
    }

    pub(crate) fn take_answers(&mut self) -> Vec<(RequiredActionRequest, PromptAnswer)> {
        std::mem::take(&mut self.answers)
    }

    /// Whether this client can be asked anything at all.
    pub(crate) const fn can_ask(&self) -> bool {
        self.modes.is_some()
    }

    /// Asks about every prompt this call has not asked about yet, as the
    /// `input_required` result to return, or `None` when there is nothing new
    /// to ask. `intent` is the agent's stated reason for the Run, when known.
    pub(crate) fn ask(
        &mut self,
        prompts: Vec<ActionPrompt>,
        run_id: &RunId,
        human_interface: &HumanInterfaceUrl,
        intent: Option<&str>,
    ) -> Result<Option<InputRequiredResult>, String> {
        let Some(modes) = self.modes else {
            return Ok(None);
        };
        let prompts = prompts
            .into_iter()
            .filter(|prompt| modes.asks(prompt))
            .filter(|prompt| self.asked.insert(prompt.request.to_string()))
            .collect::<Vec<_>>();
        if prompts.is_empty() {
            return Ok(None);
        }
        let requests = prompts
            .iter()
            .map(|prompt| {
                (
                    prompt.request.to_string(),
                    InputRequest::Elicitation(ElicitRequest::new(elicitation(
                        prompt,
                        human_interface,
                        intent,
                    ))),
                )
            })
            .collect();
        let state = serde_json::to_string(&RequestState {
            run_id: run_id.to_string(),
            asked: self.asked.clone(),
        })
        .map_err(|_error| "request state could not be encoded".to_owned())?;
        Ok(Some(InputRequiredResult::new(Some(requests), Some(state))))
    }
}

const fn answer(result: &ElicitResult) -> PromptAnswer {
    match result.action {
        ElicitationAction::Accept => PromptAnswer::Accept,
        ElicitationAction::Decline => PromptAnswer::Decline,
        // Cancel, and any future action, decides nothing.
        _ => PromptAnswer::Dismiss,
    }
}

// The form asks only for a decision. Harnesses label its buttons differently
// and some show only the first lines, so the message names no buttons and
// leads with the question. Arguments stay on the Run Status Page. A connection
// opens Reef's connect page, which is itself the link.
fn elicitation(
    prompt: &ActionPrompt,
    human_interface: &HumanInterfaceUrl,
    intent: Option<&str>,
) -> ElicitRequestParams {
    let heading = if matches!(prompt.request, RequiredActionRequest::InvocationApproval(_)) {
        // The agent's stated reason, asked as the question being approved.
        intent.map_or_else(
            || "Approve this tool call?".to_owned(),
            |intent| format!("{}?", intent.trim_end_matches(['.', '!', '?'])),
        )
    } else {
        let account = prompt.provider_label.as_deref().map_or_else(
            || "an account".to_owned(),
            |label| format!("your {label} account"),
        );
        format!("Connect {account} to continue.")
    };
    let mut lines = vec![heading, String::new()];
    if !prompt.call_path.is_empty() {
        lines.push(format!("Tool: {}", prompt.call_path));
    }
    let expires =
        chrono::DateTime::from_timestamp_nanos(prompt.deadline_at_unix_nanos).format("%H:%M UTC");
    if let (RequiredActionRequest::AccountConnection(_), Some(connect_path)) =
        (&prompt.request, &prompt.connect_path)
    {
        lines.push(format!("Expires: {expires}"));
        return ElicitRequestParams::UrlElicitationParams {
            meta: None,
            message: lines.join("\n"),
            url: human_interface.page_url(connect_path),
            // Unique per elicitation: a later call can ask about the same request again.
            elicitation_id: format!("{}#{}", prompt.request, uuid::Uuid::new_v4()),
        };
    }
    lines.push(format!(
        "Expires: {expires} · {}",
        human_interface.page_url(&prompt.run_status_path),
    ));
    ElicitRequestParams::FormElicitationParams {
        meta: None,
        message: lines.join("\n"),
        requested_schema: ElicitationSchema::new(BTreeMap::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    const CONNECTION: &str = "connection/12345678-1234-4123-8123-123456789abc";
    const APPROVAL: &str = "approval/12345678-1234-4123-8123-123456789abc";

    fn human() -> HumanInterfaceUrl {
        HumanInterfaceUrl::parse("https://reef.example.test").unwrap()
    }

    fn message(prompt: &ActionPrompt, intent: Option<&str>) -> String {
        let ElicitRequestParams::FormElicitationParams { message, .. } =
            elicitation(prompt, &human(), intent)
        else {
            panic!("form elicitation")
        };
        message
    }

    fn prompt(request: &str, call_path: &str) -> ActionPrompt {
        let request = RequiredActionRequest::parse(request).unwrap();
        ActionPrompt {
            connect_path: matches!(request, RequiredActionRequest::AccountConnection(_))
                .then(|| "/workspaces/w/accounts/p".into()),
            request,
            run_status_path: "/workspaces/w/runs/r".into(),
            // 2026-01-01T18:21:00Z
            deadline_at_unix_nanos: 1_767_291_660_000_000_000,
            call_path: call_path.into(),
            provider_label: Some("Linear".into()),
        }
    }

    fn modes(capability: Value) -> Modes {
        let capability: ElicitationCapability = serde_json::from_value(capability).unwrap();
        Modes::declared(Some(&capability)).expect("a declared mode")
    }

    #[test]
    fn messages_lead_with_the_question_and_name_no_buttons() {
        let approval = prompt(APPROVAL, "coral.providers.linear.listIssues");
        let details = "Tool: coral.providers.linear.listIssues\n\
                       Expires: 18:21 UTC · https://reef.example.test/workspaces/w/runs/r";
        assert_eq!(
            message(&approval, Some("List Linear issues assigned to the user")),
            format!("List Linear issues assigned to the user?\n\n{details}")
        );
        assert_eq!(
            message(&approval, Some("Check my issues.")),
            format!("Check my issues?\n\n{details}"),
            "the question never doubles its punctuation"
        );
        assert_eq!(
            message(&approval, None),
            format!("Approve this tool call?\n\n{details}")
        );
    }

    #[test]
    fn a_connection_prompt_opens_the_connect_page() {
        let connection = prompt(CONNECTION, "coral.providers.linear.listIssues");
        let ask = || elicitation(&connection, &human(), Some("List Linear issues"));
        let ElicitRequestParams::UrlElicitationParams {
            message,
            url,
            elicitation_id,
            ..
        } = ask()
        else {
            panic!("URL elicitation")
        };
        assert_eq!(
            message,
            "Connect your Linear account to continue.\n\n\
             Tool: coral.providers.linear.listIssues\n\
             Expires: 18:21 UTC"
        );
        assert_eq!(url, "https://reef.example.test/workspaces/w/accounts/p");
        assert!(elicitation_id.starts_with(&format!("{CONNECTION}#")));
        let ElicitRequestParams::UrlElicitationParams {
            elicitation_id: again,
            ..
        } = ask()
        else {
            panic!("URL elicitation")
        };
        assert_ne!(again, elicitation_id, "each elicitation has its own id");
    }

    #[test]
    fn declared_modes_decide_which_actions_are_asked() {
        let approval = prompt(APPROVAL, "coral.providers.linear.listIssues");
        let connection = prompt(CONNECTION, "coral.providers.linear.listIssues");
        let removed = ActionPrompt {
            connect_path: None,
            ..connection.clone()
        };
        for (capability, asks_approval, asks_connection) in [
            (json!({}), true, false),
            (json!({"form": {}}), true, false),
            (json!({"url": {}}), false, true),
            (json!({"form": {}, "url": {}}), true, true),
        ] {
            let modes = modes(capability.clone());
            assert_eq!(modes.asks(&approval), asks_approval, "{capability}");
            assert_eq!(modes.asks(&connection), asks_connection, "{capability}");
            assert!(!modes.asks(&removed), "no page to open: {capability}");
        }
        assert_eq!(Modes::declared(None), None, "no capability, no prompt");
    }

    #[test]
    fn a_call_asks_only_the_actions_its_client_can_answer() {
        let run = RunId::parse("01234567-89ab-4cde-8fab-0123456789ab").unwrap();
        let mut prompting =
            Prompting::from_request(Some(modes(json!({"url": {}}))), None, None).unwrap();
        let result = prompting
            .ask(
                vec![
                    prompt(APPROVAL, "coral.providers.linear.listIssues"),
                    prompt(CONNECTION, "coral.providers.linear.listIssues"),
                ],
                &run,
                &human(),
                None,
            )
            .unwrap()
            .expect("input required");
        let asked = serde_json::to_value(&result).unwrap();
        let requests = asked
            .get("inputRequests")
            .and_then(Value::as_object)
            .expect("input requests");
        assert_eq!(
            requests.keys().collect::<Vec<_>>(),
            [CONNECTION],
            "a URL-only client is never asked to approve: {asked}"
        );
        assert_eq!(
            requests
                .get(CONNECTION)
                .and_then(|request| request.pointer("/params/mode")),
            Some(&json!("url")),
            "{asked}"
        );
    }

    #[test]
    fn request_state_needs_a_negotiated_retry() {
        let state = json!({"run_id": "01234567-89ab-4cde-8fab-0123456789ab", "asked": []});
        for modes in [None, Some(modes(json!({})))] {
            assert_eq!(
                Prompting::from_request(modes, Some(state.to_string()), None).is_ok(),
                modes.is_some()
            );
        }
    }
}
