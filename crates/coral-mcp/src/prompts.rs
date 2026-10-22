//! Elicitation of the invoking User's current human actions.
//!
//! The Run domain decides who is prompted and what an answer does. This Adapter
//! only negotiates how to ask. An approval is a form; an account connection is a
//! URL to Reef's connect page, asked only of clients that declare URL mode. A
//! 2025-11-25 session asks in-band while the tool call waits. A 2026-07-28
//! client receives an `input_required` result and retries the same call with
//! its answers and the echoed request state. One tool call asks about each
//! action at most once, across retry rounds; a later call asks again while the
//! action is still pending. An in-band prompt is withdrawn when its action ends
//! or is resolved elsewhere; an opened in-band connection page is announced
//! complete when its request stops waiting.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use coral_run::{ActionPrompt, PromptAnswer, RequiredActionRequest, RunId};
use rmcp::model::{
    ClientResult, CustomNotification, ElicitRequest, ElicitRequestParams, ElicitResult,
    ElicitationAction, ElicitationCapability, ElicitationSchema, InputRequest, InputRequiredResult,
    InputResponses, ProtocolVersion, ServerNotification, ServerRequest,
};
use rmcp::service::{Peer, PeerRequestOptions, RequestContext, RoleServer};
use serde::{Deserialize, Serialize};

use crate::HumanInterfaceUrl;

/// The elicitation modes a client declared.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Modes {
    form: bool,
    url: bool,
}

impl Modes {
    fn declared(elicitation: Option<&ElicitationCapability>) -> Self {
        elicitation.map_or(
            Self {
                form: false,
                url: false,
            },
            // An empty elicitation capability means form mode.
            |elicitation| Self {
                form: elicitation.form.is_some() || elicitation.url.is_none(),
                url: elicitation.url.is_some(),
            },
        )
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

/// How this tool call can ask its client, fixed when the call arrives.
pub(crate) enum Channel {
    /// The client declared no elicitation.
    Unavailable,
    /// A pre-2026-07-28 client answers server requests during the call.
    InBand(Peer<RoleServer>, Modes),
    /// A 2026-07-28 client answers through a retried call.
    InputRequired(Modes),
}

impl Channel {
    pub(crate) fn negotiate(context: &RequestContext<RoleServer>) -> Self {
        let modes = Modes::declared(
            context
                .client_capabilities()
                .and_then(|capabilities| capabilities.elicitation)
                .as_ref(),
        );
        if !modes.form && !modes.url {
            Self::Unavailable
        } else if context
            .protocol_version()
            .is_some_and(|version| version.as_str() >= ProtocolVersion::V_2026_07_28.as_str())
        {
            Self::InputRequired(modes)
        } else {
            Self::InBand(context.peer.clone(), modes)
        }
    }

    const fn modes(&self) -> Option<Modes> {
        match self {
            Self::Unavailable => None,
            Self::InBand(_, modes) | Self::InputRequired(modes) => Some(*modes),
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
    channel: Channel,
    continued: Option<RunId>,
    asked: BTreeSet<String>,
    answers: Vec<(RequiredActionRequest, PromptAnswer)>,
}

/// What asking produced for the current observation.
pub(crate) enum Asked {
    /// Nothing new to ask.
    Nothing,
    /// Ask each prompt in-band, then apply the answers and observe again.
    InBand(Vec<InBandPrompt>),
    /// Return this result; the client retries with its answers.
    InputRequired(InputRequiredResult),
}

impl Prompting {
    /// Reads a retry's request state and answers.
    ///
    /// # Errors
    /// Returns a safe invalid-params message for state without a negotiated
    /// retry, malformed state, answers without state, or answers to requests
    /// this call never asked.
    pub(crate) fn from_request(
        channel: Channel,
        request_state: Option<String>,
        input_responses: Option<InputResponses>,
    ) -> Result<Self, String> {
        let Some(state) = request_state else {
            if input_responses.is_some() {
                return Err("inputResponses require requestState".into());
            }
            return Ok(Self {
                channel,
                continued: None,
                asked: BTreeSet::new(),
                answers: Vec::new(),
            });
        };
        if !matches!(channel, Channel::InputRequired(_)) {
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
            channel,
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

    pub(crate) const fn available(&self) -> bool {
        !matches!(self.channel, Channel::Unavailable)
    }

    /// Asks about every prompt this call has not asked about yet. `intent` is
    /// the agent's stated reason for the Run, when known.
    pub(crate) fn ask(
        &mut self,
        prompts: Vec<ActionPrompt>,
        run_id: &RunId,
        human_interface: &HumanInterfaceUrl,
        intent: Option<&str>,
    ) -> Result<Asked, String> {
        let Some(modes) = self.channel.modes() else {
            return Ok(Asked::Nothing);
        };
        let prompts = prompts
            .into_iter()
            .filter(|prompt| modes.asks(prompt))
            .filter(|prompt| self.asked.insert(prompt.request.to_string()))
            .collect::<Vec<_>>();
        if prompts.is_empty() {
            return Ok(Asked::Nothing);
        }
        match &self.channel {
            Channel::Unavailable => Ok(Asked::Nothing),
            Channel::InputRequired(_) => {
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
                Ok(Asked::InputRequired(InputRequiredResult::new(
                    Some(requests),
                    Some(state),
                )))
            }
            Channel::InBand(peer, _) => Ok(Asked::InBand(
                prompts
                    .into_iter()
                    .map(|prompt| InBandPrompt {
                        peer: peer.clone(),
                        params: elicitation(&prompt, human_interface, intent),
                        deadline_at_unix_nanos: prompt.deadline_at_unix_nanos,
                        request: prompt.request,
                    })
                    .collect(),
            )),
        }
    }
}

/// One prompt asked while the tool call waits for its answer.
pub(crate) struct InBandPrompt {
    peer: Peer<RoleServer>,
    request: RequiredActionRequest,
    params: ElicitRequestParams,
    deadline_at_unix_nanos: i64,
}

impl InBandPrompt {
    pub(crate) const fn request(&self) -> &RequiredActionRequest {
        &self.request
    }

    /// Waits for the answer. The client is told to close the prompt when
    /// `withdrawn` completes or the action reaches its own deadline. After the
    /// client opens a connect page, the call waits for the same end and
    /// announces the elicitation complete when `connected` confirms it.
    pub(crate) async fn ask(
        self,
        withdrawn: impl Future<Output = ()> + Unpin,
        connected: impl Future<Output = bool>,
    ) -> (RequiredActionRequest, PromptAnswer) {
        let answer = ask_in_band(
            &self.peer,
            self.params,
            self.deadline_at_unix_nanos,
            withdrawn,
            connected,
        )
        .await;
        (self.request, answer)
    }
}

async fn ask_in_band(
    peer: &Peer<RoleServer>,
    params: ElicitRequestParams,
    deadline_at_unix_nanos: i64,
    mut withdrawn: impl Future<Output = ()> + Unpin,
    connected: impl Future<Output = bool>,
) -> PromptAnswer {
    let Some(remaining) = remaining(deadline_at_unix_nanos) else {
        return PromptAnswer::Dismiss;
    };
    let completes = match &params {
        ElicitRequestParams::UrlElicitationParams { elicitation_id, .. } => {
            Some(elicitation_id.clone())
        }
        _ => None,
    };
    let Ok(mut handle) = peer
        .send_request_with_option(
            ServerRequest::ElicitRequest(ElicitRequest::new(params)),
            PeerRequestOptions::no_options(),
        )
        .await
    else {
        return PromptAnswer::Dismiss;
    };
    let reason = tokio::select! {
        response = &mut handle.rx => {
            let answer = match response {
                Ok(Ok(ClientResult::ElicitResult(result))) => answer(&result),
                _ => PromptAnswer::Dismiss,
            };
            if let (PromptAnswer::Accept, Some(elicitation_id)) = (answer, completes) {
                // The client opened the connect page. Hold the call until the
                // request stops waiting; only a connection completes the page.
                tokio::select! {
                    () = &mut withdrawn => {}
                    () = tokio::time::sleep(
                        self::remaining(deadline_at_unix_nanos).unwrap_or_default()
                    ) => {}
                }
                if !connected.await {
                    return answer;
                }
                // Best effort: a client that missed it still sees the next Run Update.
                let _sent = peer
                    .send_notification(ServerNotification::CustomNotification(
                        CustomNotification::new(
                            "notifications/elicitation/complete",
                            Some(serde_json::json!({ "elicitationId": elicitation_id })),
                        ),
                    ))
                    .await;
            }
            return answer;
        }
        () = &mut withdrawn => "The action no longer needs an answer.",
        // The prompt's own countdown ends at the deadline; what the action
        // became is its owner's record (the Run's for an approval, the
        // Account's for a connection), not the prompt's judgement.
        () = tokio::time::sleep(remaining) => "The action's deadline passed.",
    };
    // Best effort: an unreachable client keeps a stale prompt that decides nothing.
    let _cancelled = handle.cancel(Some(reason.to_owned())).await;
    PromptAnswer::Dismiss
}

const fn answer(result: &ElicitResult) -> PromptAnswer {
    match result.action {
        ElicitationAction::Accept => PromptAnswer::Accept,
        ElicitationAction::Decline => PromptAnswer::Decline,
        // Cancel, and any future action, decides nothing.
        _ => PromptAnswer::Dismiss,
    }
}

fn remaining(deadline_at_unix_nanos: i64) -> Option<Duration> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_nanos();
    let remaining = i128::from(deadline_at_unix_nanos) - i128::try_from(now).ok()?;
    u64::try_from(remaining)
        .ok()
        .filter(|nanos| *nanos > 0)
        .map(Duration::from_nanos)
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
            || "Approve this operation?".to_owned(),
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
        lines.push(format!("Operation: {}", prompt.call_path));
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
        Modes::declared(Some(&capability))
    }

    #[test]
    fn messages_lead_with_the_question_and_name_no_buttons() {
        let approval = prompt(APPROVAL, "coral.providers.linear.listIssues");
        let details = "Operation: coral.providers.linear.listIssues\n\
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
            format!("Approve this operation?\n\n{details}")
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
             Operation: coral.providers.linear.listIssues\n\
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
        assert_eq!(
            Modes::declared(None),
            Modes {
                form: false,
                url: false
            }
        );
    }

    #[test]
    fn a_retry_asks_only_the_actions_its_client_can_answer() {
        let run = RunId::parse("01234567-89ab-4cde-8fab-0123456789ab").unwrap();
        let mut prompting = Prompting::from_request(
            Channel::InputRequired(modes(json!({"url": {}}))),
            None,
            None,
        )
        .unwrap();
        let Asked::InputRequired(result) = prompting
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
        else {
            panic!("input required")
        };
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
        for channel in [
            Channel::Unavailable,
            Channel::InputRequired(modes(json!({}))),
        ] {
            let negotiated = matches!(channel, Channel::InputRequired(_));
            assert_eq!(
                Prompting::from_request(channel, Some(state.to_string()), None).is_ok(),
                negotiated
            );
        }
    }
}
