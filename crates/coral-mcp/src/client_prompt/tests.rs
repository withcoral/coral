use base64::Engine as _;
use coral_run::{RequiredAction, RequiredActionRequest, RequiredActor, RunCursor, RunId};
use rmcp::model::{ClientCapabilities, InputResponses};
use serde_json::json;

use super::{Ask, Mode, Prompted, Shown, WaitCursor, prompted_in_state};
use crate::Elicitation;

/// A Run cursor as `coral-run` issues one: format 4 and 56 bytes of position.
fn run_cursor() -> RunCursor {
    let mut bytes = vec![4];
    bytes.resize(57, 0);
    RunCursor::parse(&super::ENCODING.encode(bytes)).expect("Run cursor")
}

fn approval(id: &str) -> RequiredAction {
    RequiredAction {
        request: RequiredActionRequest::InvocationApproval(
            coral_run::approval::ApprovalId::parse(id).expect("approval identity"),
        ),
        required_actor: RequiredActor::InvokingUser,
        deadline_at_unix_nanos: 0,
        action_path: "/workspaces/example/approvals/example".into(),
        call_path: None,
        provider_label: None,
    }
}

const FIRST: &str = "12345678-1234-4123-8123-123456789abc";
const SECOND: &str = "87654321-4321-4321-8321-cba987654321";

fn connection(id: &str) -> RequiredAction {
    RequiredAction {
        request: RequiredActionRequest::parse(&format!("connection/{id}")).expect("connection"),
        ..approval(id)
    }
}

// Catches: a prompt the client did not declare, which rmcp refuses before
// 2026-07-28 and a client cannot show; a form when the operator chose links
// or none; or a form for a connection, which happens at the Provider's
// sign-in.
#[test]
fn each_request_is_asked_only_as_the_client_declares_and_the_setting_allows() {
    let declared = |capabilities: serde_json::Value| {
        serde_json::from_value::<ClientCapabilities>(capabilities).expect("capabilities")
    };
    let both = declared(json!({"elicitation": {"form": {}, "url": {}}}));
    let url = declared(json!({"elicitation": {"url": {}}}));
    let form = declared(json!({"elicitation": {"form": {}}}));
    let none = declared(json!({}));
    let (approval, connection) = (approval(FIRST), connection(FIRST));
    for (setting, capabilities, expected) in [
        (
            Elicitation::Form,
            Some(&both),
            (Some(Mode::Form), Some(Mode::Link)),
        ),
        (Elicitation::Form, Some(&form), (Some(Mode::Form), None)),
        (
            Elicitation::Form,
            Some(&url),
            (Some(Mode::Link), Some(Mode::Link)),
        ),
        (
            Elicitation::Url,
            Some(&both),
            (Some(Mode::Link), Some(Mode::Link)),
        ),
        (Elicitation::Url, Some(&form), (None, None)),
        (Elicitation::Off, Some(&both), (None, None)),
        (Elicitation::Form, Some(&none), (None, None)),
        // A request before 2026-07-28 declares nothing of its own.
        (Elicitation::Form, None, (None, None)),
    ] {
        let ask = Ask::of(setting, capabilities);
        let modes = (
            ask.and_then(|ask| ask.mode(&approval)),
            ask.and_then(|ask| ask.mode(&connection)),
        );
        assert_eq!(modes, expected, "{setting:?} {capabilities:?}");
    }
}

/// The request state that `prompt` returns for `action` asked as `mode`, as
/// the client sends it back.
fn prompted(action: &RequiredAction, mode: Mode) -> Prompted {
    let run_id = RunId::parse("play-length-height-type").expect("Run id");
    let human_interface =
        crate::HumanInterfaceUrl::parse("https://coral.example").expect("human interface");
    let prompt = super::prompt(
        &run_id,
        action,
        mode,
        Some("{}"),
        &Shown::default(),
        &human_interface,
    )
    .expect("prompt");
    let state = prompt.request_state.expect("request state");
    prompted_in_state(&state, &run_id).expect("Coral's own state")
}

// Catches: a link prompt's accept approving a tool call, as when an approval
// whose arguments are too long for a form is asked as a link under the
// `form` setting; an answer to a request this prompt did not ask deciding
// it; or a dismissed form declining.
#[test]
fn only_an_answer_to_the_form_this_prompt_asked_decides() {
    let declared = serde_json::from_value::<ClientCapabilities>(
        json!({"elicitation": {"form": {}, "url": {}}}),
    )
    .expect("capabilities");
    let form = Ask::of(Elicitation::Form, Some(&declared)).expect("form");
    let links = Ask::of(Elicitation::Url, Some(&declared)).expect("links");
    let answers = |action: &str| -> InputResponses {
        [
            (format!("approval/{FIRST}"), json!({"action": action})),
            (format!("approval/{SECOND}"), json!({"action": action})),
            (format!("connection/{SECOND}"), json!({"action": action})),
        ]
        .into_iter()
        .collect()
    };
    let first = coral_run::approval::ApprovalId::parse(FIRST).expect("approval identity");
    let asked_as_form = prompted(&approval(FIRST), Mode::Form);
    let asked_as_link = prompted(&approval(FIRST), Mode::Link);
    for (ask, state, action, expected) in [
        (form, &asked_as_form, "accept", Some("Approve")),
        (form, &asked_as_form, "decline", Some("Decline")),
        (form, &asked_as_form, "cancel", None),
        (form, &asked_as_link, "accept", None),
        (form, &asked_as_link, "decline", None),
        (links, &asked_as_form, "accept", None),
        (form, &Prompted::default(), "accept", None),
    ] {
        let decided = ask
            .decision(&answers(action), state)
            .map(|(id, decision)| (id, format!("{decision:?}")));
        let expected = expected.map(|decision| (first.clone(), decision.to_owned()));
        assert_eq!(decided, expected, "{ask:?} {state:?} {action}");
    }
}

// Catches: a cursor that loses which Required Actions were asked, so every
// `wait` asks again, or one that keeps requests the Run no longer waits on and
// grows without bound.
#[test]
fn the_cursor_keeps_what_was_asked_while_the_run_still_waits_on_it() {
    let run = run_cursor();
    let first = approval(FIRST);
    let second = approval(SECOND);
    let shown = Shown::default().with(&first).with(&second);

    let plain = WaitCursor::format(&run, &Shown::default(), std::slice::from_ref(&first));
    assert_eq!(
        plain,
        run.to_string(),
        "nothing asked: the Run's own cursor"
    );

    let carried = WaitCursor::format(&run, &shown, std::slice::from_ref(&first));
    let parsed = WaitCursor::parse(&carried).expect("own cursor");
    assert_eq!(parsed.run, run);
    assert_eq!(
        parsed.shown,
        Shown::default().with(&first),
        "only what the Run still waits on"
    );

    let parsed = WaitCursor::parse(&run.to_string()).expect("a Run cursor");
    assert_eq!(parsed.shown, Shown::default());
    assert_eq!(
        WaitCursor::format(&run, &shown, &[]),
        run.to_string(),
        "a Run that waits on nothing carries nothing"
    );
}

#[test]
fn a_cursor_with_an_unreadable_tail_is_invalid() {
    let run = run_cursor();
    for value in [
        format!("{run}.%%%"),
        format!("{run}.{}", super::ENCODING.encode([0xff])),
    ] {
        let Err(coral_run::Error::InvalidInput(message)) = WaitCursor::parse(&value) else {
            panic!("{value}: refused as invalid input")
        };
        assert_eq!(message, "Run cursor is invalid");
    }
}

// Catches: a request state from another Run, or one Coral never issued,
// suppressing that Run's prompt or failing the call.
#[test]
fn the_request_state_counts_only_for_its_own_run() {
    let run_id = RunId::parse("play-length-height-type").expect("Run id");
    let other = RunId::parse("view-future-star-market").expect("Run id");
    let state = super::ENCODING.encode(
        serde_json::to_vec(&super::RequestState {
            run: run_id.to_string(),
            shown: Shown::default().with(&approval(FIRST)).encode(),
            form: None,
        })
        .expect("state"),
    );
    assert_eq!(
        prompted_in_state(&state, &run_id).map(|prompted| prompted.shown),
        Some(Shown::default().with(&approval(FIRST)))
    );
    assert_eq!(prompted_in_state(&state, &other), None);
    assert_eq!(prompted_in_state("never-issued", &run_id), None);
}
