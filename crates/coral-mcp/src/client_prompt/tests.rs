use base64::Engine as _;
use coral_run::{RequiredAction, RequiredActionRequest, RequiredActor, RunCursor, RunId};
use rmcp::model::ClientCapabilities;
use serde_json::json;

use super::{Ask, Shown, WaitCursor, shown_in_state};
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

// Catches: a prompt sent to a client that did not declare the mode, which
// rmcp refuses before 2026-07-28 and a client cannot show; or one sent when
// the operator turned prompts off.
#[test]
fn a_link_prompt_needs_the_declared_url_mode_and_a_setting_that_allows_it() {
    let declared = |capabilities: serde_json::Value| {
        serde_json::from_value::<ClientCapabilities>(capabilities).expect("capabilities")
    };
    let both = declared(json!({"elicitation": {"form": {}, "url": {}}}));
    let url = declared(json!({"elicitation": {"url": {}}}));
    let form = declared(json!({"elicitation": {"form": {}}}));
    let none = declared(json!({}));
    for (setting, capabilities, expected) in [
        (Elicitation::Form, Some(&both), Some(Ask::Link)),
        (Elicitation::Url, Some(&both), Some(Ask::Link)),
        (Elicitation::Form, Some(&url), Some(Ask::Link)),
        (Elicitation::Off, Some(&both), None),
        (Elicitation::Form, Some(&form), None),
        (Elicitation::Form, Some(&none), None),
        // A request before 2026-07-28 declares nothing of its own.
        (Elicitation::Form, None, None),
    ] {
        assert_eq!(
            Ask::of(setting, capabilities),
            expected,
            "{setting:?} {capabilities:?}"
        );
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
        })
        .expect("state"),
    );
    assert_eq!(
        shown_in_state(&state, &run_id),
        Some(Shown::default().with(&approval(FIRST)))
    );
    assert_eq!(shown_in_state(&state, &other), None);
    assert_eq!(shown_in_state("never-issued", &run_id), None);
}
