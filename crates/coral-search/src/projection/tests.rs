use coral_catalog::{ToolHints, ToolPresentation};
use serde_json::{Value, json};

use crate::{
    ApprovalRequirement, CallPath, EntityKind, ProviderCallRoot, ProviderId, RetrieverCoverage,
    RetrieverState, RetrieverStatus, SearchHit, SearchPage, ToolAuthoringDescription,
    signature::SignatureLine, types::HitText,
};

#[test]
fn hints_project_only_what_the_provider_declared() {
    for (hints, expected) in [
        (ToolHints::default(), json!({})),
        (
            ToolHints::new(Some(true), None, None, None),
            json!({ "read_only": true }),
        ),
        (
            ToolHints::new(Some(false), Some(true), Some(false), Some(true)),
            json!({
                "read_only": false,
                "destructive": true,
                "idempotent": false,
                "open_world": true,
            }),
        ),
        (
            ToolHints::new(None, None, Some(true), Some(false)),
            json!({ "idempotent": true, "open_world": false }),
        ),
    ] {
        assert_eq!(
            json!(coral_catalog::DeclaredHints::from(hints)),
            expected,
            "{hints:?}"
        );
    }
}

#[test]
fn a_hit_carries_only_what_the_agent_chooses_and_calls_by() {
    let line = "list(input: { repo: string }): Promise<{ data: Array<Pull> }>";
    let text = || HitText {
        signature: SignatureLine {
            text: line.into(),
            complete: true,
            input_end: line.find("): ").expect("result") + 1,
        },
        description_snippet: "Lists pull requests.".into(),
    };
    for (name, hit, expected) in [
        (
            "a Tool with nothing declared or set",
            tool_hit(
                text(),
                ToolHints::default(),
                ApprovalRequirement::NotRequired,
                false,
            ),
            json!({ "call_path": "coral.providers.github.pulls.list" }),
        ),
        (
            "a Tool whose every flag applies: its tags and guidance stay out",
            tool_hit(
                text(),
                ToolHints::new(Some(true), None, None, Some(false)),
                ApprovalRequirement::Required,
                true,
            ),
            json!({
                "call_path": "coral.providers.github.pulls.list",
                "hints": { "read_only": true, "open_world": false },
                "approval_required": true,
                "deprecated": true,
            }),
        ),
        (
            "a Private Tool",
            SearchHit::from_authored_tool(
                CallPath::parse("coral.private.github.openPulls").expect("Call Path"),
                text(),
            ),
            json!({ "call_path": "coral.private.github.openPulls" }),
        ),
    ] {
        let mut expected = expected;
        merge(
            &mut expected,
            json!({
                "signature_line": "list(input: { repo: string }): Promise<{ data: Array<Pull> }>",
                "complete": true,
                "description_snippet": "Lists pull requests.",
            }),
        );
        assert_eq!(json!(super::hit(&hit)), expected, "{name}");
    }
}

#[test]
fn a_page_states_its_next_offset_and_retriever_statuses_only_when_they_matter() {
    for (name, page, expected) in [
        (
            "a last page that every retriever searched",
            SearchPage::new(
                Vec::new(),
                None,
                vec![
                    status(EntityKind::ProviderTool, RetrieverState::ResultsFound),
                    status(EntityKind::PrivateTool, RetrieverState::Empty),
                ],
            ),
            json!({ "hits": [] }),
        ),
        (
            "a page with more results and one retriever that failed",
            SearchPage::new(
                Vec::new(),
                Some(20),
                vec![
                    status(EntityKind::ProviderTool, RetrieverState::Partial),
                    status(EntityKind::PrivateTool, RetrieverState::ResultsFound),
                ],
            ),
            json!({
                "hits": [],
                "next_offset": 20,
                "retriever_statuses": [{
                    "retriever": "provider_tool",
                    "state": "partial",
                    "coverage": {
                        "eligible_units": 3,
                        "searched_units": 2,
                        "failed_units": 1,
                        "returned_count": 1,
                        "has_more": false,
                    },
                }],
            }),
        ),
    ] {
        assert_eq!(super::search(&page), expected, "{name}");
    }
}

fn tool_hit(
    text: HitText,
    hints: ToolHints,
    approval: ApprovalRequirement,
    deprecated: bool,
) -> SearchHit {
    let call_root = ProviderCallRoot::parse("coral.providers.github").expect("call root");
    let presentation: ToolPresentation = serde_json::from_value(json!({
        "title": "List pull requests",
        "description": "Lists pull requests.",
        "deprecated": deprecated,
        "tags": ["pulls"],
        "guidance": "Prefer search for one author.",
    }))
    .expect("presentation");
    let description = ToolAuthoringDescription::new(
        CallPath::tool(&call_root, "pulls.list").expect("Call Path"),
        "declare function list(): void;".into(),
        call_root,
        ProviderId::parse("6f1c7e52-7d1d-4a8e-9a43-3c1d1f0b5a10").expect("Provider id"),
        "GitHub".into(),
        presentation,
        hints,
        approval,
    );
    SearchHit::from_tool(&description, text)
}

fn status(retriever: EntityKind, state: RetrieverState) -> RetrieverStatus {
    RetrieverStatus::new(retriever, state, RetrieverCoverage::new(3, 2, 1, false))
}

fn merge(target: &mut Value, extra: Value) {
    let (Value::Object(target), Value::Object(extra)) = (target, extra) else {
        panic!("objects");
    };
    target.extend(extra);
}
