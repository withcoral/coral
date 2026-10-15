use coral_catalog::{OperationHints, OperationPresentation};
use serde_json::{Value, json};

use crate::{
    ApprovalRequirement, CallPath, EntityKind, OperationAuthoringDescription, ProviderCallRoot,
    ProviderId, RetrieverCoverage, RetrieverState, RetrieverStatus, SearchCursor, SearchEvidence,
    SearchHit, SearchPage, types::HitText,
};

#[test]
fn hints_project_only_what_the_provider_declared() {
    for (hints, expected) in [
        (OperationHints::default(), json!({})),
        (
            OperationHints::new(Some(true), None, None, None),
            json!({ "read_only": true }),
        ),
        (
            OperationHints::new(Some(false), Some(true), Some(false), Some(true)),
            json!({
                "read_only": false,
                "destructive": true,
                "idempotent": false,
                "open_world": true,
            }),
        ),
        (
            OperationHints::new(None, None, Some(true), Some(false)),
            json!({ "idempotent": true, "open_world": false }),
        ),
    ] {
        assert_eq!(super::hints(hints), expected, "{hints:?}");
    }
}

#[test]
fn a_hit_carries_only_what_the_agent_chooses_and_calls_by() {
    let text = |guidance: Option<&str>, tags: &[&str]| HitText {
        signature_line: "list(input: { repo: string }): Promise<{ data: Array<Pull> }>".into(),
        description_snippet: "Lists pull requests.".into(),
        guidance_snippet: guidance.map(Into::into),
        matched_tags: tags.iter().map(|&tag| tag.into()).collect(),
    };
    for (name, hit, expected) in [
        (
            "an Operation listed without a query, with nothing declared or set",
            operation_hit(
                text(None, &[]),
                SearchEvidence::default(),
                OperationHints::default(),
                ApprovalRequirement::NotRequired,
                false,
            ),
            json!({ "call_path": "coral.providers.github.pulls.list" }),
        ),
        (
            "an Operation whose every field and flag applies",
            operation_hit(
                text(Some("Prefer search for one author."), &["pulls"]),
                SearchEvidence::new(
                    vec!["title".into(), "call_path".into(), "tags".into()],
                    vec!["result.data[].head".into(), "input.repo".into()],
                ),
                OperationHints::new(Some(true), None, None, Some(false)),
                ApprovalRequirement::Required,
                true,
            ),
            json!({
                "call_path": "coral.providers.github.pulls.list",
                "guidance_snippet": "Prefer search for one author.",
                "matched_tags": ["pulls"],
                "hints": { "read_only": true, "open_world": false },
                "approval_required": true,
                "deprecated": true,
            }),
        ),
        (
            "an Operation with a matched argument its signature line does not show",
            operation_hit(
                text(None, &[]),
                SearchEvidence::new(
                    vec!["description".into()],
                    vec![
                        "input.query".into(),
                        "input.query.text".into(),
                        "input.repo".into(),
                        "result.data[].query".into(),
                    ],
                ),
                OperationHints::default(),
                ApprovalRequirement::NotRequired,
                false,
            ),
            json!({
                "call_path": "coral.providers.github.pulls.list",
                "matched_inputs": ["query"],
            }),
        ),
        (
            "a Saved Function that matched in its declaration",
            SearchHit::from_saved_function(
                CallPath::saved_function("github.openPulls").expect("Call Path"),
                text(None, &[]),
                SearchEvidence::new(vec!["title".into()], vec!["input.repo".into()]),
            ),
            json!({ "call_path": "coral.functions.github.openPulls" }),
        ),
    ] {
        let mut expected = expected;
        merge(
            &mut expected,
            json!({
                "signature_line": "list(input: { repo: string }): Promise<{ data: Array<Pull> }>",
                "description_snippet": "Lists pull requests.",
            }),
        );
        assert_eq!(super::hit(&hit), expected, "{name}");
    }
}

#[test]
fn a_page_states_its_cursor_and_retriever_statuses_only_when_they_matter() {
    let cursor = SearchCursor::parse("eyJwb3NpdGlvbiI6MX0").expect("cursor");
    for (name, page, expected) in [
        (
            "a last page that every retriever searched",
            SearchPage::new(
                Vec::new(),
                None,
                false,
                vec![
                    status(EntityKind::Operation, RetrieverState::ResultsFound),
                    status(EntityKind::SavedFunction, RetrieverState::Empty),
                ],
            ),
            json!({ "hits": [], "has_more": false }),
        ),
        (
            "a page with more results and one retriever that failed",
            SearchPage::new(
                Vec::new(),
                Some(cursor.clone()),
                true,
                vec![
                    status(EntityKind::Operation, RetrieverState::Partial),
                    status(EntityKind::SavedFunction, RetrieverState::ResultsFound),
                ],
            ),
            json!({
                "hits": [],
                "has_more": true,
                "next_cursor": cursor.to_string(),
                "retriever_statuses": [{
                    "retriever": "operation",
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

fn operation_hit(
    text: HitText,
    evidence: SearchEvidence,
    hints: OperationHints,
    approval: ApprovalRequirement,
    deprecated: bool,
) -> SearchHit {
    let call_root = ProviderCallRoot::parse("coral.providers.github").expect("call root");
    let presentation: OperationPresentation = serde_json::from_value(json!({
        "title": "List pull requests",
        "description": "Lists pull requests.",
        "deprecated": deprecated,
        "tags": ["pulls"],
        "guidance": "Prefer search for one author.",
    }))
    .expect("presentation");
    let description = OperationAuthoringDescription::new(
        CallPath::operation(&call_root, "pulls.list").expect("Call Path"),
        "declare function list(): void;".into(),
        call_root,
        ProviderId::parse("6f1c7e52-7d1d-4a8e-9a43-3c1d1f0b5a10").expect("Provider id"),
        "GitHub".into(),
        presentation,
        hints,
        approval,
    );
    SearchHit::from_operation(&description, text, evidence)
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
