//! Safe discovery responses, shared by current callers and historical snapshots.
use crate::{
    ApprovalRequirement, AuthoringDescription, EntityKind, ExactAuthoringDescription,
    RetrieverState, RetrieverStatus, SearchHit, SearchPage,
};
use coral_catalog::OperationHints;
use serde_json::{Map, Value, json};

#[cfg(test)]
mod tests;

/// Projects every ordered hit, continuation metadata, and one status per
/// retriever, without truncation.
#[must_use]
pub fn search(page: &SearchPage) -> Value {
    json!({
        "hits": page.hits().iter().map(hit).collect::<Vec<_>>(),
        "next_cursor": page.next_cursor().map(ToString::to_string),
        "has_more": page.has_more(),
        "retriever_statuses": page
            .retriever_statuses()
            .iter()
            .map(retriever_status)
            .collect::<Vec<_>>(),
    })
}

/// One Search Hit. Keys absent for a Saved Function do not apply to it; the
/// Guidance Snippet and matched tags appear only on a match.
fn hit(hit: &SearchHit) -> Value {
    let mut fields = Map::from_iter([
        ("kind".to_string(), json!(entity_kind(hit.kind()))),
        ("call_path".to_string(), json!(hit.call_path().as_str())),
        ("signature_line".to_string(), json!(hit.signature_line())),
        (
            "description_snippet".to_string(),
            json!(hit.description_snippet()),
        ),
        (
            "evidence".to_string(),
            json!({
                "fields": hit.evidence().fields(),
                "paths": hit.evidence().paths(),
            }),
        ),
    ]);
    if let Some(operation) = hit.operation() {
        fields.extend([
            (
                "provider_display_name".to_string(),
                json!(operation.provider_display_name()),
            ),
            ("title".to_string(), json!(operation.title())),
            ("hints".to_string(), hints(operation.hints())),
            (
                "approval_required".to_string(),
                json!(operation.approval_requirement() == ApprovalRequirement::Required),
            ),
            ("deprecated".to_string(), json!(operation.deprecated())),
        ]);
    }
    if let Some(guidance) = hit.guidance_snippet() {
        fields.insert("guidance_snippet".to_string(), json!(guidance));
    }
    if !hit.matched_tags().is_empty() {
        fields.insert("matched_tags".to_string(), json!(hit.matched_tags()));
    }
    Value::Object(fields)
}

fn retriever_status(status: &RetrieverStatus) -> Value {
    let coverage = status.coverage();
    json!({
        "retriever": entity_kind(status.retriever()),
        "state": match status.state() {
            RetrieverState::ResultsFound => "results_found",
            RetrieverState::Empty => "empty",
            RetrieverState::Skipped => "skipped",
            RetrieverState::Partial => "partial",
            RetrieverState::Error => "error",
        },
        "coverage": {
            "eligible_units": coverage.eligible_units(),
            "searched_units": coverage.searched_units(),
            "failed_units": coverage.failed_units(),
            "returned_count": coverage.returned_count(),
            "has_more": coverage.has_more(),
        },
    })
}

/// Projects the complete exact response, including owned source and saved diagnostics.
#[must_use]
pub fn exact_description(description: &ExactAuthoringDescription) -> Value {
    let mut result = authoring(description.authoring());
    let Value::Object(fields) = &mut result else {
        return result;
    };
    if let Some(detail) = description.saved_function_detail() {
        fields.insert("source".to_string(), json!(detail.source().as_str()));
        fields.insert("diagnostics".to_string(), json!(detail.diagnostics()));
        fields.insert(
            "diagnostics_truncated".to_string(),
            json!(detail.diagnostics_truncated()),
        );
    }
    result
}

fn authoring(description: &AuthoringDescription) -> Value {
    let mut fields = Map::from_iter([
        ("kind".to_string(), json!(entity_kind(description.kind()))),
        (
            "call_path".to_string(),
            json!(description.call_path().as_str()),
        ),
        ("description".to_string(), json!(description.description())),
        (
            "typescript_declaration".to_string(),
            json!(description.typescript_declaration()),
        ),
    ]);
    if let AuthoringDescription::Operation(operation) = description {
        fields.extend([
            (
                "provider_access".to_string(),
                json!(crate::provider_filter_value(operation.provider_call_root())),
            ),
            (
                "provider_id".to_string(),
                json!(operation.provider_id().to_string()),
            ),
            (
                "provider_display_name".to_string(),
                json!(operation.provider_display_name()),
            ),
            ("title".to_string(), json!(operation.title())),
            ("deprecated".to_string(), json!(operation.deprecated())),
            ("tags".to_string(), json!(operation.tags())),
            ("guidance".to_string(), json!(operation.guidance())),
            ("hints".to_string(), hints(operation.hints())),
            (
                "approval_required".to_string(),
                json!(operation.approval_requirement() == ApprovalRequirement::Required),
            ),
        ]);
    }
    Value::Object(fields)
}

fn entity_kind(kind: EntityKind) -> &'static str {
    match kind {
        EntityKind::Operation => "operation",
        EntityKind::SavedFunction => "saved_function",
    }
}

/// Only the hints the Provider declared. An absent key is undeclared, not a
/// protocol default.
fn hints(hints: OperationHints) -> Value {
    Value::Object(
        [
            ("read_only", hints.read_only()),
            ("destructive", hints.destructive()),
            ("idempotent", hints.idempotent()),
            ("open_world", hints.open_world()),
        ]
        .into_iter()
        .filter_map(|(name, hint)| hint.map(|hint| (name.to_string(), json!(hint))))
        .collect(),
    )
}
