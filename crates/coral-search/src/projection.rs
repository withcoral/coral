//! Safe discovery responses, shared by current callers and historical snapshots.
use crate::{
    ApprovalRequirement, AuthoringDescription, EntityKind, ExactAuthoringDescription,
    RetrieverState, RetrieverStatus, SearchEvidence, SearchHit, SearchPage,
};
use coral_catalog::OperationHints;
use serde_json::{Map, Value, json};

#[cfg(test)]
mod tests;

/// Projects every ordered hit, and only what the agent acts on besides: the
/// cursor while more remain, and the status of each retriever whose results
/// may be missing.
#[must_use]
pub fn search(page: &SearchPage) -> Value {
    let mut fields = Map::from_iter([
        (
            "hits".to_string(),
            json!(page.hits().iter().map(hit).collect::<Vec<_>>()),
        ),
        ("has_more".to_string(), json!(page.has_more())),
    ]);
    if let Some(cursor) = page.next_cursor() {
        fields.insert("next_cursor".to_string(), json!(cursor.to_string()));
    }
    let incomplete = page
        .retriever_statuses()
        .iter()
        .filter(|status| {
            matches!(
                status.state(),
                RetrieverState::Partial | RetrieverState::Error
            )
        })
        .map(retriever_status)
        .collect::<Vec<_>>();
    if !incomplete.is_empty() {
        fields.insert("retriever_statuses".to_string(), json!(incomplete));
    }
    Value::Object(fields)
}

/// One Search Hit, cut to what the agent needs to choose and call it. The Call
/// Path already names the kind and the Provider, and `describe` gives the
/// rest. A flag appears only when set, a hint only when declared, and a match
/// only where a query term matched.
fn hit(hit: &SearchHit) -> Value {
    let mut fields = Map::from_iter([
        ("call_path".to_string(), json!(hit.call_path().as_str())),
        ("signature_line".to_string(), json!(hit.signature_line())),
        (
            "description_snippet".to_string(),
            json!(hit.description_snippet()),
        ),
    ]);
    if let Some(guidance) = hit.guidance_snippet() {
        fields.insert("guidance_snippet".to_string(), json!(guidance));
    }
    let matched = matched_fields(hit.evidence());
    if !matched.is_empty() {
        fields.insert("matched_fields".to_string(), json!(matched));
    }
    if !hit.matched_tags().is_empty() {
        fields.insert("matched_tags".to_string(), json!(hit.matched_tags()));
    }
    if let Some(operation) = hit.operation() {
        if operation.hints() != OperationHints::default() {
            fields.insert("hints".to_string(), hints(operation.hints()));
        }
        if operation.approval_requirement() == ApprovalRequirement::Required {
            fields.insert("approval_required".to_string(), json!(true));
        }
        if operation.deprecated() {
            fields.insert("deprecated".to_string(), json!(true));
        }
    }
    Value::Object(fields)
}

/// The fields a query term matched, in name order. One `declaration` stands
/// for every matched input or result path: a common word matches dozens, and
/// they explain the ranking rather than the choice.
fn matched_fields(evidence: &SearchEvidence) -> Vec<&str> {
    let mut fields = evidence
        .fields()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    if !evidence.paths().is_empty() {
        fields.push("declaration");
        fields.sort_unstable();
    }
    fields
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
                json!(operation.provider_namespace().as_str()),
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
