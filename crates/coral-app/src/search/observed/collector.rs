//! Observed-values batch collection.

#![allow(
    dead_code,
    reason = "observed-values provider substrate is staged before app wiring in the next PR"
)]

use std::collections::HashSet;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use crate::hash::sha256_hex;
use crate::search::observed::sensitive::{is_sensitive_column, is_sensitive_value};
use crate::search::observed::sqlite_queue::{ObservedValueCandidate, ObservedValuesQueuePayload};

#[derive(Debug, Clone, Default)]
pub(crate) struct ObservedValuesCollector {
    budget: ObservedValuesCollectionBudget,
}

impl ObservedValuesCollector {
    pub(crate) fn collect_batch(&self, batch: &RecordBatch) -> ObservedValuesQueuePayload {
        let mut values = Vec::new();
        let mut observed_bytes = 0_usize;
        let mut seen = HashSet::new();
        let schema = batch.schema();
        for column_index in 0..batch.num_columns() {
            let field = schema.field(column_index);
            let column_name = field.name();
            if is_sensitive_column(column_name) {
                continue;
            }
            let column = batch.column(column_index);
            for row_index in 0..batch.num_rows() {
                if values.len() >= self.budget.candidate_limit {
                    return ObservedValuesQueuePayload { values };
                }
                let Some(display_value) = observed_display_value(
                    column.as_ref(),
                    row_index,
                    self.budget.value_bytes_limit,
                ) else {
                    continue;
                };
                if is_sensitive_value(&display_value) {
                    continue;
                }
                let search_text = normalize_search_text(&display_value);
                if search_text.is_empty() {
                    continue;
                }
                if !seen.insert((column_name.clone(), search_text.clone())) {
                    continue;
                }
                let candidate_bytes = column_name
                    .len()
                    .saturating_add(display_value.len())
                    .saturating_add(search_text.len());
                if observed_bytes.saturating_add(candidate_bytes)
                    > self.budget.candidate_bytes_limit
                {
                    return ObservedValuesQueuePayload { values };
                }
                observed_bytes = observed_bytes.saturating_add(candidate_bytes);
                values.push(ObservedValueCandidate {
                    column_name: column_name.clone(),
                    display_value,
                    search_text: search_text.clone(),
                    value_key: sha256_hex(search_text.as_bytes()),
                });
            }
        }
        ObservedValuesQueuePayload { values }
    }

    pub(crate) fn budget(&self) -> &ObservedValuesCollectionBudget {
        &self.budget
    }
}

#[derive(Debug, Clone)]
#[expect(
    clippy::struct_field_names,
    reason = "observed-values collection budgets are byte/count limits with explicit units"
)]
pub(crate) struct ObservedValuesCollectionBudget {
    pub(crate) candidate_limit: usize,
    pub(crate) candidate_bytes_limit: usize,
    pub(crate) value_bytes_limit: usize,
    pub(crate) job_bytes_limit: usize,
}

const DEFAULT_CANDIDATE_LIMIT: usize = 10_000;
const DEFAULT_CANDIDATE_BYTES_LIMIT: usize = 8 * 1024 * 1024;
const DEFAULT_VALUE_BYTES_LIMIT: usize = 4 * 1024;
const DEFAULT_JOB_BYTES_LIMIT: usize = 1024 * 1024;

impl Default for ObservedValuesCollectionBudget {
    fn default() -> Self {
        Self {
            candidate_limit: DEFAULT_CANDIDATE_LIMIT,
            candidate_bytes_limit: DEFAULT_CANDIDATE_BYTES_LIMIT,
            value_bytes_limit: DEFAULT_VALUE_BYTES_LIMIT,
            job_bytes_limit: DEFAULT_JOB_BYTES_LIMIT,
        }
    }
}

#[cfg(test)]
impl ObservedValuesCollector {
    pub(crate) fn with_budget(budget: ObservedValuesCollectionBudget) -> Self {
        Self { budget }
    }
}

fn observed_display_value(
    column: &dyn Array,
    row_index: usize,
    max_value_bytes: usize,
) -> Option<String> {
    if column.is_null(row_index) {
        return None;
    }
    let value = array_value_to_string(column, row_index).ok()?;
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.len() > max_value_bytes {
        return None;
    }
    Some(trimmed.to_string())
}

fn normalize_search_text(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::{ObservedValuesCollectionBudget, ObservedValuesCollector};

    #[test]
    fn suppresses_sensitive_columns_and_values() {
        let batch = batch(
            ["name", "api_token", "note", "password_hash"],
            [
                vec!["Grace", "Ada"],
                vec!["ghp_supersecret", "plain-token"],
                vec!["ok", "sk-super-secret-value"],
                vec!["hashed", "values"],
            ],
        );
        let payload = ObservedValuesCollector::default().collect_batch(&batch);
        let values = payload
            .values
            .iter()
            .map(|candidate| candidate.display_value.as_str())
            .collect::<Vec<_>>();

        assert_eq!(values, ["Grace", "Ada", "ok"]);
    }

    #[test]
    fn suppresses_sensitive_column_stems_and_short_sk_prefixes_survive() {
        let batch = batch(
            [
                "ticket_key",
                "secret_key",
                "token_id",
                "api_key_value",
                "note",
            ],
            [
                vec!["SK-101"],
                vec!["secret"],
                vec!["token"],
                vec!["api-key"],
                vec!["sk-abcdefghijklmnopqrstuvwxyz"],
            ],
        );
        let payload = ObservedValuesCollector::default().collect_batch(&batch);
        let values = payload
            .values
            .iter()
            .map(|candidate| candidate.display_value.as_str())
            .collect::<Vec<_>>();

        assert_eq!(values, ["SK-101"]);
    }

    #[test]
    fn suppresses_embedded_sensitive_values() {
        let batch = batch(
            ["note"],
            [vec![
                "Bearer sk-abcdefghijklmnopqrstuvwxyz",
                "https://example.test/callback?api_key=literal-secret",
                "Ticket SK-101 is public",
            ]],
        );
        let payload = ObservedValuesCollector::default().collect_batch(&batch);
        let values = payload
            .values
            .iter()
            .map(|candidate| candidate.display_value.as_str())
            .collect::<Vec<_>>();

        assert_eq!(values, ["Ticket SK-101 is public"]);
    }

    #[test]
    fn collection_budget_caps_candidates() {
        let collector = ObservedValuesCollector::with_budget(ObservedValuesCollectionBudget {
            candidate_limit: 1,
            candidate_bytes_limit: usize::MAX,
            value_bytes_limit: usize::MAX,
            job_bytes_limit: usize::MAX,
        });
        let payload = collector.collect_batch(&batch(["name"], [vec!["Grace", "Ada"]]));

        assert_eq!(payload.values.len(), 1);
        assert_eq!(
            payload
                .values
                .first()
                .expect("one observed candidate")
                .display_value,
            "Grace"
        );
    }

    #[test]
    fn deduplicates_batch_values_before_budgeting() {
        let collector = ObservedValuesCollector::with_budget(ObservedValuesCollectionBudget {
            candidate_limit: 2,
            candidate_bytes_limit: usize::MAX,
            value_bytes_limit: usize::MAX,
            job_bytes_limit: usize::MAX,
        });
        let payload = collector.collect_batch(&batch(
            ["first", "second"],
            [
                vec!["Repeat", "Repeat", "Repeat"],
                vec!["Distinct", "Other", "More"],
            ],
        ));
        let values = payload
            .values
            .iter()
            .map(|candidate| {
                (
                    candidate.column_name.as_str(),
                    candidate.display_value.as_str(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(values, [("first", "Repeat"), ("second", "Distinct")]);
    }

    fn batch<const N: usize>(
        columns: [&'static str; N],
        values: [Vec<&'static str>; N],
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(
            columns
                .into_iter()
                .map(|name| Field::new(name, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));
        let arrays = values
            .into_iter()
            .map(|values| Arc::new(StringArray::from(values)) as _)
            .collect();
        RecordBatch::try_new(schema, arrays).expect("record batch")
    }
}
