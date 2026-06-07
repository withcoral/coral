use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

#[derive(Debug, Default)]
pub(crate) struct OperationIdAllocator {
    allocated: BTreeSet<String>,
    next_suffixes: BTreeMap<String, usize>,
}

impl OperationIdAllocator {
    pub(crate) fn allocate(&mut self, raw: &str) -> String {
        let base = normalized_operation_id_base(raw);
        if self.allocated.insert(base.clone()) {
            return base;
        }

        let next_suffix = self.next_suffixes.entry(base.clone()).or_insert(2);
        loop {
            let candidate = format!("{base}_{next_suffix}");
            *next_suffix += 1;
            if self.allocated.insert(candidate.clone()) {
                return candidate;
            }
        }
    }
}

pub(crate) fn normalize_operation_id(raw: &str) -> String {
    normalized_operation_id_base(raw)
}

fn normalized_operation_id_base(raw: &str) -> String {
    let mut out = String::new();
    let mut previous_lower_or_digit = false;
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            if ch.is_ascii_uppercase()
                && previous_lower_or_digit
                && !out.is_empty()
                && !out.ends_with('_')
            {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
            previous_lower_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
        } else if !out.ends_with('_') {
            out.push('_');
            previous_lower_or_digit = false;
        } else {
            previous_lower_or_digit = false;
        }
    }
    let normalized = out.trim_matches('_');
    if normalized.is_empty() {
        "operation".to_string()
    } else {
        normalized.to_string()
    }
}

pub(crate) fn pascal(raw: &str) -> String {
    normalize_operation_id(raw)
        .split('_')
        .filter(|segment| !segment.is_empty())
        .fold(String::new(), |mut out, segment| {
            let mut chars = segment.chars();
            if let Some(first) = chars.next().map(|ch| ch.to_ascii_uppercase()) {
                write!(out, "{first}").expect("writing to String cannot fail");
                out.push_str(chars.as_str());
            }
            out
        })
}
