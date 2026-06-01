use super::super::identifiers::normalize_identifier;
use super::super::ir::{IrExecutionAttachment, IrOperation, OutputCardinality};

pub(super) fn projection_name(operation: &IrOperation, is_search: bool) -> String {
    let entity = operation.entity.as_ref().map_or_else(
        || normalize_identifier(&operation.id, "projection"),
        |_| projection_entity_name(operation, is_search),
    );
    if is_search {
        return format!("search_{}", pluralize(&entity));
    }
    match operation.output.cardinality {
        OutputCardinality::List | OutputCardinality::WrappedList => pluralize(&entity),
        OutputCardinality::Singleton if operation.inputs.iter().any(|input| input.required) => {
            format!("get_{}", singularize(&entity))
        }
        OutputCardinality::Singleton => singularize(&entity),
        OutputCardinality::None | OutputCardinality::Unknown => {
            normalize_identifier(&operation.id, "projection")
        }
    }
}

fn projection_entity_name(operation: &IrOperation, is_search: bool) -> String {
    if is_search && let Some(search_entity) = search_entity_from_operation_id(&operation.id) {
        return search_entity;
    }
    operation.entity.as_ref().map_or_else(
        || normalize_identifier(&operation.id, "projection"),
        |entity| {
            let entity_name = normalize_entity_identifier(&entity.name);
            if operation.id.starts_with("pulls_") && entity_name == "pull_request" {
                "pull".to_string()
            } else {
                entity_name
            }
        },
    )
}

fn search_entity_from_operation_id(operation_id: &str) -> Option<String> {
    let mut raw = operation_id.strip_prefix("search_")?;
    raw = raw.strip_suffix("_and_pull_requests").unwrap_or(raw);
    raw = raw.strip_suffix("_result_items").unwrap_or(raw);
    Some(singularize(raw))
}

fn normalize_entity_identifier(raw: &str) -> String {
    let normalized = normalize_identifier(&entity_identifier_seed(raw), "projection");
    let mut tokens = normalized.split('_').collect::<Vec<_>>();
    tokens.retain(|token| !matches!(*token, "minimal" | "simple" | "base" | "short"));
    if tokens.is_empty() {
        normalized
    } else {
        tokens.join("_")
    }
}

fn entity_identifier_seed(raw: &str) -> String {
    let mut seed = String::new();
    let mut previous_was_lowercase_or_digit = false;
    for ch in raw.chars() {
        if ch.is_ascii_uppercase() && previous_was_lowercase_or_digit {
            seed.push('_');
        }
        if ch == '-' || ch == ' ' {
            seed.push('_');
            previous_was_lowercase_or_digit = false;
        } else {
            seed.push(ch.to_ascii_lowercase());
            previous_was_lowercase_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
        }
    }
    seed
}

pub(super) fn is_search_operation(operation: &IrOperation) -> bool {
    let id_tokens = operation.id.split('_').collect::<Vec<_>>();
    let path_has_search = match &operation.execution {
        IrExecutionAttachment::Rest(rest) => rest
            .path_template
            .split(|c: char| !c.is_ascii_alphanumeric())
            .any(|token| token.eq_ignore_ascii_case("search")),
    };
    path_has_search
        || id_tokens
            .iter()
            .any(|token| token.eq_ignore_ascii_case("search"))
}

fn singularize(value: &str) -> String {
    if let Some(stem) = value.strip_suffix("ies")
        && !stem.is_empty()
    {
        return format!("{stem}y");
    }
    for suffix in ["ches", "shes", "xes", "ses"] {
        if let Some(stem) = value.strip_suffix(suffix)
            && !stem.is_empty()
        {
            return format!("{stem}{}", suffix.trim_end_matches("es"));
        }
    }
    if value.ends_with('s')
        && !value.ends_with("ss")
        && !value.ends_with("us")
        && !value.ends_with("ics")
        && value != "news"
    {
        return value.trim_end_matches('s').to_string();
    }
    value.to_string()
}

fn pluralize(value: &str) -> String {
    if value.ends_with('s') {
        value.to_string()
    } else if let Some(stem) = value.strip_suffix('y') {
        if stem
            .chars()
            .next_back()
            .is_some_and(|c| !"aeiou".contains(c))
        {
            format!("{stem}ies")
        } else {
            format!("{value}s")
        }
    } else if value.ends_with('x') || value.ends_with("ch") || value.ends_with("sh") {
        format!("{value}es")
    } else {
        format!("{value}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_names_avoid_obvious_bad_singulars() {
        assert_eq!(singularize("status"), "status");
        assert_eq!(singularize("news"), "news");
        assert_eq!(singularize("analytics"), "analytics");
        assert_eq!(singularize("addresses"), "address");
        assert_eq!(pluralize("box"), "boxes");
    }
}
