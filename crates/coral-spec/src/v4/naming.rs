pub fn normalize_identifier(value: &str, prefix: &str) -> String {
    let mut output = String::new();
    let mut last_underscore = false;
    for c in value.chars() {
        if c.is_ascii_alphanumeric() {
            output.push(c.to_ascii_lowercase());
            last_underscore = false;
        } else if !last_underscore {
            output.push('_');
            last_underscore = true;
        }
    }
    let output = output.trim_matches('_').to_string();
    if output.is_empty() || output.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        format!("{prefix}_{output}")
    } else {
        output
    }
}

pub(crate) fn normalize_sql_identifier(value: &str, prefix: &str) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    let mut output = String::new();
    let mut last_underscore = false;
    for (index, c) in chars.iter().copied().enumerate() {
        if c.is_ascii_alphanumeric() {
            let previous = index.checked_sub(1).and_then(|idx| chars.get(idx)).copied();
            let next = chars.get(index + 1).copied();
            let starts_camel_word = c.is_ascii_uppercase()
                && !output.is_empty()
                && !last_underscore
                && (previous
                    .is_some_and(|prev| prev.is_ascii_lowercase() || prev.is_ascii_digit())
                    || (previous.is_some_and(|prev| prev.is_ascii_uppercase())
                        && next.is_some_and(|next| next.is_ascii_lowercase())));
            if starts_camel_word {
                output.push('_');
            }
            output.push(c.to_ascii_lowercase());
            last_underscore = false;
        } else if !last_underscore {
            output.push('_');
            last_underscore = true;
        }
    }
    let output = output.trim_matches('_').to_string();
    if output.is_empty() || output.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        format!("{prefix}_{output}")
    } else {
        output
    }
}

pub(crate) fn singularize(value: &str) -> String {
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

pub(crate) fn pluralize(value: &str) -> String {
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

pub(crate) fn stable_suffix(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{hash:016x}").chars().take(8).collect()
}

#[cfg(test)]
mod tests {
    use super::normalize_sql_identifier;

    #[test]
    fn sql_identifier_normalization_splits_camel_case_and_acronyms() {
        let cases = [
            ("perPage", "per_page"),
            ("pullNumber", "pull_number"),
            ("notificationID", "notification_id"),
            ("repositoryURLValue", "repository_url_value"),
        ];

        for (input, expected) in cases {
            assert_eq!(
                normalize_sql_identifier(input, "input"),
                expected,
                "failed to normalize {input}"
            );
        }
    }
}
