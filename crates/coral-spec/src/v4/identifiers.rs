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

pub(crate) fn entity_name_from_ref(reference: &str) -> String {
    reference
        .rsplit('/')
        .next()
        .map_or_else(|| "entity".to_string(), |raw| raw.replace(" Response", ""))
}

pub(crate) fn type_id_from_ref(reference: &str) -> String {
    normalize_identifier(reference.rsplit('/').next().unwrap_or(reference), "type")
}

pub(crate) fn entity_name_from_path(path: &str) -> String {
    path.split('/')
        .rfind(|segment| !segment.is_empty() && !segment.starts_with('{'))
        .unwrap_or("entity")
        .to_string()
}

pub(crate) fn stable_suffix(value: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{hash:016x}").chars().take(8).collect()
}
