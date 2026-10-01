//! Pure setup-input preparation and resolution for installed identity specs.

use std::collections::BTreeMap;
use std::fmt;

use coral_spec::{IdentityManifest, ManifestInputKind};

use crate::bootstrap::AppError;
use crate::state::db::{DbError, IdentitySpecKey, IdentitySpecScope};

/// One caller-supplied setup input. Its value must never appear in diagnostics.
pub(crate) struct IdentitySpecInputValue {
    key: String,
    value: String,
}

impl IdentitySpecInputValue {
    pub(crate) fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

impl fmt::Debug for IdentitySpecInputValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IdentitySpecInputValue")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

/// Canonical explicit material ready for encrypted persistence.
pub(crate) struct PreparedIdentitySpecInputMaterial {
    values: BTreeMap<String, String>,
}

impl PreparedIdentitySpecInputMaterial {
    pub(crate) fn values(&self) -> &BTreeMap<String, String> {
        &self.values
    }
}

impl fmt::Debug for PreparedIdentitySpecInputMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedIdentitySpecInputMaterial")
            .field("value_count", &self.values.len())
            .finish()
    }
}

/// Runtime-ready setup inputs partitioned by the authored input kind.
pub(crate) struct ResolvedIdentitySpecInputs {
    variables: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
}

impl ResolvedIdentitySpecInputs {
    pub(crate) fn variables(&self) -> &BTreeMap<String, String> {
        &self.variables
    }

    pub(crate) fn secrets(&self) -> &BTreeMap<String, String> {
        &self.secrets
    }
}

impl fmt::Debug for ResolvedIdentitySpecInputs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedIdentitySpecInputs")
            .field("variable_count", &self.variables.len())
            .field("secret_count", &self.secrets.len())
            .finish()
    }
}

/// Merge caller values with compatible previous material without persisting defaults.
pub(crate) fn prepare_identity_spec_input_material(
    key: &IdentitySpecKey,
    manifest: &IdentityManifest,
    previous_manifest: Option<&IdentityManifest>,
    previous_values: &BTreeMap<String, String>,
    supplied: &[IdentitySpecInputValue],
) -> Result<PreparedIdentitySpecInputMaterial, AppError> {
    if manifest.name != key.name() {
        return Err(AppError::InvalidInput(format!(
            "identity spec manifest name '{}' does not match key '{}'",
            manifest.name,
            key.name()
        )));
    }
    let current_kinds = input_kinds(manifest);
    let previous_kinds = previous_manifest.map(input_kinds).unwrap_or_default();
    validate_previous_material(key, previous_manifest, previous_values, &previous_kinds)?;

    let mut supplied_values = BTreeMap::new();
    for input in supplied {
        if !current_kinds.contains_key(input.key.as_str()) {
            return Err(AppError::InvalidInput(format!(
                "unknown identity spec input '{}' for identity spec '{}'",
                input.key, manifest.name
            )));
        }
        if supplied_values
            .insert(input.key.as_str(), non_blank_value(&input.value))
            .is_some()
        {
            return Err(AppError::InvalidInput(format!(
                "duplicate identity spec input '{}' for identity spec '{}'",
                input.key, manifest.name
            )));
        }
    }

    let mut values = BTreeMap::new();
    for input in &manifest.inputs {
        let supplied_value = supplied_values.get(input.key.as_str()).cloned().flatten();
        let previous_value = (input.kind == ManifestInputKind::Variable
            && previous_kinds.get(input.key.as_str()) == Some(&input.kind))
        .then(|| previous_values.get(&input.key))
        .flatten()
        .and_then(|value| non_blank_value(value));
        if let Some(value) = supplied_value.or(previous_value) {
            values.insert(input.key.clone(), value);
        }
    }
    resolve_declared_inputs(manifest, &values).map_err(|missing| {
        AppError::InvalidInput(format!(
            "missing required identity spec input '{missing}' for identity spec '{}'",
            manifest.name
        ))
    })?;
    Ok(PreparedIdentitySpecInputMaterial { values })
}

/// Validate and partition decrypted setup material for use.
pub(crate) fn resolve_identity_spec_inputs_for_use(
    key: &IdentitySpecKey,
    manifest: &IdentityManifest,
    stored_values: &BTreeMap<String, String>,
) -> Result<ResolvedIdentitySpecInputs, AppError> {
    if manifest.name != key.name() {
        return Err(corrupt_input_material(
            key,
            &format!(
                "manifest name '{}' does not match its persistence key",
                manifest.name
            ),
        )
        .into());
    }
    let declared = input_kinds(manifest);
    if let Some(unknown) = stored_values
        .keys()
        .find(|input_key| !declared.contains_key(input_key.as_str()))
    {
        return Err(
            corrupt_input_material(key, &format!("contains undeclared input '{unknown}'")).into(),
        );
    }
    resolve_declared_inputs(manifest, stored_values).map_err(|missing| {
        AppError::FailedPrecondition(format!(
            "missing identity spec input '{missing}' for identity spec '{}'",
            manifest.name
        ))
    })
}

fn validate_previous_material(
    key: &IdentitySpecKey,
    previous_manifest: Option<&IdentityManifest>,
    previous_values: &BTreeMap<String, String>,
    previous_kinds: &BTreeMap<&str, ManifestInputKind>,
) -> Result<(), DbError> {
    if let Some(previous_manifest) = previous_manifest
        && previous_manifest.name != key.name()
    {
        return Err(corrupt_input_material(
            key,
            &format!(
                "previous manifest name '{}' does not match its persistence key",
                previous_manifest.name
            ),
        ));
    }
    if previous_manifest.is_none() && !previous_values.is_empty() {
        return Err(corrupt_input_material(
            key,
            "exists without a previous identity spec",
        ));
    }
    if let Some(unknown) = previous_values
        .keys()
        .find(|input_key| !previous_kinds.contains_key(input_key.as_str()))
    {
        return Err(corrupt_input_material(
            key,
            &format!("contains undeclared input '{unknown}'"),
        ));
    }
    Ok(())
}

fn resolve_declared_inputs(
    manifest: &IdentityManifest,
    values: &BTreeMap<String, String>,
) -> Result<ResolvedIdentitySpecInputs, String> {
    let mut variables = BTreeMap::new();
    let mut secrets = BTreeMap::new();
    for input in &manifest.inputs {
        let value = values
            .get(&input.key)
            .and_then(|value| non_blank_value(value))
            .or_else(|| {
                (input.kind == ManifestInputKind::Variable && !input.required)
                    .then(|| non_blank_value(&input.default_value))
                    .flatten()
            });
        let Some(value) = value else {
            if input.required {
                return Err(input.key.clone());
            }
            continue;
        };
        match input.kind {
            ManifestInputKind::Variable => variables.insert(input.key.clone(), value),
            ManifestInputKind::Secret => secrets.insert(input.key.clone(), value),
        };
    }
    Ok(ResolvedIdentitySpecInputs { variables, secrets })
}

fn input_kinds(manifest: &IdentityManifest) -> BTreeMap<&str, ManifestInputKind> {
    manifest
        .inputs
        .iter()
        .map(|input| (input.key.as_str(), input.kind))
        .collect()
}

fn non_blank_value(value: &str) -> Option<String> {
    (!value.trim().is_empty()).then(|| value.to_string())
}

fn corrupt_input_material(key: &IdentitySpecKey, detail: &str) -> DbError {
    let scope = match key.scope() {
        IdentitySpecScope::Global => "global".to_string(),
        IdentitySpecScope::Workspace(workspace) => format!("workspace:{workspace}"),
    };
    DbError::CorruptData(format!(
        "identity spec '{scope}:{}' has invalid encrypted input material: {detail}",
        key.name()
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use coral_spec::parse_identity_manifest_yaml;

    use super::{
        IdentitySpecInputValue, prepare_identity_spec_input_material as prepare,
        resolve_identity_spec_inputs_for_use as resolve,
    };
    use crate::bootstrap::AppError;
    use crate::state::db::IdentitySpecKey;

    #[test]
    fn preserves_input_boundaries_and_inherits_only_variables() {
        let key = IdentitySpecKey::global("demo").unwrap();
        let previous = oauth_manifest(
            "  CLIENT_SECRET: {kind: secret}\n  REGION: {kind: variable, required: false}\n  REMOVE: {kind: variable, required: false}",
        );
        let current = oauth_manifest(
            "  CLIENT_SECRET: {kind: secret}\n  REGION: {kind: variable, required: false}\n  DEFAULTED: {kind: variable, default: '  fallback  ', required: false}\n  OPTIONAL_SECRET: {kind: secret, required: false}",
        );
        let previous_values = BTreeMap::from([
            ("CLIENT_SECRET".to_string(), "  old-secret  ".to_string()),
            ("REGION".to_string(), "  eu-west-1  ".to_string()),
            ("REMOVE".to_string(), "old-remove".to_string()),
        ]);
        let prepared = prepare(
            &key,
            &current,
            Some(&previous),
            &previous_values,
            &[IdentitySpecInputValue::new(
                "CLIENT_SECRET",
                "  new-secret  ",
            )],
        )
        .unwrap();
        assert_eq!(
            prepared.values(),
            &BTreeMap::from([
                ("CLIENT_SECRET".to_string(), "  new-secret  ".to_string()),
                ("REGION".to_string(), "  eu-west-1  ".to_string()),
            ])
        );
        let resolved = resolve(&key, &current, prepared.values()).unwrap();
        assert_eq!(
            value(resolved.secrets(), "CLIENT_SECRET"),
            Some("  new-secret  ")
        );
        assert_eq!(value(resolved.variables(), "REGION"), Some("  eu-west-1  "));
        assert_eq!(
            value(resolved.variables(), "DEFAULTED"),
            Some("  fallback  ")
        );
        assert!(!resolved.secrets().contains_key("OPTIONAL_SECRET"));

        assert!(matches!(
            prepare(&key, &current, Some(&previous), &previous_values, &[],),
            Err(AppError::InvalidInput(_))
        ));
    }

    #[test]
    fn preserves_stored_input_boundaries() {
        let key = IdentitySpecKey::global("demo").unwrap();
        let manifest = oauth_manifest(
            "  CLIENT_SECRET: {kind: secret}\n  REGION: {kind: variable, required: true}",
        );
        let stored = BTreeMap::from([
            ("CLIENT_SECRET".to_string(), "  stored-secret  ".to_string()),
            ("REGION".to_string(), "  stored-region  ".to_string()),
        ]);

        let resolved = resolve(&key, &manifest, &stored).unwrap();
        assert_eq!(
            value(resolved.secrets(), "CLIENT_SECRET"),
            Some("  stored-secret  ")
        );
        assert_eq!(
            value(resolved.variables(), "REGION"),
            Some("  stored-region  ")
        );
    }

    #[test]
    fn caller_errors_are_invalid_and_secret_debug_is_redacted() {
        let key = IdentitySpecKey::global("demo").unwrap();
        let manifest = oauth_manifest("  NEEDED: {kind: secret}");
        for supplied in [
            vec![IdentitySpecInputValue::new(" NEEDED ", "secret")],
            vec![IdentitySpecInputValue::new("NEEDED", " \t ")],
            vec![
                IdentitySpecInputValue::new("NEEDED", "one"),
                IdentitySpecInputValue::new("NEEDED", "two"),
            ],
            Vec::new(),
        ] {
            assert!(matches!(
                prepare(
                    &key,
                    &manifest,
                    Some(&manifest),
                    &BTreeMap::new(),
                    &supplied,
                ),
                Err(AppError::InvalidInput(_))
            ));
        }
        let supplied = IdentitySpecInputValue::new("NEEDED", "never-print-me");
        assert!(!format!("{supplied:?}").contains("never-print-me"));
        let prepared = prepare(
            &key,
            &manifest,
            Some(&manifest),
            &BTreeMap::new(),
            &[supplied],
        )
        .expect("supplied value repairs incomplete previous material");
        assert_eq!(value(prepared.values(), "NEEDED"), Some("never-print-me"));
        assert!(!format!("{prepared:?}").contains("never-print-me"));
    }

    #[test]
    fn stored_unknown_is_corrupt_but_missing_required_is_a_precondition() {
        let key = IdentitySpecKey::global("demo").unwrap();
        let manifest = oauth_manifest("  NEEDED: {kind: secret}");
        let unknown = BTreeMap::from([("OTHER".to_string(), "do-not-print".to_string())]);
        let error = resolve(&key, &manifest, &unknown).unwrap_err();
        assert!(matches!(error, AppError::Database(_)));
        assert!(!error.to_string().contains("do-not-print"));
        assert!(matches!(
            resolve(
                &key,
                &manifest,
                &BTreeMap::from([("NEEDED".to_string(), " \n ".to_string())]),
            ),
            Err(AppError::FailedPrecondition(_))
        ));
        let key = IdentitySpecKey::global("fixed").unwrap();
        let manifest = fixed_manifest();
        let empty = BTreeMap::new();
        assert!(
            prepare(&key, &manifest, None, &empty, &[])
                .unwrap()
                .values()
                .is_empty()
        );
        assert!(
            resolve(&key, &manifest, &empty)
                .unwrap()
                .variables()
                .is_empty()
        );
        assert!(matches!(
            prepare(
                &key,
                &manifest,
                None,
                &empty,
                &[IdentitySpecInputValue::new("TOKEN", "secret")],
            ),
            Err(AppError::InvalidInput(_))
        ));
        assert!(matches!(
            resolve(
                &key,
                &manifest,
                &BTreeMap::from([("TOKEN".to_string(), "secret".to_string())]),
            ),
            Err(AppError::Database(_))
        ));
    }

    #[test]
    fn rejects_mismatched_manifests_and_treats_required_defaults_as_missing() {
        let key = IdentitySpecKey::global("demo").unwrap();
        let other = named_oauth_manifest("other", "  TOKEN: {kind: secret}");
        assert!(matches!(
            prepare(&key, &other, None, &BTreeMap::new(), &[]),
            Err(AppError::InvalidInput(_))
        ));

        let current = oauth_manifest("  TOKEN: {kind: secret}");
        assert!(matches!(
            prepare(&key, &current, Some(&other), &BTreeMap::new(), &[]),
            Err(AppError::Database(_))
        ));
        assert!(matches!(
            resolve(&key, &other, &BTreeMap::new()),
            Err(AppError::Database(_))
        ));

        let optional = oauth_manifest("  BLANK: {kind: variable, default: '   ', required: false}");
        assert!(
            resolve(&key, &optional, &BTreeMap::new())
                .unwrap()
                .variables()
                .is_empty()
        );
        let required =
            oauth_manifest("  REQUIRED: {kind: variable, default: fallback, required: true}");
        assert!(matches!(
            prepare(&key, &required, None, &BTreeMap::new(), &[]),
            Err(AppError::InvalidInput(_))
        ));
        assert!(matches!(
            prepare(
                &key,
                &required,
                None,
                &BTreeMap::new(),
                &[IdentitySpecInputValue::new("REQUIRED", " \t ")],
            ),
            Err(AppError::InvalidInput(_))
        ));
        assert!(matches!(
            resolve(&key, &required, &BTreeMap::new()),
            Err(AppError::FailedPrecondition(_))
        ));
        assert!(matches!(
            resolve(
                &key,
                &required,
                &BTreeMap::from([("REQUIRED".to_string(), " \t ".to_string())]),
            ),
            Err(AppError::FailedPrecondition(_))
        ));
    }

    fn oauth_manifest(inputs: &str) -> coral_spec::IdentityManifest {
        named_oauth_manifest("demo", inputs)
    }

    fn named_oauth_manifest(name: &str, inputs: &str) -> coral_spec::IdentityManifest {
        parse_identity_manifest_yaml(&format!(
            "kind: identity\nspec_version: 1\nname: {name}\nversion: '1'\nissuer: demo\ntype: oauth\naudience: {{host: example.com}}\ninputs:\n{inputs}\noauth:\n  method:\n    flow: {{type: device_code}}\n    endpoints: {{device_authorization_url: 'https://example.com/device', token_url: 'https://example.com/token'}}\n    client: {{id: {{default: demo}}}}\n"
        )).unwrap()
    }

    fn fixed_manifest() -> coral_spec::IdentityManifest {
        parse_identity_manifest_yaml(
            "kind: identity\nspec_version: 1\nname: fixed\nversion: '1'\nissuer: demo\ntype: fixed_token\naudience: {host: example.com}\n",
        ).unwrap()
    }

    fn value<'a>(values: &'a BTreeMap<String, String>, key: &str) -> Option<&'a str> {
        values.get(key).map(String::as_str)
    }
}
