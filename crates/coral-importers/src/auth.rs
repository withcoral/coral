use coral_capabilities::{
    CredentialRequirement, CredentialRequirementAlternative, CredentialRequirementSet,
};
use coral_spec::{AuthDescriptor, SourceSpec};

pub(crate) fn credential_requirements(
    spec: &SourceSpec,
    auth: Option<&AuthDescriptor>,
) -> CredentialRequirementSet {
    match auth {
        Some(AuthDescriptor::BearerInput { key } | AuthDescriptor::HeaderInput { key, .. }) => {
            CredentialRequirementSet {
                alternatives: vec![CredentialRequirementAlternative {
                    requirements: vec![CredentialRequirement {
                        scheme_id: format!("{}_input", spec.name),
                        scopes: Vec::new(),
                        source_input_key: Some(key.clone()),
                    }],
                    anonymous: false,
                }],
            }
        }
        Some(AuthDescriptor::None) | None => CredentialRequirementSet::anonymous(),
    }
}
