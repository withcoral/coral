//! Durable, secret-free results of expensive Setup work. Reads never rebuild them.
use std::collections::BTreeMap;
use std::sync::Arc;

use coral_catalog::{
    CatalogDiagnostic, CatalogDiagnosticSeverity, MaterializationOutcome,
    ProviderConfigurationReview, ProviderCredentialContract, ProviderSetupInput,
    ProviderSetupInputDigest, ToolAvailability,
};
use coral_outbound::DestinationPolicy;
use serde::{Deserialize, Serialize};

use crate::publication::materialization::{CatalogMaterializer, Materializer};
use crate::{Error, repository::LoadedSetup};

// Advance when source analysis, validation, or preview semantics change. Never reuse
// a preparation produced by a different algorithm, even for identical source bytes.
const VERSION: u32 = 6;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SourceAnalysis {
    version: u32,
    input_digest: String,
    pub(crate) document: Result<ProviderConfigurationReview, CatalogDiagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PreparedReview {
    version: u32,
    materializer_version: String,
    input_digest: String,
    revision: i64,
    pub(crate) contract: ProviderCredentialContract,
    rejected_contract: ProviderCredentialContract,
    pub(crate) destinations: BTreeMap<String, String>,
    pub(crate) tools: Vec<super::ReviewedTool>,
    pub(crate) unavailable_tools: BTreeMap<String, Vec<CatalogDiagnostic>>,
    pub(crate) diagnostic: Option<String>,
}

impl SourceAnalysis {
    /// Reviews an OpenAPI source's configuration. This is CPU work: run it
    /// through [`Materializer::analyze`], off the async runtime.
    pub(crate) fn of(input: ProviderSetupInput) -> Option<Self> {
        let digest = ProviderSetupInputDigest::of_setup_input(&input);
        let ProviderSetupInput::OpenApi(document) = input else {
            return None;
        };
        Some(Self {
            version: VERSION,
            input_digest: digest.as_str().into(),
            document: document.configuration_review(),
        })
    }

    pub(crate) fn check(&self, loaded: &LoadedSetup) -> Result<(), Error> {
        if self.version != VERSION {
            return Err(Error::ProviderSetupNotApplicable);
        }
        check_digest(&self.input_digest, loaded)
    }
}

impl PreparedReview {
    pub(crate) fn check(
        &self,
        _policy: DestinationPolicy,
        loaded: &LoadedSetup,
    ) -> Result<(), Error> {
        if self.version != VERSION
            || self.materializer_version
                != coral_catalog::materializer_version(loaded.setup.input_kind())
        {
            return Err(Error::ProviderSetupNotApplicable);
        }
        if self.revision != loaded.configuration_revision {
            return Err(Error::CorruptData(
                "setup preview has a stale revision".into(),
            ));
        }
        check_digest(&self.input_digest, loaded)
    }

    /// Destination policy remains a live host decision, not cached authority. The
    /// stored catalog permits loopback and private networks without making any
    /// outbound request. Recheck its resolved addresses under the current policy.
    pub(crate) fn for_policy(&self, policy: DestinationPolicy) -> std::borrow::Cow<'_, Self> {
        let Err(diagnostic) = coral_catalog::validate_provider_destinations(
            policy,
            self.destinations.values().map(String::as_str),
            &self.contract,
        ) else {
            return std::borrow::Cow::Borrowed(self);
        };
        let mut rejected = self.clone();
        rejected.contract.clone_from(&self.rejected_contract);
        rejected.destinations.clear();
        rejected.tools.clear();
        rejected.unavailable_tools.clear();
        rejected.diagnostic = Some(diagnostic.code().into());
        std::borrow::Cow::Owned(rejected)
    }
}

fn check_digest(digest: &str, loaded: &LoadedSetup) -> Result<(), Error> {
    if loaded
        .setup
        .input_digest()
        .is_none_or(|input| input.as_str() != digest)
    {
        return Err(Error::CorruptData(
            "setup preparation has a different input".into(),
        ));
    }
    Ok(())
}

/// Owns Setup computation separately from final Publication and its path assignments.
#[derive(Clone)]
pub(crate) struct Preparation {
    materializer: Arc<dyn Materializer>,
}

impl Preparation {
    pub(crate) fn new() -> Self {
        Self {
            materializer: Arc::new(CatalogMaterializer(
                DestinationPolicy::default()
                    .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps)
                    .with_private_networks(coral_outbound::NetworkAccess::HttpAndHttps),
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_materializer(mut self, materializer: Arc<dyn Materializer>) -> Self {
        self.materializer = materializer;
        self
    }

    pub(crate) async fn analyze(
        &self,
        input: Option<&ProviderSetupInput>,
    ) -> Result<Option<Box<SourceAnalysis>>, Error> {
        let Some(input @ ProviderSetupInput::OpenApi(_)) = input else {
            return Ok(None);
        };
        self.materializer
            .analyze(input.clone())
            .await
            .map(|analysis| analysis.map(Box::new))
            .map_err(|_worker| Error::SetupPreparationUnavailable)
    }

    pub(crate) async fn preview(
        &self,
        input: Option<&ProviderSetupInput>,
        revision: i64,
    ) -> Result<Option<Box<PreparedReview>>, Error> {
        let Some(input) = input else { return Ok(None) };
        let outcome = self
            .materializer
            .materialize(input, &[])
            .await
            .map_err(|_worker| Error::SetupPreparationUnavailable)?;
        let mut review = PreparedReview {
            version: VERSION,
            materializer_version: coral_catalog::materializer_version(input.kind()).into(),
            input_digest: ProviderSetupInputDigest::of_setup_input(input)
                .as_str()
                .into(),
            revision,
            // Structural failures still belong to Publication. Preserve the selected
            // OpenAPI authentication method without inventing executable destinations.
            contract: match input {
                ProviderSetupInput::OpenApi(document) => document
                    .configuration()
                    .and_then(|values| values.authentication.clone())
                    .map_or(ProviderCredentialContract::NONE, Into::into),
                ProviderSetupInput::Mcp(_) => ProviderCredentialContract::NONE,
            },
            rejected_contract: ProviderCredentialContract::NONE,
            destinations: BTreeMap::new(),
            tools: Vec::new(),
            unavailable_tools: BTreeMap::new(),
            diagnostic: None,
        };
        review.rejected_contract.clone_from(&review.contract);
        match outcome {
            MaterializationOutcome::Ready(catalog) => {
                review.contract = catalog.credential_contract();
                for tool in catalog.tools() {
                    review.tools.push(super::ReviewedTool {
                        key: tool.key().as_str().into(),
                        relative_path: tool.relative_path().into(),
                        summary: tool.summary().into(),
                        tags: tool.presentation().tags().to_vec(),
                    });
                    if tool.availability() == ToolAvailability::Unavailable {
                        review.unavailable_tools.insert(
                            tool.key().as_str().into(),
                            tool.diagnostics()
                                .iter()
                                .filter(|diagnostic| {
                                    diagnostic.severity() == CatalogDiagnosticSeverity::Error
                                })
                                .cloned()
                                .collect(),
                        );
                    }
                    let destination = match tool.execution_binding() {
                        coral_catalog::ExecutionBinding::Http(binding) => binding.server_url(),
                        coral_catalog::ExecutionBinding::Mcp(binding) => Some(binding.server_url()),
                        coral_catalog::ExecutionBinding::Builtin(_) => None,
                    };
                    if let Some(url) = destination {
                        review
                            .destinations
                            .insert(tool.key().as_str().into(), url.into());
                    }
                }
            }
            MaterializationOutcome::Rejected(diagnostic) => {
                review.diagnostic = Some(diagnostic.code().into());
            }
        }
        Ok(Some(Box::new(review)))
    }
}
