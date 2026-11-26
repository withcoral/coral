// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

//! The released Coral binary. Without a License it runs exactly like the
//! `coral` binary, apart from the Secret Store kinds it can build, whose reads
//! fail closed until a License sets the Edition to Enterprise. A configured
//! License that fails the check refuses the start, and an Evaluation License
//! stops Coral when it ends (ADR 0038).

use std::process::ExitCode;
use std::sync::Arc;

fn main() -> ExitCode {
    let edition = coral_edition::EditionCheck::new(Arc::new(
        coral_ee_license::LicensedEdition::for_this_build(),
    ));
    coral::run(extensions(edition))
}

/// What this binary adds to Coral, each part asking `edition` when used.
fn extensions(edition: coral_edition::EditionCheck) -> coral_app::Extensions {
    let secret_store_kinds = coral_secret_store::SecretStoreKinds::default()
        .with(Arc::new(coral_ee_secret_stores::KvV2Kind::new(
            edition.clone(),
        )))
        .with(Arc::new(
            coral_ee_secret_stores::AwsSecretsManagerKind::new(edition.clone()),
        ));
    let enterprise_features = coral_edition::EnterpriseFeatures::default()
        .with(coral_ee_secret_stores::EXTERNAL_SECRET_STORES);
    coral_app::Extensions {
        edition,
        secret_store_kinds,
        enterprise_features,
        ..coral_app::Extensions::default()
    }
}

#[cfg(test)]
mod tests {
    /// Config can name each Secret Store kind this binary adds.
    #[test]
    fn registers_its_secret_store_kinds() {
        let extensions = extensions(EditionCheck::new(Arc::new(Edition::Community)));
        for kind in ["kv-v2", "aws-secrets-manager"] {
            assert!(extensions.secret_store_kinds.get(kind).is_some(), "{kind}");
        }
    }

    use coral_edition::{Edition, EditionCheck, EditionInspection, FeatureState};

    use super::*;

    /// Reef shows External Secret Stores only because this binary
    /// registers it.
    #[test]
    fn registers_external_secret_stores() {
        let extensions = extensions(EditionCheck::new(Arc::new(Edition::Community)));
        let report =
            EditionInspection::new(extensions.edition, extensions.enterprise_features).report();
        assert_eq!(
            report.features,
            [(
                coral_ee_secret_stores::EXTERNAL_SECRET_STORES,
                FeatureState::NeedsEnterprise
            )]
        );
    }
}
