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
    let secret_store_kinds = coral_secret_store::SecretStoreKinds::default().with(Arc::new(
        coral_ee_secret_stores::KvV2Kind::new(edition.clone()),
    ));
    coral::run(coral_app::Extensions {
        edition,
        secret_store_kinds,
    })
}
