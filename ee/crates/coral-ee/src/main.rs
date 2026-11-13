// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

//! The released Coral binary. It runs exactly like the `coral` binary until
//! the Edition is Enterprise, apart from the Secret Store kinds it can build,
//! whose reads fail closed until then.

use std::process::ExitCode;
use std::sync::Arc;

fn main() -> ExitCode {
    let edition = coral_edition::EditionCheck::default();
    let secret_store_kinds = coral_secret_store::SecretStoreKinds::default().with(Arc::new(
        coral_ee_secret_stores::KvV2Kind::new(edition.clone()),
    ));
    coral::run(coral_app::Extensions {
        edition,
        secret_store_kinds,
    })
}
