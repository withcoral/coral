// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

//! The released Coral binary. It runs exactly like the `coral` binary until
//! the Edition is Enterprise.

use std::process::ExitCode;

fn main() -> ExitCode {
    coral::run(coral_app::Extensions::default())
}
