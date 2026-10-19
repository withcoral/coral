//! Rebuild hints for the embedded migrations.
//!
//! `sqlx::migrate!` embeds the migration files at compile time, and Cargo does
//! not see them. Rebuild when a migration is added, changed, or removed.

fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
