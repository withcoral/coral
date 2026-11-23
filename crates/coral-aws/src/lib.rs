//! Coral's AWS identity: credentials, the account check and `SigV4` signing.
//!
//! Credentials come from `aws-config`, whose own HTTP client reaches AWS's
//! credential addresses and STS outside the outbound destination policy
//! (ADR 0041). A caller sends a request it signs here through
//! `coral-outbound`, or hands it on.

mod environment;
mod identity;
mod settings;

pub use environment::Environment;
pub use identity::{AwsIdentity, Error};
pub use settings::{Settings, SettingsError};
