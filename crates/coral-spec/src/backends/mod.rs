//! Backend-specific validated manifest models.
//!
//! These modules define the normalized manifest shapes consumed by the engine:
//!
//! - [`http`] for HTTP-backed sources
//! - [`mod@file`] for file-backed sources such as `parquet` and `jsonl`
//!
//! Parsing entry points remain crate-private. Callers should normally use
//! [`crate::parse_source_manifest_yaml`] or [`crate::parse_source_manifest_value`]
//! and then inspect the resulting [`crate::ValidatedSourceManifest`].

pub mod file;
pub mod http;
