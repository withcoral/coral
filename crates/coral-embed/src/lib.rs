//! Turns text into a vector with the embedding model that ADR 0035 picks.
//!
//! [`MANIFEST`] is the model and how Universal Search uses it, compiled into
//! the binary. [`EmbedderRevision`] is the digest of the manifest fields that
//! change stored vectors. [`check_cpu`] tells whether this CPU meets the
//! floor below which the vector side stays off.
//!
//! [`Manifest::find`] looks for the manifest's files in the folders that
//! [`models_dirs`] lists and checks their digests.
//!
//! [`Model`] loads the manifest's files into ONNX Runtime and gives vectors
//! through [`Embed`], the one interface search code takes.

mod cpu;
mod files;
mod manifest;
mod model;
mod query_first;

// The integration tests read their fixture with serde.
#[cfg(test)]
use serde as _;

pub use cpu::{CpuTooOld, check_cpu};
pub use files::{
    ModelState, SYSTEM_MODELS_DIR, UnreadableModelFile, models_dirs, state_models_dir,
};
pub use manifest::{
    EmbedderRevision, IndexText, MANIFEST, Manifest, Merge, ModelFile, Pooling, QueryText,
};
pub use model::{Embed, EmbedError, LoadError, Model};
