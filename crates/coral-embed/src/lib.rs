//! Turns text into a vector with the embedding model of ADR 0033
//! ("Universal Search adds a vector list from a bundled CPU model").
//!
//! [`MANIFEST`] is the model and how Universal Search uses it, compiled into
//! the binary. [`EmbedderRevision`] is the digest of the manifest fields that
//! change stored vectors. [`check_cpu`] tells whether this CPU meets the
//! floor below which the vector side stays off.
//!
//! [`Model`] loads the manifest's files into ONNX Runtime and gives vectors
//! through [`Embed`], the one interface search code takes.

mod cpu;
mod manifest;
mod model;

// The integration tests read their fixture with serde.
#[cfg(test)]
use serde as _;

pub use cpu::{CpuTooOld, check_cpu};
pub use manifest::{
    EmbedderRevision, IndexText, MANIFEST, Manifest, Merge, ModelFile, Pooling, QueryText,
};
pub use model::{Embed, EmbedError, LoadError, Model};
