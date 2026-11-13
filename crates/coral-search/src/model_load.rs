//! The search model as a Coral Instance has it: loaded, or why not. The
//! Embedder and the query embedding read the same slot, so a span can say
//! why a search ranked by words alone (ADR 0035, "The setting and
//! failures").

use std::sync::Arc;

use coral_embed::Embed;

/// The search model as this Coral Instance has it.
#[derive(Clone)]
pub enum ModelLoad {
    /// The files are not ready yet: Coral looks for them or downloads them.
    Pending,
    /// The model is loaded.
    Ready(Arc<dyn Embed>),
    /// The model does not load while this process runs: Coral cannot read
    /// its files, stopped their download, or ONNX Runtime refused them.
    Failed,
    /// This CPU is below the model's floor.
    CpuTooOld,
}

impl ModelLoad {
    /// The loaded model, when there is one.
    pub(crate) fn ready(&self) -> Option<Arc<dyn Embed>> {
        match self {
            Self::Ready(model) => Some(Arc::clone(model)),
            Self::Pending | Self::Failed | Self::CpuTooOld => None,
        }
    }
}
