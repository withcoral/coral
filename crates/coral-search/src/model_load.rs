//! The search model as a Coral Instance has it: loaded, or why not. The
//! Embedder and the query embedding read the same slot, so a span can say
//! why a search ranked by words alone (ADR 0035, "The setting and
//! failures").

use std::sync::Arc;

use coral_embed::Embed;

use crate::retriever::LexicalReason;

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
    /// The loaded model, or why a search ranks by words alone without it.
    pub(crate) fn model(&self) -> Result<Arc<dyn Embed>, LexicalReason> {
        match self {
            Self::Ready(model) => Ok(Arc::clone(model)),
            Self::Pending => Err(LexicalReason::ModelAbsent),
            Self::Failed => Err(LexicalReason::ModelFailed),
            Self::CpuTooOld => Err(LexicalReason::CpuFloor),
        }
    }
}
