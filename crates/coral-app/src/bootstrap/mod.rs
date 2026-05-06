//! Internal bootstrap seam for assembling the local server runtime.

mod consts;
mod env;
mod error;
mod server;

#[cfg(test)]
pub(crate) use error::MAX_STATUS_DETAIL_BYTES;
pub(crate) use error::{app_status, core_status};

pub use error::AppError;
pub use server::{RunningServer, ServerBuilder, ServerMode, StaticAsset, StaticAssetsProvider};
