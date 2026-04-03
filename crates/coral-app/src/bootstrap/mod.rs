//! Internal bootstrap seam for assembling the local server runtime.

mod consts;
mod env;
mod error;
mod server;

pub(crate) use error::{app_status, core_status};

pub use error::AppError;
pub use server::{RunningServer, ServerBuilder};
