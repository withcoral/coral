#![allow(dead_code, reason = "mounted by control-plane composition in t15")]

mod manager;
mod model;
mod service;

pub(crate) use manager::UserManager;
pub(crate) use model::{CurrentUser, UserView};
#[expect(unused_imports, reason = "mounted by control-plane composition in t15")]
pub(crate) use service::UserService;
