#![cfg_attr(not(test), expect(dead_code, reason = "used higher in the PR stack"))]

mod manager;
mod model;
mod service;

#[expect(unused_imports, reason = "used higher in the PR stack")]
pub(crate) use manager::UserManager;
pub(crate) use model::{CurrentUser, UserView};
#[expect(unused_imports, reason = "used higher in the PR stack")]
pub(crate) use service::UserService;
