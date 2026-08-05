#![cfg_attr(
    not(test),
    expect(dead_code, reason = "wired to the UserService adapter in t14")
)]

mod manager;
mod model;

#[expect(unused_imports, reason = "wired to the UserService adapter in t14")]
pub(crate) use manager::UserManager;
pub(crate) use model::{CurrentUser, UserView};
