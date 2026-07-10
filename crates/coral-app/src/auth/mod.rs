#[expect(dead_code, reason = "wired later in the serving stack")]
pub(crate) mod session;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "wired later in the OAuth serving stack")
)]
pub(crate) mod state_store;
