//! Database-backed installed identity-spec management.
#![cfg_attr(
    not(test),
    expect(dead_code, reason = "later stack layers wire consumers")
)]

pub(crate) mod inputs;
pub(crate) mod manager;
