//! Database-backed installed identity-spec management.
#![cfg_attr(
    not(test),
    expect(dead_code, reason = "later stack layers wire consumers")
)]

mod fingerprint;
pub(crate) mod inputs;
pub(crate) mod manager;
pub(crate) mod service;

pub(crate) use fingerprint::identity_spec_fingerprint;
