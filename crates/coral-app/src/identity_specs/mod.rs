//! Database-backed installed identity-spec management.
#![cfg_attr(
    not(test),
    expect(dead_code, reason = "later identity units consume resolution APIs")
)]
mod fingerprint;
pub(crate) mod manager;
pub(crate) mod service;

#[expect(unused_imports, reason = "B4f2 consumes the durable fingerprint API.")]
pub(crate) use fingerprint::identity_spec_fingerprint;
pub(crate) use service::IdentitySpecService;
