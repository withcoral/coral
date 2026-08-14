//! The deployment's user directory.
//!
//! A directory row exists so one person can be named by another — as the owner
//! of a workspace, or as somebody to invite into one. That is the whole of its
//! product purpose, so the domain exposes only the internal user id and the
//! display name. The issuer and subject that identify the same person upstream
//! stay inside `state/db`, where login provisioning writes them.

pub(crate) mod manager;
pub(crate) mod model;
