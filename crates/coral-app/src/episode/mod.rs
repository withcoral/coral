//! Trajectory-memory episodes: persist the intent (and parent) of a task-attempt,
//! keyed by a client-minted episode id, so the read side can join a task's queries
//! to the intent they served. Experimental — gated by the `episodes` feature (off
//! by default). PR 1 lands the store; the `OpenEpisode` handler and the
//! `coral-episode-id` span attribution stack on top.

pub(crate) mod id;
mod store;

pub(crate) use id::EpisodeId;
