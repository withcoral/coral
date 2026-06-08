//! Trajectory-memory episodes: persist the intent (and parent) of a task-attempt,
//! keyed by a client-minted episode id, so the read side can join a task's queries
//! to the intent they served. Experimental — gated by the `episodes` feature (off
//! by default). The store and the `OpenEpisode` write path land here; the
//! `coral-episode-id` span attribution stacks on top.

pub(crate) mod id;
pub(crate) mod service;
pub(crate) mod store;

pub(crate) use id::EpisodeId;
