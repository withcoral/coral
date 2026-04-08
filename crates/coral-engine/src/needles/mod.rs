//! Benchmark needle planting: unions synthetic rows with live provider data.
//!
//! This implements a "needle in a haystack" evaluation pattern: a benchmark
//! harness writes a YAML file of synthetic rows, and when `CORAL_NEEDLES_FILE`
//! is set the engine converts matching entries to Arrow batches at source
//! registration time, wrapping each affected table provider with
//! [`provider::NeedleTableProvider`].

pub(crate) mod error;
pub(crate) mod loader;
pub(crate) mod provider;
