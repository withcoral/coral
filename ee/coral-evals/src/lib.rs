//! Commercial live-eval helpers for Coral.

#[cfg(test)]
use arrow as _;
#[cfg(test)]
use coral_spec as _;

mod needles;

pub use needles::{CORAL_NEEDLES_FILE, NeedleInjectionConfig};
