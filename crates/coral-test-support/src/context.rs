//! Change attribution fixtures.
use coral_workspace::{AuthenticatedUser, ChangeContext, TraceId};

/// A change by `actor` at `at` nanoseconds, traced by `digit` repeated 32 times.
///
/// # Panics
/// Panics when `digit` is not a lowercase hexadecimal digit.
#[must_use]
pub fn change(actor: AuthenticatedUser, at: i64, digit: char) -> ChangeContext {
    ChangeContext::new(actor, at, trace(digit))
}

/// A trace ID of `digit` repeated 32 times.
///
/// # Panics
/// Panics when `digit` is not a lowercase hexadecimal digit.
#[must_use]
pub fn trace(digit: char) -> TraceId {
    TraceId::parse(&digit.to_string().repeat(32)).expect("fixture trace id")
}
