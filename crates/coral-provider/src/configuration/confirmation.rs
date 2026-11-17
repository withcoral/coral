//! What confirming a Setup's configuration decides, apart from reading and
//! writing it.
//!
//! [`check_revision`] settles the caller's revision against the stored one, and
//! [`transition`] names the state the Setup moves to and what the confirmation
//! invalidates. Both are pure: the confirmation loads their facts, and one
//! repository write applies the result.
use crate::ProviderSetupState;

/// How a confirmation stands against the stored configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RevisionCheck {
    /// Nothing to write: the stored configuration already is this one. The
    /// caller answers with the current review.
    Current,
    /// Another confirmation changed the Setup first.
    Stale,
    /// Write the configuration as this revision.
    Confirm { revision: i64 },
}

/// The facts [`check_revision`] decides from.
#[derive(Debug, Clone, Copy)]
pub(super) struct RevisionFacts<'a> {
    pub(super) stored_revision: i64,
    pub(super) expected_revision: i64,
    /// Whether the stored values equal the accepted ones.
    pub(super) saved: bool,
    pub(super) state: &'a ProviderSetupState,
    /// Whether the confirmation carries an application secret to store.
    pub(super) secret: bool,
}

/// Settles one confirmation's revision. The same values confirmed once more
/// are a replay, not a conflict, when they are one revision behind. Values
/// seeded for an Update stay unconfirmed until their first confirmation, so
/// that confirmation always writes.
pub(super) fn check_revision(facts: RevisionFacts<'_>) -> RevisionCheck {
    let confirmed = facts.saved && *facts.state != ProviderSetupState::AwaitingConfiguration;
    if facts.stored_revision != facts.expected_revision {
        return if confirmed && facts.expected_revision.checked_add(1) == Some(facts.stored_revision)
        {
            RevisionCheck::Current
        } else {
            RevisionCheck::Stale
        };
    }
    if confirmed && !facts.secret {
        return RevisionCheck::Current;
    }
    facts
        .expected_revision
        .checked_add(1)
        .map_or(RevisionCheck::Stale, |revision| RevisionCheck::Confirm {
            revision,
        })
}

/// How the Setup's catalog comes to be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Source {
    /// Pinned without sign-in: an OpenAPI document, or an MCP server that
    /// lists its Tools anonymously.
    Open,
    /// An MCP server that lists its Tools only after sign-in. `read` says
    /// whether sign-in has read and pinned them.
    SignIn { read: bool },
}

/// The facts [`transition`] decides from.
#[derive(Debug, Clone, Copy)]
pub(super) struct TransitionFacts<'a> {
    /// The Setup's state, read under the row hold.
    pub(super) state: &'a ProviderSetupState,
    pub(super) source: Source,
    /// Whether the request headers differ from the ones in effect.
    pub(super) headers_changed: bool,
    /// Whether the credential contract differs from the one in effect.
    pub(super) authority_changed: bool,
    /// Whether the confirmed contract names an OAuth application that
    /// authenticates with a secret.
    pub(super) confidential_application: bool,
}

/// What one confirmation changes besides the saved values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Transition {
    /// The state the Setup moves to.
    pub(super) state: ProviderSetupState,
    /// Drop the pinned input and its preview: sign-in read them with headers
    /// or a contract that the confirmation replaced.
    pub(super) clears_catalog: bool,
    /// End sign-in begun under the previous contract.
    pub(super) resets_credentials: bool,
    /// Delete the application secret the Setup holds: the confirmed contract
    /// has no application that uses one.
    pub(super) drops_application_secret: bool,
}

/// Decides where a confirmation leaves the Setup.
///
/// A source pinned without sign-in is ready once confirmed; a changed contract
/// only ends the sign-in begun under the old one. A source that sign-in reads
/// starts authorization over when the confirmation changes its headers or its
/// contract. A change that leaves both alone, such as the name, keeps a read
/// catalog ready and keeps an unread one waiting on sign-in or discovery,
/// whichever it was in; a first confirmation sends it to sign-in. Either
/// source keeps an application secret only while its contract uses one.
pub(super) fn transition(facts: TransitionFacts<'_>) -> Transition {
    let drops_application_secret = !facts.confidential_application;
    match facts.source {
        Source::Open => Transition {
            state: ProviderSetupState::Ready,
            clears_catalog: false,
            resets_credentials: facts.authority_changed,
            drops_application_secret,
        },
        Source::SignIn { read } => {
            let starts_over = facts.authority_changed || facts.headers_changed;
            let state = if starts_over
                || (!read && *facts.state == ProviderSetupState::AwaitingConfiguration)
            {
                ProviderSetupState::AwaitingAuthorization
            } else if read {
                ProviderSetupState::Ready
            } else {
                facts.state.clone()
            };
            Transition {
                state,
                clears_catalog: starts_over,
                resets_credentials: starts_over,
                drops_application_secret,
            }
        }
    }
}
