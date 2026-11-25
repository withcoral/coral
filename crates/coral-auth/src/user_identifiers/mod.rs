//! The store for User Identifiers.
//!
//! A User Identifier is one name that the Identity Provider uses for a User.
//! The Coral User ID is the key of a User, and each identifier is a row that
//! points at it. One identifier belongs to at most one User, and a User holds
//! at most one of each kind. An identifier finds its User and grants no
//! authority.
//!
//! No sign-in reads or writes the store yet: the upgrade copies each Subject
//! into it, and a User made after the upgrade has no row.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the sign-in reads and writes the store in the next change"
    )
)]

mod repository;
#[cfg(test)]
mod tests;

use coral_db::{QueryExecutor, Transaction, UnixNanos};

use crate::{Error, UserId};

/// The kind of a [`UserIdentifier`]: the channel in which the Identity
/// Provider uses that name for a person.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum UserIdentifierKind {
    /// The `sub` of an OIDC sign-in.
    Subject,
    /// The value of a `NameID`: the name of a person at a sign-in that has no
    /// Subject.
    NameId,
}

/// One name that the Identity Provider uses for a User. The value is compared
/// by its bytes: no change of case, no trimming and no Unicode normalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UserIdentifier<'a> {
    kind: UserIdentifierKind,
    value: &'a str,
}

impl<'a> UserIdentifier<'a> {
    /// Names `value` as an identifier of `kind`.
    pub(crate) const fn new(kind: UserIdentifierKind, value: &'a str) -> Self {
        Self { kind, value }
    }

    pub(crate) const fn kind(self) -> UserIdentifierKind {
        self.kind
    }

    /// The value, as the Identity Provider sent it.
    pub(crate) const fn value(self) -> &'a str {
        self.value
    }
}

/// What the store did with a request to link an identifier to a User.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LinkOutcome {
    /// The store now holds the identifier for the User.
    Linked,
    /// The User held this identifier already.
    AlreadyLinked,
    /// Another User holds the identifier. Nothing changed. This is the answer
    /// also when the User holds another identifier of this kind.
    HeldBy(UserId),
    /// Nobody holds the identifier, and the User holds another one of this
    /// kind. Nothing changed.
    UserHoldsAnother,
}

/// Finds the User that holds `identifier`, by its exact value.
pub(crate) async fn find(
    executor: impl QueryExecutor,
    identifier: UserIdentifier<'_>,
) -> Result<Option<UserId>, Error> {
    repository::find(executor, identifier).await
}

/// Links `identifier` to `user_id` unless either uniqueness rule refuses it,
/// and says which case it met.
pub(crate) async fn link(
    transaction: &mut Transaction<'_>,
    identifier: UserIdentifier<'_>,
    user_id: &UserId,
    linked_at: UnixNanos,
) -> Result<LinkOutcome, Error> {
    if repository::insert(transaction, identifier, user_id, linked_at).await? {
        return Ok(LinkOutcome::Linked);
    }
    // The insert met a row of one of the two rules, and no row is ever
    // removed. So a holder of the identifier is read here, and with no holder
    // the row that refused the insert is the User's other identifier.
    Ok(
        match repository::find(&mut *transaction, identifier).await? {
            Some(holder) if &holder == user_id => LinkOutcome::AlreadyLinked,
            Some(holder) => LinkOutcome::HeldBy(holder),
            None => LinkOutcome::UserHoldsAnother,
        },
    )
}
