//! Transport-free directory views.

/// One directory user as everybody but their identity provider sees them.
///
/// These two fields are the whole of the client-visible directory. The stored
/// row also carries the issuer and subject that authenticate this person, and
/// projecting onto this type is where those are dropped: a directory read says
/// who a caller may name in a membership, never where anybody signs in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct User {
    pub(crate) user_id: String,
    pub(crate) display_name: Option<String>,
}
