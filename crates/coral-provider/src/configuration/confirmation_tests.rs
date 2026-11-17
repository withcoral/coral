use super::confirmation::{
    RevisionCheck, RevisionFacts, Source, Transition, TransitionFacts, check_revision, transition,
};
use crate::ProviderSetupState::{
    AwaitingAuthorization, AwaitingConfiguration, AwaitingDiscovery, Ready,
};

#[test]
fn a_confirmation_writes_only_what_the_stored_revision_does_not_already_hold() {
    let confirm = |revision| RevisionCheck::Confirm { revision };
    // (stored revision, expected revision, saved, state, secret, request)
    let cases = [
        // At the stored revision: new values, a secret, or values seeded for
        // an Update and never confirmed, each write the next revision.
        (3, 3, false, Ready, false, confirm(4)),
        (3, 3, true, Ready, false, RevisionCheck::Current),
        (3, 3, true, Ready, true, confirm(4)),
        (1, 1, true, AwaitingConfiguration, false, confirm(2)),
        // One revision behind: only a replay of the confirmation that made
        // the stored revision is not a conflict, with or without a secret.
        (
            4,
            3,
            true,
            AwaitingAuthorization,
            false,
            RevisionCheck::Current,
        ),
        (4, 3, true, Ready, true, RevisionCheck::Current),
        (4, 3, false, Ready, false, RevisionCheck::Stale),
        (
            4,
            3,
            true,
            AwaitingConfiguration,
            false,
            RevisionCheck::Stale,
        ),
        // Any other distance, and a revision with no successor.
        (5, 3, true, Ready, false, RevisionCheck::Stale),
        (3, 4, true, Ready, false, RevisionCheck::Stale),
        (
            i64::MAX,
            i64::MAX,
            false,
            Ready,
            false,
            RevisionCheck::Stale,
        ),
    ];
    for (stored_revision, expected_revision, saved, state, secret, expected) in cases {
        let facts = RevisionFacts {
            stored_revision,
            expected_revision,
            saved,
            state: &state,
            secret,
        };
        assert_eq!(check_revision(facts), expected, "{facts:?}");
    }
}

#[test]
fn a_confirmation_moves_the_setup_by_what_it_changed() {
    const READ: Source = Source::SignIn { read: true };
    const UNREAD: Source = Source::SignIn { read: false };
    // What the confirmation changed: (headers, contract).
    const NAME: (bool, bool) = (false, false);
    const HEADERS: (bool, bool) = (true, false);
    const CONTRACT: (bool, bool) = (false, true);
    // What it invalidates: (catalog, credentials).
    const NOTHING: (bool, bool) = (false, false);
    const SIGN_IN: (bool, bool) = (false, true);
    const CATALOG_AND_SIGN_IN: (bool, bool) = (true, true);
    // (source, state, changed, next state, invalidated)
    let cases = [
        // Pinned without sign-in: ready once confirmed.
        (Source::Open, AwaitingConfiguration, NAME, Ready, NOTHING),
        (Source::Open, Ready, HEADERS, Ready, NOTHING),
        (Source::Open, Ready, CONTRACT, Ready, SIGN_IN),
        // Read by sign-in: new headers or a new contract start it over.
        (READ, Ready, NAME, Ready, NOTHING),
        (READ, AwaitingConfiguration, NAME, Ready, NOTHING),
        (
            READ,
            Ready,
            HEADERS,
            AwaitingAuthorization,
            CATALOG_AND_SIGN_IN,
        ),
        (
            READ,
            Ready,
            CONTRACT,
            AwaitingAuthorization,
            CATALOG_AND_SIGN_IN,
        ),
        // Not read yet: a first confirmation asks for sign-in, and a change
        // that leaves authority alone keeps the wait it was in.
        (
            UNREAD,
            AwaitingConfiguration,
            NAME,
            AwaitingAuthorization,
            NOTHING,
        ),
        (
            UNREAD,
            AwaitingAuthorization,
            NAME,
            AwaitingAuthorization,
            NOTHING,
        ),
        (UNREAD, AwaitingDiscovery, NAME, AwaitingDiscovery, NOTHING),
        (
            UNREAD,
            AwaitingDiscovery,
            HEADERS,
            AwaitingAuthorization,
            CATALOG_AND_SIGN_IN,
        ),
        (
            UNREAD,
            AwaitingDiscovery,
            CONTRACT,
            AwaitingAuthorization,
            CATALOG_AND_SIGN_IN,
        ),
    ];
    for (source, state, changed, next, invalidated) in cases {
        let (headers_changed, authority_changed) = changed;
        let (clears_catalog, resets_credentials) = invalidated;
        // The application secret follows the confirmed contract alone.
        for confidential_application in [false, true] {
            let facts = TransitionFacts {
                state: &state,
                source,
                headers_changed,
                authority_changed,
                confidential_application,
            };
            let expected = Transition {
                state: next.clone(),
                clears_catalog,
                resets_credentials,
                drops_application_secret: !confidential_application,
            };
            assert_eq!(transition(facts), expected, "{facts:?}");
        }
    }
}
