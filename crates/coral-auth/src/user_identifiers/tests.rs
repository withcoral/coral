use coral_db::UnixNanos;
use coral_test_support::TestDatabase;

use super::{LinkOutcome, UserIdentifier, UserIdentifierKind};
use crate::{LoginProvisioning, UserDirectory, UserId, VerifiedLogin};

const NOW: UnixNanos = UnixNanos::new(1_800_000_000_000_000_000);
pub(crate) const ISSUER: &str = "https://issuer.test";
/// The version of the first of the store's two steps. It makes
/// `users.issuer` and `users.subject` optional, and the next step adds the
/// table and copies each Subject.
const FIRST_STORE_MIGRATION_VERSION: i64 = 88;

pub(crate) const fn subject(value: &str) -> UserIdentifier<'_> {
    UserIdentifier::new(UserIdentifierKind::Subject, value)
}

pub(crate) const fn name_id(value: &str) -> UserIdentifier<'_> {
    UserIdentifier::new(UserIdentifierKind::NameId, value)
}

/// The ID of a User that holds no identifier.
fn new_user_id() -> UserId {
    UserId::parse(&uuid::Uuid::new_v4().to_string()).unwrap()
}

async fn link(
    fixture: &TestDatabase,
    identifier: UserIdentifier<'_>,
    user_id: &UserId,
) -> LinkOutcome {
    let database = fixture.database();
    let mut transaction = database.begin().await.unwrap();
    let outcome = super::link(&mut transaction, identifier, user_id, NOW)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    outcome
}

async fn holder(fixture: &TestDatabase, identifier: UserIdentifier<'_>) -> Option<UserId> {
    super::find(&*fixture.database(), identifier).await.unwrap()
}

/// Asserts the holder of each identifier, or that it has none.
pub(crate) async fn assert_holders(
    fixture: &TestDatabase,
    expected: &[(UserIdentifier<'_>, Option<&UserId>)],
) {
    for (identifier, expected) in expected {
        assert_eq!(
            holder(fixture, *identifier).await.as_ref(),
            *expected,
            "{identifier:?}"
        );
    }
}

/// One identifier belongs to at most one User, and a User holds at most one
/// of each kind. A refused link changes nothing.
#[tokio::test]
async fn a_link_keeps_both_uniqueness_rules() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let ana = new_user_id();
    let ben = new_user_id();

    let cases = [
        (
            "a new identifier",
            name_id("ana-1"),
            &ana,
            LinkOutcome::Linked,
        ),
        (
            "the same link again",
            name_id("ana-1"),
            &ana,
            LinkOutcome::AlreadyLinked,
        ),
        (
            "an identifier that another User holds",
            name_id("ana-1"),
            &ben,
            LinkOutcome::HeldBy(ana.clone()),
        ),
        (
            "a second identifier of one kind",
            name_id("ana-2"),
            &ana,
            LinkOutcome::UserHoldsAnother,
        ),
        // Had the link that was refused for this User written a row, this
        // one would be refused too.
        (
            "the first identifier of a User whose link was refused",
            name_id("ben-1"),
            &ben,
            LinkOutcome::Linked,
        ),
        (
            "an identifier that another User holds, for a User who holds one of the kind",
            name_id("ana-1"),
            &ben,
            LinkOutcome::HeldBy(ana.clone()),
        ),
        (
            "the same value under another kind",
            subject("ana-1"),
            &ben,
            LinkOutcome::Linked,
        ),
    ];
    for (case, identifier, user_id, expected) in cases {
        assert_eq!(
            link(&fixture, identifier, user_id).await,
            expected,
            "{case}"
        );
    }

    // No refused link left a row: each identifier has the holder that a
    // `Linked` answer gave it, or none.
    assert_holders(
        &fixture,
        &[
            (name_id("ana-1"), Some(&ana)),
            (name_id("ana-2"), None),
            (name_id("ben-1"), Some(&ben)),
            (subject("ana-1"), Some(&ben)),
            (subject("ben-1"), None),
        ],
    )
    .await;
}

/// The lookup and the uniqueness rule compare a value by its bytes, on both
/// databases: a value that only looks like a held one is another identifier.
#[tokio::test]
async fn a_value_is_compared_by_its_bytes() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let held = "José";
    let first = new_user_id();
    assert_eq!(
        link(&fixture, name_id(held), &first).await,
        LinkOutcome::Linked
    );

    let lookalikes = [
        ("another case", "josé"),
        ("a trailing space", "José "),
        ("a leading space", " José"),
        ("a decomposed accent", "Jose\u{301}"),
    ];
    for (case, value) in lookalikes {
        assert_eq!(
            holder(&fixture, name_id(value)).await,
            None,
            "{case} finds nobody"
        );
        let other = new_user_id();
        assert_eq!(
            link(&fixture, name_id(value), &other).await,
            LinkOutcome::Linked,
            "{case} is its own identifier"
        );
        assert_eq!(
            holder(&fixture, name_id(value)).await,
            Some(other),
            "{case}"
        );
    }
    assert_eq!(holder(&fixture, name_id(held)).await, Some(first));
}

/// The two steps of the store keep each User and copy its Subject: a User
/// from before the upgrade is found through the store, and by the sign-in.
#[tokio::test]
async fn the_upgrade_keeps_each_user_and_copies_its_subject_into_the_store() {
    let before = coral_test_support::migration_plan_before(FIRST_STORE_MIGRATION_VERSION);
    let fixture = TestDatabase::new(&before).await.unwrap();
    let ana = insert_user_as_the_release_before(&fixture, "ana").await;
    let ben = insert_user_as_the_release_before(&fixture, "ben").await;

    fixture.upgrade().await.unwrap();

    assert_holders(
        &fixture,
        &[
            (subject("ana"), Some(&ana)),
            (subject("ben"), Some(&ben)),
            (subject("cleo"), None),
            (name_id("ana"), None),
        ],
    )
    .await;
    let login = VerifiedLogin::oidc(ISSUER, "ana", None, NOW);
    assert_eq!(
        UserDirectory::new(fixture.database())
            .provision_login(login)
            .await
            .unwrap(),
        LoginProvisioning::Provisioned(ana)
    );
}

/// Two links of one identifier to two Users that arrive together: one is
/// linked, and the other reads the holder and changes nothing.
#[tokio::test]
async fn two_links_together_leave_one_holder() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let ana = new_user_id();
    let ben = new_user_id();
    let outcomes = tokio::join!(
        link(&fixture, name_id("joy"), &ana),
        link(&fixture, name_id("joy"), &ben),
    );
    let winner = match outcomes {
        (LinkOutcome::Linked, LinkOutcome::HeldBy(holder))
        | (LinkOutcome::HeldBy(holder), LinkOutcome::Linked) => holder,
        outcomes => panic!("expected one link and one holder, got {outcomes:?}"),
    };
    assert_holders(&fixture, &[(name_id("joy"), Some(&winner))]).await;
}

/// Writes a `users` row as the release before this one does, and no row in
/// the store.
pub(crate) async fn insert_user_as_the_release_before(
    fixture: &TestDatabase,
    oidc_subject: &str,
) -> UserId {
    let user_id = new_user_id();
    fixture
        .execute_fixture_sql(&format!(
            "INSERT INTO users (user_id, issuer, subject, display_name, \
             created_at_unix_nanos, last_login_at_unix_nanos) \
             VALUES ('{}', '{ISSUER}', '{oidc_subject}', NULL, 1, 1)",
            user_id.as_str()
        ))
        .await
        .unwrap();
    user_id
}
