//! `discover` with exact Call Paths: several at once, with the ones the
//! caller cannot discover reported missing. The visibility and abridging
//! rules are tested at `describe` and `signature::abridge`.

use super::{TestModules, TracerScope};
use crate::{Discovery, DiscoveryRequest, Error, ExactLookup, ExactPaths};
use coral_catalog::{SEARCH_PATH_MAX_BYTES, SEARCH_PATHS_MAX};
use coral_policy::ToolAccessState;
use coral_test_support::change;

async fn look_up(fixture: &TestModules, scope: &TracerScope, paths: &[&str]) -> ExactLookup {
    let paths = ExactPaths::new(paths.iter().map(ToString::to_string).collect(), Vec::new())
        .expect("a bounded lookup");
    let found = fixture
        .search
        .discover(
            &scope.principal,
            &scope.workspace_id,
            &DiscoveryRequest::Paths(paths),
        )
        .await
        .expect("an exact lookup");
    let Discovery::Exact(lookup) = found else {
        panic!("paths return an exact lookup: {found:?}");
    };
    lookup
}

// Catches: a lookup that fails as a whole for one path it cannot describe,
// tells a disabled Tool from an absent one, or describes a path otherwise
// than `describe` does.
#[tokio::test]
async fn a_lookup_describes_each_path_it_can_and_reports_the_rest_missing_in_order() {
    let fixture = TestModules::new().await;
    let scope = TracerScope::new(&fixture).await;
    let tool = scope.describe(&fixture, TracerScope::TOOL).await.unwrap();
    let function = scope
        .describe(&fixture, TracerScope::FUNCTION)
        .await
        .unwrap();
    let absent = "coral.providers.weather.getForecasts";
    let malformed = "coral.providers.weather.get-forecast";

    let lookup = look_up(
        &fixture,
        &scope,
        &[absent, TracerScope::TOOL, malformed, TracerScope::FUNCTION],
    )
    .await;

    assert_eq!(lookup.descriptions(), [tool, function.clone()]);
    assert_eq!(
        lookup
            .missing()
            .iter()
            .map(crate::MissingPath::call_path)
            .collect::<Vec<_>>(),
        [absent, malformed]
    );

    fixture
        .policies
        .set_access_state(
            &change(scope.user.clone(), 50, '5'),
            scope.published.policy_target.clone(),
            ToolAccessState::Disabled,
        )
        .await
        .expect("disable the tracer Tool");
    let lookup = look_up(
        &fixture,
        &scope,
        &[TracerScope::TOOL, TracerScope::FUNCTION],
    )
    .await;
    assert_eq!(lookup.descriptions(), [function]);
    assert_eq!(
        lookup
            .missing()
            .iter()
            .map(crate::MissingPath::call_path)
            .collect::<Vec<_>>(),
        [TracerScope::TOOL]
    );
}

// Catches: a lookup bound that the Built-in Tool's contract states but the
// operation does not hold, or one that refuses a lookup at its limit.
#[test]
fn a_lookup_is_bounded_before_it_reads_anything() {
    let path = |bytes: usize| format!("coral.functions.{}", "a".repeat(bytes - 16));
    let names = |count: usize| vec!["Repository".to_owned(); count];
    for (story, paths, types, accepted) in [
        ("no path", Vec::new(), Vec::new(), false),
        (
            "the most paths",
            vec![path(20); SEARCH_PATHS_MAX],
            Vec::new(),
            true,
        ),
        (
            "too many paths",
            vec![path(20); SEARCH_PATHS_MAX + 1],
            Vec::new(),
            false,
        ),
        (
            "the longest path",
            vec![path(SEARCH_PATH_MAX_BYTES)],
            Vec::new(),
            true,
        ),
        (
            "a path too long",
            vec![path(SEARCH_PATH_MAX_BYTES + 1)],
            Vec::new(),
            false,
        ),
        ("types for one path", vec![path(20)], names(1), true),
        ("types for two paths", vec![path(20); 2], names(1), false),
    ] {
        let bounded = ExactPaths::new(paths, types);
        assert_eq!(bounded.is_ok(), accepted, "{story}: {bounded:?}");
        if !accepted {
            assert!(
                matches!(bounded, Err(Error::InvalidInput(_))),
                "{story}: {bounded:?}"
            );
        }
    }
    assert!(matches!(
        ExactPaths::new(vec![path(20)], names(crate::DESCRIBE_TYPES_MAX + 1)),
        Err(Error::TypesLimit)
    ));
}
