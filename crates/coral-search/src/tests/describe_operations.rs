//! The batch describe that types `coral.providers` and `coral.workspace`
//! keeps `describe`'s visibility.

use coral_policy::OperationAccessState;
use coral_test_support::change;
use coral_workspace::AuthenticatedPrincipal;

use crate::CallPath;

use super::{TestModules, TracerScope, organization_user};

fn paths(paths: &[&str]) -> Vec<CallPath> {
    paths
        .iter()
        .map(|path| CallPath::parse(path).expect("Call Path"))
        .collect()
}

/// Only a discoverable Operation is described. A missing Operation and an
/// Operation the caller cannot discover are absent alike.
#[tokio::test]
async fn a_batch_describes_only_what_the_caller_can_describe() {
    let fixture = TestModules::new().await;
    let scope = TracerScope::new(&fixture).await;
    let outsider = AuthenticatedPrincipal::user(
        organization_user(fixture.search.database.clone(), "user:outsider").await,
    );
    let requested = paths(&[
        TracerScope::OPERATION,
        "coral.providers.weather.missing",
        "coral.providers.hidden.getForecast",
    ]);

    let visible = fixture
        .search
        .describe_operations(&scope.principal, &scope.workspace_id, &requested)
        .await
        .expect("describe visible Operations");
    let described = scope
        .describe(&fixture, TracerScope::OPERATION)
        .await
        .expect("describe the Operation");
    let crate::AuthoringDescription::Operation(described) = described.authoring() else {
        panic!("an Operation path describes an Operation");
    };
    assert_eq!(
        visible,
        std::slice::from_ref(described),
        "the tracer's declaration fits describe's budget whole"
    );

    let foreign = fixture
        .search
        .describe_operations(&outsider, &scope.workspace_id, &requested)
        .await
        .expect("an outsider's read is concealed, not refused");
    assert!(foreign.is_empty(), "{foreign:?}");

    fixture
        .policies
        .set_access_state(
            &change(scope.user.clone(), 50, '5'),
            scope.published.policy_target.clone(),
            OperationAccessState::Disabled,
        )
        .await
        .expect("disable tracer Operation");
    let disabled = fixture
        .search
        .describe_operations(&scope.principal, &scope.workspace_id, &requested)
        .await
        .expect("read after policy change");
    assert!(disabled.is_empty(), "{disabled:?}");
}
