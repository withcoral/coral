use coral_catalog::OperationHints;
use serde_json::json;

#[test]
fn hints_project_only_what_the_provider_declared() {
    for (hints, expected) in [
        (OperationHints::default(), json!({})),
        (
            OperationHints::new(Some(true), None, None, None),
            json!({ "read_only": true }),
        ),
        (
            OperationHints::new(Some(false), Some(true), Some(false), Some(true)),
            json!({
                "read_only": false,
                "destructive": true,
                "idempotent": false,
                "open_world": true,
            }),
        ),
        (
            OperationHints::new(None, None, Some(true), Some(false)),
            json!({ "idempotent": true, "open_world": false }),
        ),
    ] {
        assert_eq!(super::hints(hints), expected, "{hints:?}");
    }
}
