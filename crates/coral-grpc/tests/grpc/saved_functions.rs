use super::{Fixture, error_reason, management, request};
use coral_functions::{
    SavedFunctionChangeContext, SavedFunctionDescription, SavedFunctionPath, SavedFunctionSave,
    SavedFunctionSource,
};
use coral_workspace::{AuthenticatedPrincipal, AuthenticatedUser, TraceId};

async fn save(f: &Fixture, user: &AuthenticatedUser, path: &str, source: &str) {
    f.saved_functions
        .save(
            &SavedFunctionChangeContext::new(
                AuthenticatedPrincipal::user(user.clone()),
                1_700_000_000_000_000_007,
                TraceId::current_or_new(),
            ),
            f.workspace.id(),
            SavedFunctionSave::new(
                SavedFunctionPath::parse(path).unwrap(),
                SavedFunctionDescription::parse(&format!("Describe {path}")).unwrap(),
                SavedFunctionSource::parse(source).unwrap(),
            ),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn list_pages_round_trip_as_source_free_summaries_and_bind_their_token() {
    let f = Fixture::new().await;
    for path in ["reports.total", "alpha.first", "zeta.last"] {
        save(&f, &f.owner, path, "export default () => 1;").await;
    }
    let parent = format!("workspaces/{}", f.workspace.id());
    let list = async |user: &AuthenticatedUser, page_token: String| {
        f.saved_functions()
            .list_saved_functions(request(
                management::ListSavedFunctionsRequest {
                    parent: parent.clone(),
                    page_size: 2,
                    page_token,
                },
                &f.token(user, "reef"),
                "reef",
            ))
            .await
            .map(tonic::Response::into_inner)
    };

    let first = list(&f.owner, String::new()).await.unwrap();
    assert_eq!(
        first.saved_functions.first(),
        Some(&management::SavedFunctionSummary {
            path: "alpha.first".into(),
            description: "Describe alpha.first".into(),
            update_time: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 7,
            }),
        })
    );
    let second = list(&f.owner, first.next_page_token.clone()).await.unwrap();
    let paths = [first.saved_functions, second.saved_functions]
        .concat()
        .into_iter()
        .map(|summary| summary.path)
        .collect::<Vec<_>>();
    assert_eq!(paths, ["alpha.first", "reports.total", "zeta.last"]);
    assert!(second.next_page_token.is_empty());

    let foreign = list(&f.member, first.next_page_token).await.unwrap_err();
    assert_eq!(
        (foreign.code(), error_reason(&foreign).as_str()),
        (tonic::Code::InvalidArgument, "INVALID_ARGUMENT")
    );
}

#[tokio::test]
async fn get_returns_authoring_material_and_conceals_other_owners_as_not_found() {
    let f = Fixture::new().await;
    save(
        &f,
        &f.owner,
        "reports.total",
        "export default function total(): number { return missing; }",
    )
    .await;
    let get = async |user: &AuthenticatedUser, path: &str, audience: &str| {
        f.saved_functions()
            .get_saved_function(request(
                management::GetSavedFunctionRequest {
                    parent: format!("workspaces/{}", f.workspace.id()),
                    path: path.into(),
                },
                &f.token(user, audience),
                audience,
            ))
            .await
    };

    let function = get(&f.owner, "reports.total", "reef")
        .await
        .unwrap()
        .into_inner();
    assert_eq!(function.path, "reports.total");
    assert_eq!(function.description, "Describe reports.total");
    assert!(function.source.contains("return missing;"));
    assert!(function.declaration.contains("export default"));
    assert!(matches!(
        function.diagnostics.as_slice(),
        [diagnostic]
            if diagnostic.category() == management::SavedFunctionDiagnosticCategory::Error
                && diagnostic.message.contains("missing")
                && diagnostic.line == Some(1)
    ));
    assert!(!function.diagnostics_truncated);

    for (user, path, audience, expected) in [
        (
            &f.member,
            "reports.total",
            "reef",
            (tonic::Code::NotFound, "NOT_FOUND"),
        ),
        (
            &f.owner,
            "reports.absent",
            "reef",
            (tonic::Code::NotFound, "NOT_FOUND"),
        ),
        (
            &f.owner,
            "reports..total",
            "reef",
            (tonic::Code::InvalidArgument, "INVALID_ARGUMENT"),
        ),
        (
            &f.owner,
            "reports.total",
            "mcp",
            (tonic::Code::PermissionDenied, "PERMISSION_DENIED"),
        ),
    ] {
        let status = get(user, path, audience).await.unwrap_err();
        assert_eq!(
            (status.code(), error_reason(&status).as_str()),
            expected,
            "{path} via {audience}"
        );
    }
}

#[tokio::test]
async fn delete_removes_only_the_callers_own_function() {
    let f = Fixture::new().await;
    save(&f, &f.owner, "reports.total", "export default () => 1;").await;
    let parent = format!("workspaces/{}", f.workspace.id());
    let delete = async |user: &AuthenticatedUser, path: &str, audience: &str| {
        f.saved_functions()
            .delete_saved_function(request(
                management::DeleteSavedFunctionRequest {
                    parent: parent.clone(),
                    path: path.into(),
                },
                &f.token(user, audience),
                audience,
            ))
            .await
    };
    let exists = async || {
        f.saved_functions
            .get(
                &AuthenticatedPrincipal::user(f.owner.clone()),
                f.workspace.id(),
                &SavedFunctionPath::parse("reports.total").unwrap(),
            )
            .await
            .is_ok()
    };

    for (user, path, audience, expected) in [
        (
            &f.member,
            "reports.total",
            "reef",
            (tonic::Code::NotFound, "NOT_FOUND"),
        ),
        (
            &f.owner,
            "reports..total",
            "reef",
            (tonic::Code::InvalidArgument, "INVALID_ARGUMENT"),
        ),
        (
            &f.owner,
            "reports.total",
            "mcp",
            (tonic::Code::PermissionDenied, "PERMISSION_DENIED"),
        ),
    ] {
        let status = delete(user, path, audience).await.unwrap_err();
        assert_eq!(
            (status.code(), error_reason(&status).as_str()),
            expected,
            "{path} via {audience}"
        );
        assert!(exists().await, "a refused delete keeps the function");
    }

    delete(&f.owner, "reports.total", "reef").await.unwrap();
    assert!(!exists().await);
    let again = delete(&f.owner, "reports.total", "reef").await.unwrap_err();
    assert_eq!(
        (again.code(), error_reason(&again).as_str()),
        (tonic::Code::NotFound, "NOT_FOUND")
    );
}
