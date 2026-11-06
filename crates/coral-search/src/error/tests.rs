use super::{DECLARED_NAMES_SHOWN, Error, undeclared_types_message};
use crate::ProviderCallRoot;

// Catches: the refusal listing namespaces in Call Root order, which differs
// from the namespace order `exec`'s description lists them in.
#[test]
fn an_unknown_provider_filter_is_refused_with_only_the_searchable_namespaces() {
    for (searchable, message) in [
        (
            &["coral", "coral.providers.github"][..],
            "No Provider you can search has this namespace. Use one of: coral, github.",
        ),
        (
            &[],
            "No Provider you can search has this namespace. You can search no Provider.",
        ),
    ] {
        let error = Error::UnknownProviderCallRoot {
            searchable: searchable
                .iter()
                .map(|root| ProviderCallRoot::parse(root).unwrap())
                .collect(),
            unmatched: vec![0],
        };
        assert_eq!(
            error.safe_rejection(),
            Some(("invalid_input", message.to_string())),
            "{searchable:?}"
        );
    }
}

// Catches: a lookup failure that every later request repeats being served
// as a missing list, or a passing fault (an archived Workspace, a corrupt row)
// denying the whole request.
#[test]
fn only_a_lost_workspace_counts_as_lost_authority() {
    for (error, lost) in [
        (Error::PermissionDenied("discovery"), true),
        (
            Error::Workspace(coral_workspace::Error::WorkspaceNotFound),
            true,
        ),
        (
            Error::Workspace(coral_workspace::Error::PermissionDenied("inspect")),
            true,
        ),
        (
            Error::Workspace(coral_workspace::Error::AgentNotFound),
            true,
        ),
        (
            Error::Workspace(coral_workspace::Error::InvalidAgentCredential),
            true,
        ),
        (
            Error::Workspace(coral_workspace::Error::AgentDisabled),
            true,
        ),
        (
            Error::Workspace(coral_workspace::Error::AgentRegistrationExpired),
            true,
        ),
        (
            Error::Workspace(coral_workspace::Error::WorkspaceArchived),
            false,
        ),
        (
            Error::Workspace(coral_workspace::Error::CorruptData("row".to_owned())),
            false,
        ),
        (Error::ServiceUnavailable, false),
        (Error::InvalidInput("query".to_owned()), false),
    ] {
        assert_eq!(error.lost_workspace_authority(), lost, "{error:?}");
    }
}

#[test]
fn an_undeclared_types_message_names_the_missing_types_and_a_bounded_declared_list() {
    let names = |count: usize| {
        (0..count)
            .map(|index| format!("T{index}"))
            .collect::<Vec<_>>()
    };
    let many = names(DECLARED_NAMES_SHOWN + 3);
    let shown = many
        .iter()
        .take(DECLARED_NAMES_SHOWN)
        .map(|name| format!("\"{name}\""))
        .collect::<Vec<_>>()
        .join(", ");
    for (undeclared, declared, expected) in [
        (
            vec!["Repo".to_string()],
            vec!["Issue".to_string(), "Repository".to_string()],
            r#"Type "Repo" is not declared by this path. It declares: "Issue", "Repository"."#
                .to_string(),
        ),
        (
            vec!["A".to_string(), "B".to_string()],
            Vec::new(),
            r#"Types "A", "B" are not declared by this path. Coral cannot list the types it declares."#.to_string(),
        ),
        (
            vec!["Repo".to_string()],
            many,
            format!(
                r#"Type "Repo" is not declared by this path. It declares: {shown}, and 3 more."#
            ),
        ),
    ] {
        assert_eq!(undeclared_types_message(&undeclared, &declared), expected);
    }
}
