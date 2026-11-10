use super::{ProviderCallRoot, SearchInput, SearchRequest};
use coral_catalog::SEARCH_PROVIDERS_MAX;

use crate::Error;

// Catches: a bare `workspace` selecting the Built-in Provider instead of an
// Owner's Provider named `workspace`, a complete Call Root such as
// `coral.providers.github` selecting its Provider instead of being refused
// with the namespaces to use, the old root `coral.workspace` or the Private
// or Shared Provider's root selecting anything, or a filter past the bound.
#[test]
fn the_provider_filter_takes_namespaces_or_coral() {
    let github = ProviderCallRoot::parse("coral.providers.github").unwrap();
    let workspace = ProviderCallRoot::parse("coral.providers.workspace").unwrap();
    let roots = [&ProviderCallRoot::Builtin, &github, &workspace];
    for (values, admitted, unmatched) in [
        (
            &[][..],
            &[
                "coral",
                "coral.providers.github",
                "coral.providers.workspace",
            ][..],
            &[][..],
        ),
        (&["github"], &["coral.providers.github"], &[]),
        (&["workspace"], &["coral.providers.workspace"], &[]),
        (
            &["coral", "github"],
            &["coral", "coral.providers.github"],
            &[],
        ),
        (
            &["coral.providers.github", "then", "github"],
            &["coral.providers.github"],
            &[0, 1],
        ),
        (
            &["coral.workspace", "coral.private", "coral.shared", "coral"],
            &["coral"],
            &[0, 1, 2],
        ),
    ] {
        let request = SearchRequest::try_from(SearchInput {
            provider: values.iter().map(|value| (*value).to_owned()).collect(),
            ..SearchInput::default()
        })
        .unwrap();
        let filter = request.provider();
        assert_eq!(
            roots
                .iter()
                .filter(|root| filter.admits(root))
                .map(|root| root.as_str())
                .collect::<Vec<_>>(),
            admitted,
            "{values:?}"
        );
        assert_eq!(filter.unmatched(&roots), unmatched, "{values:?}");
    }
    let too_many = SearchRequest::try_from(SearchInput {
        provider: vec!["github".to_owned(); SEARCH_PROVIDERS_MAX + 1],
        ..SearchInput::default()
    });
    assert!(
        matches!(
            &too_many,
            Err(Error::InvalidInput(message)) if message == "provider names at most 32 namespaces"
        ),
        "{too_many:?}"
    );
}
