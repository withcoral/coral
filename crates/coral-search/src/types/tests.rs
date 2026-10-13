use super::{ProviderCallRoot, SearchInput, SearchRequest};
use crate::Error;

// Catches: an agent that filters by a complete Call Root, such as
// `coral.providers.github`, getting a rejection that does not name the
// namespace to use, or a bare `admin` selecting the Built-in Provider instead
// of an Owner's Provider named `admin`.
#[test]
fn the_provider_filter_takes_a_namespace_or_coral_admin() {
    for (provider, expected) in [
        ("github", Ok("coral.providers.github")),
        ("admin", Ok("coral.providers.admin")),
        ("coral.admin", Ok("coral.admin")),
        (
            "coral.providers.github",
            Err("provider must be a Provider namespace, such as `github`"),
        ),
        ("coral..github", Err("invalid Provider namespace")),
        ("then", Err("invalid Provider namespace")),
    ] {
        let request = SearchRequest::try_from(SearchInput {
            provider: Some(provider.to_string()),
            ..SearchInput::default()
        });
        match (request, expected) {
            (Ok(request), Ok(root)) => assert_eq!(
                request.provider.as_ref().map(ProviderCallRoot::as_str),
                Some(root),
                "{provider}"
            ),
            (Err(Error::InvalidInput(message)), Err(expected)) => {
                assert_eq!(message, expected, "{provider}");
            }
            (outcome, _) => panic!("{provider}: {outcome:?}"),
        }
    }
}
