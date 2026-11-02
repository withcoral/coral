//! The nearest Discoverable Call Paths to a path that names nothing the
//! caller can call, so a Program can correct a guessed path without a
//! separate search. Only Tools of the path's own Call Root are suggested,
//! and only ones the caller can already discover.

use std::collections::BTreeSet;
use std::sync::Arc;

use coral_db::Transaction;

use crate::retriever::{
    EntityRetriever, QueryError, QueryRequest, RequestContext, abandon_query, begin_query,
    end_query,
};
use crate::{EntityKind, Error, ProviderCallRoot, normalize_query};

/// The most Call Paths one missing path names.
pub const SUGGESTIONS_MAX: usize = 3;

/// Words that change no name's meaning, so `retrievePage` and
/// `retrieveAPage` name the same words.
const FILLER_WORDS: [&str; 3] = ["a", "an", "the"];

/// The shortest word that matches a longer one it starts.
const SHORTEST_WORD_STEM: usize = 3;

/// Up to [`SUGGESTIONS_MAX`] Call Paths near `written`, nearest first:
/// each Tool of its Call Root whose last segment has the same words, in
/// Call Path order, then the Tools a ranked query of its words under that
/// root finds: first those whose own path shares a word with `written`,
/// then the rest, each in rank order. None when the root is not one the
/// caller can search.
///
/// The ranked query reads every text of a Tool, its schema text too, so a
/// Tool whose types merely mention a word can rank first. A path names a
/// Tool, so a Tool named with one of its words is nearer.
///
/// The exact match reads the catalog, not the index, so it still answers
/// while ranking is down; a ranking failure leaves only the exact matches.
pub(crate) async fn suggestions(
    transaction: &mut Transaction<'_>,
    context: RequestContext<'_>,
    retrievers: &[Arc<dyn EntityRetriever>],
    written: &str,
) -> Result<Vec<String>, Error> {
    let Some((call_root, relative_path)) = ProviderCallRoot::split_call_path(written) else {
        return Ok(Vec::new());
    };
    let Some(tools) = retrievers
        .iter()
        .find(|retriever| retriever.kind() == EntityKind::ProviderTool)
    else {
        return Ok(Vec::new());
    };
    let listing = match tools.list(transaction, context, Some(&call_root)).await {
        Ok(listing) => listing,
        Err(Error::UnknownProviderCallRoot { .. }) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let leaf = leaf_words(relative_path);
    let mut exact = listing
        .entities
        .iter()
        .map(|entity| entity.call_path().as_str())
        .filter(|path| {
            *path != written
                && ProviderCallRoot::split_call_path(path)
                    .is_some_and(|(_root, relative)| leaf_words(relative) == leaf)
        })
        .map(str::to_owned)
        .collect::<Vec<_>>();
    exact.sort();
    let mut suggested = exact;
    suggested.truncate(SUGGESTIONS_MAX);
    if suggested.len() == SUGGESTIONS_MAX {
        return Ok(suggested);
    }
    let query = normalize_query(&words(relative_path).join(" "));
    if query.text.is_empty() {
        return Ok(suggested);
    }
    let ranking = RequestContext {
        terms: Some(&query.terms),
        ..context
    };
    begin_query(transaction).await?;
    let window = match tools
        .query(
            transaction,
            ranking,
            &QueryRequest {
                terms: &query.terms,
                provider_filter: Some(&call_root),
                window: SUGGESTIONS_MAX * 2,
            },
        )
        .await
    {
        Ok(answer) => {
            end_query(transaction).await?;
            answer.window
        }
        Err(QueryError::Retriever(_error)) => {
            abandon_query(transaction).await?;
            Vec::new()
        }
        Err(QueryError::Shared(error)) => return Err(error),
    };
    let written_words = path_words(relative_path);
    let (named, others): (Vec<_>, Vec<_>) = window.into_iter().partition(|entity| {
        ProviderCallRoot::split_call_path(entity.call_path().as_str()).is_some_and(
            |(_root, relative)| {
                path_words(relative)
                    .iter()
                    .any(|word| written_words.iter().any(|written| same_word(word, written)))
            },
        )
    });
    for entity in named.into_iter().chain(others) {
        let path = entity.call_path().as_str();
        if suggested.len() == SUGGESTIONS_MAX {
            break;
        }
        if path != written && !suggested.iter().any(|known| known == path) {
            suggested.push(path.to_owned());
        }
    }
    Ok(suggested)
}

/// The distinct words of a Call Path's last segment, without filler words.
fn leaf_words(relative_path: &str) -> BTreeSet<String> {
    let leaf = relative_path.rsplit('.').next().unwrap_or(relative_path);
    path_words(leaf)
}

/// The distinct words of every segment of `relative_path`, without filler
/// words.
fn path_words(relative_path: &str) -> BTreeSet<String> {
    words(relative_path)
        .into_iter()
        .filter(|word| !FILLER_WORDS.contains(&word.as_str()))
        .collect()
}

/// Whether two path words are one word: equal, or one starts with the other
/// and is at least [`SHORTEST_WORD_STEM`] long, as `repo` and `repos`, or
/// `org` and `organization`, are.
fn same_word(one: &str, other: &str) -> bool {
    let (shorter, longer) = if one.len() <= other.len() {
        (one, other)
    } else {
        (other, one)
    };
    shorter == longer || (shorter.len() >= SHORTEST_WORD_STEM && longer.starts_with(shorter))
}

/// The lowercase words of `text`: split on every character that is not a
/// letter or digit, and where camelCase starts a word. A run of capitals is
/// one word, apart from a last capital that starts a lowercase word, so
/// `retrieveAPage` is `retrieve a page`, `getHTTPStatus` is
/// `get http status`, and `listURLs` is `list urls`.
fn words(text: &str) -> Vec<String> {
    let mut words = Vec::new();
    for part in text
        .split(|character: char| !character.is_alphanumeric())
        .filter(|part| !part.is_empty())
    {
        let characters = part.chars().collect::<Vec<_>>();
        let mut word = String::new();
        for (index, &character) in characters.iter().enumerate() {
            let previous = index.checked_sub(1).and_then(|at| characters.get(at));
            let next = characters.get(index + 1);
            let starts_word = character.is_uppercase()
                && previous.is_some_and(|previous| {
                    previous.is_lowercase()
                        || previous.is_numeric()
                        || (previous.is_uppercase()
                            && next.is_some_and(|next| next.is_lowercase())
                            // A plural `s` after an acronym ends it: `URLs`.
                            && !(next == Some(&'s') && characters.get(index + 2).is_none()))
                });
            if starts_word && !word.is_empty() {
                words.push(std::mem::take(&mut word));
            }
            word.extend(character.to_lowercase());
        }
        if !word.is_empty() {
            words.push(word);
        }
    }
    words
}

#[cfg(test)]
mod tests {
    use super::{leaf_words, same_word, words};

    #[test]
    fn words_split_separators_and_camel_case() {
        for (text, expected) in [
            ("retrieveAPage", &["retrieve", "a", "page"][..]),
            ("retrievePage", &["retrieve", "page"]),
            ("get_repo-content", &["get", "repo", "content"]),
            ("getHTTPStatus", &["get", "http", "status"]),
            ("listURLs", &["list", "urls"]),
            ("v2Get", &["v2", "get"]),
            ("", &[]),
        ] {
            assert_eq!(words(text), expected, "{text}");
        }
    }

    #[test]
    fn a_leaf_matches_without_filler_words_or_its_namespace() {
        assert_eq!(
            leaf_words("retrievePage"),
            leaf_words("pages.retrieveAPage")
        );
        assert_ne!(leaf_words("getRepo"), leaf_words("repos.get"));
    }

    #[test]
    fn a_word_matches_the_longer_word_it_starts() {
        for (one, other, same) in [
            ("repo", "repos", true),
            ("organization", "org", true),
            ("policy", "policy", true),
            ("retrieve", "retrievers", true),
            ("get", "getter", true),
            ("a", "add", false),
            ("ge", "get", false),
            ("policy", "providers", false),
        ] {
            assert_eq!(same_word(one, other), same, "{one} {other}");
        }
    }
}
