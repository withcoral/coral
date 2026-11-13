//! The text the Embedder embeds for a Search Entity: the manifest's index
//! text, built as the bench harness built it when it measured the pick
//! (`path_title_desc_params` in coral-benchmarks
//! `tools/universal-search/prototype-semantic`). A change here changes the
//! vectors without a new Embedder revision, so it keeps the harness's rules.

use coral_embed::{IndexText, MANIFEST};

use crate::types::EntityKind;

/// The most field names one Search Entity's text holds, as in the harness.
const FIELD_NAMES_CAP: usize = 100;

/// The words of the leaf name of each declaration path in `paths`, in
/// order, each distinct name once and at most [`FIELD_NAMES_CAP`] of them,
/// joined by spaces. `input.body.incident_id` gives `incident id`. The
/// caller picks the paths: a Provider Tool gives those of its input and its
/// result, as the harness did, and an Authored Tool those of its input. A
/// path with a segment that is not an identifier, such as `[key]` or a
/// number, gives nothing.
pub(crate) fn field_names<'a>(paths: impl IntoIterator<Item = &'a str>) -> String {
    let mut names: Vec<String> = Vec::new();
    for path in paths {
        let Some(leaf) = identifier_path_leaf(path.trim()) else {
            continue;
        };
        let name = segment_words(leaf).join(" ");
        if name.is_empty() || names.contains(&name) {
            continue;
        }
        names.push(name);
        if names.len() >= FIELD_NAMES_CAP {
            break;
        }
    }
    names.join(" ")
}

/// The columns of a Search Entity's row that its index text reads.
pub(crate) struct IndexTextSource<'a> {
    pub(crate) kind: EntityKind,
    pub(crate) qualified_name: &'a str,
    pub(crate) title: &'a str,
    pub(crate) description: &'a str,
    /// The Call Root's name, a space and the Provider's display name; empty
    /// for an Authored Tool.
    pub(crate) provider_name: &'a str,
    pub(crate) field_names: &'a str,
}

/// The text of one Search Entity for the manifest's index text, without the
/// document prefix, which the model adds.
///
/// A Provider Tool gives its Provider's display name with the words of its
/// path, its title, its description and its field names, one per line. A
/// Private or Shared Tool has no Provider and no title: the words of its path,
/// its description and its field names.
pub(crate) fn index_text(source: &IndexTextSource<'_>) -> String {
    match MANIFEST.index_text {
        IndexText::PathTitleDescriptionParameterNames => {
            let path = segment_words(source.qualified_name).join(" ");
            match source.kind {
                EntityKind::ProviderTool => {
                    let (call_root, display_name) = source
                        .provider_name
                        .split_once(' ')
                        .unwrap_or((source.provider_name, ""));
                    let display_name = if display_name.is_empty() {
                        call_root
                    } else {
                        display_name
                    };
                    format!(
                        "{display_name} {path}\n{}\n{}\n{}",
                        source.title, source.description, source.field_names
                    )
                }
                EntityKind::PrivateTool | EntityKind::SharedTool => {
                    format!("{path}\n{}\n{}", source.description, source.field_names)
                }
            }
        }
    }
}

/// The last segment of `path`, without a trailing `[]`, when `path` is two
/// or more identifiers joined by dots, each after the first optionally
/// followed by `[]`. An identifier starts with an ASCII letter or `_`, and
/// goes on with letters, digits or `_`.
fn identifier_path_leaf(path: &str) -> Option<&str> {
    let mut segments = path.split('.');
    let root = segments.next()?;
    if !is_identifier(root) {
        return None;
    }
    let mut leaf = None;
    for segment in segments {
        let name = segment.strip_suffix("[]").unwrap_or(segment);
        if !is_identifier(name) {
            return None;
        }
        leaf = Some(name);
    }
    leaf
}

fn is_identifier(name: &str) -> bool {
    let mut characters = name.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && characters.all(|character| character.is_alphanumeric() || character == '_')
}

/// The lowercase words of a name: parts split at `_`, then at each change of
/// case, a run of capitals before a capitalized word (`HTTPServer` gives
/// `http server`), and each run of digits. Other characters split words and
/// are dropped.
fn segment_words(name: &str) -> Vec<String> {
    let mut words = Vec::new();
    for part in name.split('_') {
        let characters: Vec<char> = part.chars().collect();
        let mut start = 0;
        while let Some(&first) = characters.get(start) {
            let end = word_end(&characters, start, first);
            if let Some(end) = end {
                words.push(
                    characters
                        .get(start..end)
                        .unwrap_or_default()
                        .iter()
                        .collect::<String>()
                        .to_lowercase(),
                );
                start = end;
            } else {
                start += 1;
            }
        }
    }
    words
}

/// Where the word that starts at `start` ends, or `None` when no word
/// starts there.
fn word_end(characters: &[char], start: usize, first: char) -> Option<usize> {
    let run = |from: usize, test: fn(&char) -> bool| {
        characters
            .get(from..)
            .unwrap_or_default()
            .iter()
            .take_while(|character| test(character))
            .count()
            + from
    };
    if first.is_ascii_uppercase() {
        let capitals = run(start, char::is_ascii_uppercase);
        let next_is_lower = characters
            .get(capitals)
            .is_some_and(char::is_ascii_lowercase);
        // Capitals before a capitalized word: all but the last.
        if capitals - start >= 2 && next_is_lower {
            return Some(capitals - 1);
        }
        // One capital and its lowercase letters.
        if next_is_lower {
            return Some(run(start + 1, char::is_ascii_lowercase));
        }
        return Some(capitals);
    }
    if first.is_ascii_lowercase() {
        return Some(run(start, char::is_ascii_lowercase));
    }
    if first.is_ascii_digit() {
        return Some(run(start, char::is_ascii_digit));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{field_names, segment_words};

    /// Each expected value is the harness's own output for the same input
    /// (`segment_words` and `parameter_names` in `semantic_prototype.py`).
    #[test]
    fn names_split_into_words_as_the_bench_split_them() {
        for (name, words) in [
            ("getForecast", &["get", "forecast"][..]),
            ("HTTPServer_url", &["http", "server", "url"]),
            ("X509CertURL", &["x", "509", "cert", "url"]),
            ("ABc", &["a", "bc"]),
            ("temperatureC", &["temperature", "c"]),
            ("list_users.v2", &["list", "users", "v", "2"]),
            ("odata.nextLink", &["odata", "next", "link"]),
            ("éclair_x", &["clair", "x"]),
            ("a__b", &["a", "b"]),
            ("__", &[]),
            ("9lives", &["9", "lives"]),
        ] {
            assert_eq!(segment_words(name), words, "{name}");
        }
    }

    #[test]
    fn field_names_keep_each_identifier_leaf_once_as_the_bench_did() {
        let paths = [
            "input",
            "input.city",
            "input.body.incident_id",
            "input.items[].userName",
            "result.temperatureC",
            "input.headers[key]",
            "input.0",
            "result.items[].incident_id",
            "input.body.Incident_ID",
            "result.__",
            "input.x-request-id",
            "result",
            "input.ÿcount",
        ];

        assert_eq!(
            field_names(paths),
            "city incident id user name temperature c"
        );
    }

    #[test]
    fn field_names_stop_at_the_cap() {
        let paths = (0..150).map(|field| format!("input.field{field}"));
        let paths = paths.collect::<Vec<_>>();

        let names = field_names(paths.iter().map(String::as_str));

        assert_eq!(
            names.split(' ').filter(|word| *word == "field").count(),
            100
        );
    }
}
