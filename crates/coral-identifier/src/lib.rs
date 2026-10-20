//! Coral-issued Task and Run identifiers.
//!
//! A new identifier is four words from [`WORDS`], drawn at random and joined by
//! hyphens, for example `height-word-database-library`. The words are the
//! identifier: Coral stores the text as the row's primary key and never
//! derives it from a UUID. Uniqueness is the primary key's job; the caller
//! inserts, checks the row count, and draws again on a conflict.
//!
//! Every word is one token on the tokenizers of the major model families, so
//! an agent copies an identifier for about eight tokens on Claude and four on
//! GPT, against 22 to 32 for a UUID.
//!
//! [`validate`] accepts the word form with four or five words, so a deployment
//! can add a fifth word later without a contract change, and a canonical
//! lowercase UUID, the form of rows written before this release. It does not
//! check the words against the list: the list is used only at generation, so an
//! edit to it never invalidates a stored identifier.

use std::sync::LazyLock;

use ring::rand::{SecureRandom as _, SystemRandom};

/// Words in a generated identifier.
pub const GENERATED_WORDS: usize = 4;
/// Inserts a caller attempts with fresh identifiers before it fails. With
/// 246⁴ identifiers in the space, eight conflicts in a row at a ten percent
/// retry rate happen once in 10⁸ inserts.
pub const ISSUE_ATTEMPTS: usize = 8;
/// Fewest words in an accepted word-form identifier.
pub const MIN_WORDS: usize = 4;
/// Most words in an accepted word-form identifier.
pub const MAX_WORDS: usize = 5;
/// Most letters in one accepted word.
pub const WORD_MAX_LETTERS: usize = 20;
/// Longest accepted identifier in bytes: five words of twenty letters and four hyphens.
pub const MAX_LENGTH: usize = MAX_WORDS * WORD_MAX_LETTERS + (MAX_WORDS - 1);
/// JSON Schema pattern of an accepted identifier: the word form, each word at
/// most [`WORD_MAX_LETTERS`] letters, or a canonical lowercase UUID. It agrees
/// with [`validate`] on every value.
pub const SCHEMA_PATTERN: &str = r"^([a-z]{1,20}(-[a-z]{1,20}){3,4}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$";

/// The word list, in file order: 246 lowercase English words, each one token on
/// the tokenizers of Claude, GPT, Llama 3, Qwen3, Gemma 3, Mistral and `DeepSeek`.
/// `validate_words.py` beside the list checks that rule; the README says how.
pub static WORDS: LazyLock<Vec<&'static str>> =
    LazyLock::new(|| include_str!("../words.txt").lines().collect());

/// Why a value is not an identifier Coral issues or accepts.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The value is neither the word form nor a canonical lowercase UUID.
    #[error(
        "identifier must be four or five lowercase words joined by hyphens, or a canonical lowercase UUID"
    )]
    Invalid,
    /// The operating system's random source gave no bytes.
    #[error("random source is unavailable")]
    RandomUnavailable,
}

/// Draws [`GENERATED_WORDS`] words from [`WORDS`] with the operating system's
/// random source and joins them with hyphens.
///
/// # Errors
///
/// Returns [`Error::RandomUnavailable`] when the random source fails.
pub fn generate() -> Result<String, Error> {
    let random = SystemRandom::new();
    let words = (0..GENERATED_WORDS)
        .map(|_word| draw(&random))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(words.join("-"))
}

/// One uniform draw from the list by rejection sampling over 16 random bits.
fn draw(random: &SystemRandom) -> Result<&'static str, Error> {
    let count = u16::try_from(WORDS.len()).map_err(|_count| Error::RandomUnavailable)?;
    // The largest multiple of `count` that fits: values at or above it are
    // rejected, so every word has the same chance.
    let limit = u16::MAX - (u16::MAX % count);
    loop {
        let mut bytes = [0_u8; 2];
        random
            .fill(&mut bytes)
            .map_err(|_unspecified| Error::RandomUnavailable)?;
        let value = u16::from_be_bytes(bytes);
        if value < limit {
            return WORDS
                .get(usize::from(value % count))
                .copied()
                .ok_or(Error::RandomUnavailable);
        }
    }
}

/// Accepts the word form, four or five lowercase ASCII words of at most
/// [`WORD_MAX_LETTERS`] letters joined by single hyphens, or a canonical
/// lowercase hyphenated UUID.
///
/// # Errors
///
/// Returns [`Error::Invalid`] for any other value.
pub fn validate(value: &str) -> Result<(), Error> {
    if is_word_form(value) || is_canonical_uuid(value) {
        Ok(())
    } else {
        Err(Error::Invalid)
    }
}

fn is_word_form(value: &str) -> bool {
    if value.len() > MAX_LENGTH {
        return false;
    }
    let words = value.split('-').collect::<Vec<_>>();
    (MIN_WORDS..=MAX_WORDS).contains(&words.len())
        && words.iter().all(|word| {
            !word.is_empty()
                && word.len() <= WORD_MAX_LETTERS
                && word.bytes().all(|byte| byte.is_ascii_lowercase())
        })
}

fn is_canonical_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|id| id.hyphenated().to_string() == value)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use super::*;

    /// Words that must never enter the list: Coral terms, code and schema tokens,
    /// instruction-like words, and words a reader could take for a value.
    const DENIED: &[&str] = &[
        "task",
        "tasks",
        "run",
        "runs",
        "agent",
        "agents",
        "provider",
        "providers",
        "workspace",
        "workspaces",
        "account",
        "accounts",
        "user",
        "users",
        "admin",
        "token",
        "tokens",
        "key",
        "keys",
        "secret",
        "secrets",
        "password",
        "private",
        "public",
        "delete",
        "remove",
        "ignore",
        "system",
        "prompt",
        "tool",
        "tools",
        "call",
        "calls",
        "function",
        "functions",
        "program",
        "programs",
        "operation",
        "operations",
        "error",
        "errors",
        "cancel",
        "wait",
        "start",
        "end",
        "search",
        "describe",
        "exec",
        "save",
        "saved",
        "status",
        "request",
        "requests",
        "member",
        "members",
        "owner",
        "owners",
        "null",
        "none",
        "true",
        "false",
        "yes",
        "nil",
        "void",
        "undefined",
        "drop",
        "kill",
        "stop",
        "abort",
        "reset",
        "clear",
        "fail",
        "failed",
        "exit",
        "quit",
        "never",
        "always",
        "every",
        "all",
        "skip",
        "retry",
        "undo",
        "redo",
        "sudo",
        "eval",
        "echo",
        "return",
        "break",
        "while",
        "until",
        "unless",
        "else",
        "then",
        "case",
        "switch",
        "default",
        "export",
        "import",
        "class",
        "value",
        "test",
        "tests",
        "id",
        "ids",
        "uuid",
        "first",
        "second",
        "third",
        "last",
        "next",
        "prev",
        "zero",
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
        "seven",
        "eight",
        "nine",
        "ten",
        "hundred",
        "north",
        "south",
        "east",
        "west",
        "left",
        "right",
        "up",
        "down",
    ];

    #[test]
    fn list_is_sorted_unique_lowercase_and_denies_nothing_hazardous() {
        let words = &*WORDS;
        assert_eq!(words.len(), 246);
        let sorted = words
            .windows(2)
            .all(|pair| matches!(pair, [before, after] if before < after));
        assert!(sorted, "words.txt must be sorted and unique");
        for word in words {
            assert!(
                (3..=13).contains(&word.len())
                    && word.bytes().all(|byte| byte.is_ascii_lowercase()),
                "{word} must be 3 to 13 lowercase ASCII letters"
            );
            assert!(!DENIED.contains(word), "{word} is on the deny list");
        }
    }

    #[test]
    fn generated_identifier_is_four_list_words_and_validates() {
        let identifier = generate().expect("random source");
        let words = identifier.split('-').collect::<Vec<_>>();
        assert_eq!(words.len(), GENERATED_WORDS);
        for word in &words {
            assert!(WORDS.contains(word), "{word} is not a list word");
        }
        validate(&identifier).expect("generated identifier validates");
    }

    #[test]
    fn generation_reaches_every_word() {
        let mut seen = HashMap::<&str, usize>::new();
        for _draw in 0..5_000 {
            for word in generate().expect("random source").split('-') {
                *seen
                    .entry(WORDS.iter().find(|w| **w == word).expect("list word"))
                    .or_default() += 1;
            }
        }
        let missing = WORDS
            .iter()
            .filter(|word| !seen.contains_key(*word))
            .collect::<BTreeSet<_>>();
        assert!(missing.is_empty(), "words never drawn: {missing:?}");
        let (least, most) = seen.values().fold((usize::MAX, 0), |(low, high), &count| {
            (low.min(count), high.max(count))
        });
        // 20,000 draws over 246 words: about 81 each. A ratio above 3 would
        // mean the draw is not uniform.
        assert!(most < 3 * least, "uneven draws: least {least}, most {most}");
    }

    #[test]
    fn validate_accepts_both_shapes_and_rejects_the_rest() {
        let accepted = [
            "height-word-database-library",
            "a-b-c-d",
            "small-title-work-image-power",
            "1d983ec7-0c2e-4b7a-9b2f-6f1f0d3c2a11",
        ];
        for value in accepted {
            assert_eq!(validate(value), Ok(()), "{value}");
        }
        let rejected = [
            "",
            "height-word-database",
            "one-two-three-four-five-six",
            "Height-word-database-library",
            "height-word-database-library-",
            "height--word-database-library",
            "height word database library",
            "height-word-database-librar1",
            "1D983EC7-0C2E-4B7A-9B2F-6F1F0D3C2A11",
            "1d983ec70c2e4b7a9b2f6f1f0d3c2a11",
            "abcdefghijklmnopqrstu-word-database-library",
        ];
        for value in rejected {
            assert_eq!(validate(value), Err(Error::Invalid), "{value}");
        }
    }
}
