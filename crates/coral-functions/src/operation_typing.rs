//! Advisory typing of the Operations and Saved Functions a source names.
//!
//! The composing caller supplies an [`OperationDeclarationReader`] bound to
//! its own discovery authority, so this crate never decides which Operations
//! a caller can see. Typing is advisory: whatever it cannot finish leaves the
//! source untyped rather than fail the save.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use coral_v8::CallableDeclaration;

/// The most candidate Call Paths one source may name and still be typed.
/// Each prefix of a literal chain is a candidate, so a four-segment call
/// names three. Above it, Operations stay untyped.
const OPERATION_DECLARATION_MAX_COUNT: usize = 128;

/// The most UTF-8 bytes the declarations of one source may total and still
/// be typed, so a typed compile fits the reserve an untyped one would need.
const OPERATION_DECLARATIONS_MAX_BYTES: usize = 1024 * 1024;

/// The longest typing may take: finding the Call Paths a source names and
/// reading their declarations, together.
const TYPING_BUDGET: Duration = Duration::from_millis(500);

/// The compilation time typing always leaves, so typing never makes a
/// compile time out that would have finished untyped.
const TYPING_COMPILATION_RESERVE: Duration = Duration::from_secs(1);

/// Reads Operation declarations with one caller's discovery authority.
#[async_trait]
pub trait OperationDeclarationReader: Send + Sync {
    /// Returns the declaration of each candidate Call Path, such as
    /// `coral.providers.github.repos.get`, that names an Operation the
    /// caller can discover.
    ///
    /// Every other path is omitted, so absence never says why.
    ///
    /// # Errors
    ///
    /// Returns the reader's operational failure.
    async fn read(
        &self,
        call_paths: &[String],
    ) -> Result<Vec<OperationDeclaration>, Box<dyn std::error::Error + Send + Sync>>;
}

/// One Operation's complete self-contained declaration, by its Call Path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationDeclaration {
    call_path: String,
    declaration: String,
}

impl OperationDeclaration {
    /// Pairs an Operation Call Path with its declaration.
    #[must_use]
    pub const fn new(call_path: String, declaration: String) -> Self {
        Self {
            call_path,
            declaration,
        }
    }
}

/// When typing must stop for a compile due by `compilation_deadline`, or
/// `None` when too little time is left to type at all.
///
/// Typing gets at most [`TYPING_BUDGET`] and always leaves
/// [`TYPING_COMPILATION_RESERVE`] to compile.
pub(crate) fn typing_deadline(compilation_deadline: Instant) -> Option<Instant> {
    let now = Instant::now();
    let deadline =
        (now + TYPING_BUDGET).min(compilation_deadline.checked_sub(TYPING_COMPILATION_RESERVE)?);
    (deadline > now).then_some(deadline)
}

/// The declarations that type `coral.providers` and `coral.workspace`, or
/// `None` to leave them untyped: without literal paths (the source reaches
/// them only through aliases, if at all), above
/// [`OPERATION_DECLARATION_MAX_COUNT`] paths or
/// [`OPERATION_DECLARATIONS_MAX_BYTES`], or when the read fails.
pub(crate) async fn operation_declarations(
    reader: &dyn OperationDeclarationReader,
    call_paths: &[String],
) -> Option<Vec<CallableDeclaration>> {
    if call_paths.is_empty() || call_paths.len() > OPERATION_DECLARATION_MAX_COUNT {
        return None;
    }
    let declarations = reader
        .read(call_paths)
        .await
        .inspect_err(|error| {
            tracing::warn!(%error, "Operation declarations are unavailable; the source is checked untyped");
        })
        .ok()?;
    let bytes = declarations
        .iter()
        .map(|operation| operation.declaration.len())
        .sum::<usize>();
    if bytes > OPERATION_DECLARATIONS_MAX_BYTES {
        return None;
    }
    // Operation Call Paths are typed relative to `coral`.
    Some(
        declarations
            .into_iter()
            .filter_map(|operation| {
                let segments = operation.call_path.strip_prefix("coral.")?.split('.');
                Some(CallableDeclaration::new(
                    segments.map(str::to_owned).collect(),
                    operation.declaration,
                ))
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Failing;

    #[async_trait]
    impl OperationDeclarationReader for Failing {
        async fn read(
            &self,
            _call_paths: &[String],
        ) -> Result<Vec<OperationDeclaration>, Box<dyn std::error::Error + Send + Sync>> {
            Err("catalog is unreadable".into())
        }
    }

    /// Declares every Call Path it is asked for, in `.0` bytes each.
    struct DeclaresEverything(usize);

    #[async_trait]
    impl OperationDeclarationReader for DeclaresEverything {
        async fn read(
            &self,
            call_paths: &[String],
        ) -> Result<Vec<OperationDeclaration>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(call_paths
                .iter()
                .map(|path| OperationDeclaration::new(path.clone(), " ".repeat(self.0)))
                .collect())
        }
    }

    #[tokio::test]
    async fn operations_fall_back_to_untyped_rather_than_fail() {
        let path = "coral.providers.github.repos.get".to_owned();
        let too_many = vec![path.clone(); OPERATION_DECLARATION_MAX_COUNT + 1];
        let one = std::slice::from_ref(&path);
        let two = [path.clone(), path.clone()];
        let half = OPERATION_DECLARATIONS_MAX_BYTES / 2;
        let cases: [(&str, &dyn OperationDeclarationReader, &[String], bool); 6] = [
            ("a read", &DeclaresEverything(0), one, true),
            ("no literal paths", &DeclaresEverything(0), &[], false),
            ("a failed read", &Failing, one, false),
            (
                "too many paths",
                &DeclaresEverything(0),
                too_many.as_slice(),
                false,
            ),
            (
                "declarations at the size limit",
                &DeclaresEverything(half),
                &two,
                true,
            ),
            (
                "declarations over the size limit",
                &DeclaresEverything(half + 1),
                &two,
                false,
            ),
        ];
        for (name, reader, call_paths, typed) in cases {
            assert_eq!(
                operation_declarations(reader, call_paths).await.is_some(),
                typed,
                "{name}"
            );
        }
    }

    #[test]
    fn typing_always_leaves_the_compile_its_reserve() {
        let reserve = TYPING_COMPILATION_RESERVE;
        let cases = [
            ("a fresh compile", Duration::from_secs(2), true),
            (
                "a compile near its reserve",
                reserve + Duration::from_millis(100),
                true,
            ),
            ("a compile inside its reserve", reserve, false),
        ];
        for (name, remaining, typed) in cases {
            let before = Instant::now();
            let compilation_deadline = before + remaining;
            let deadline = typing_deadline(compilation_deadline);
            let after = Instant::now();
            assert_eq!(deadline.is_some(), typed, "{name}");
            if let Some(deadline) = deadline {
                assert!(deadline <= after + TYPING_BUDGET, "{name}");
                assert!(deadline + reserve <= compilation_deadline, "{name}");
            }
        }
        let expired = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("representable instant");
        assert_eq!(typing_deadline(expired), None, "an expired compile");
    }
}
