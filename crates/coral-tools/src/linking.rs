//! What Program admission links: the Private and Shared Tools a source names
//! by a literal path, the current definition of each, and the hold that
//! keeps every definition as selected through the acceptance commit.
//!
//! An [`AuthoredToolPath`] resolves by the root its source wrote (ADR 0033):
//! `coral.private` is always the invoking Principal's, also inside a Shared
//! Tool, and `coral.shared` is always the Workspace's. These facets do not
//! authorize callers or commit transactions.

use coral_catalog::{AuthoredToolKind, CodeExecutionBinding, ToolAccessState, ToolKey};
use coral_db::Transaction;
use coral_workspace::{Principal, WorkspaceId};

use crate::types::{AuthoredToolPath, AuthoredToolSource, PrivateTool};
use crate::{Error, repository, shared_tool_repository};

/// One linked Tool a source names by a literal path, and where it names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkedToolReference {
    path: AuthoredToolPath,
    line: u64,
    column: u64,
}

impl LinkedToolReference {
    /// A reference to `path` at a one-based authored `line` and `column`.
    #[must_use]
    pub const fn new(path: AuthoredToolPath, line: u64, column: u64) -> Self {
        Self { path, line, column }
    }

    /// Returns the Tool the source names.
    #[must_use]
    pub const fn path(&self) -> &AuthoredToolPath {
        &self.path
    }

    /// Returns the one-based authored source line.
    #[must_use]
    pub const fn line(&self) -> u64 {
        self.line
    }

    /// Returns the one-based authored source column.
    #[must_use]
    pub const fn column(&self) -> u64 {
        self.column
    }

    /// The references the compiler found under `coral.private` and
    /// `coral.shared`, each with its path parsed, Private Tools first.
    ///
    /// # Errors
    ///
    /// Returns the first path outside the authoring bounds, with where the
    /// source names it.
    pub fn from_compiler(
        private_tools: &[coral_v8::LinkedToolReference],
        shared_tools: &[coral_v8::LinkedToolReference],
    ) -> Result<Vec<Self>, InvalidLinkedToolReference> {
        let of_kind = |kind, references: &[coral_v8::LinkedToolReference]| {
            references
                .iter()
                .map(move |reference| {
                    AuthoredToolPath::parse(kind, reference.path())
                        .map(|path| Self::new(path, reference.line(), reference.column()))
                        .map_err(|error| InvalidLinkedToolReference {
                            line: reference.line(),
                            column: reference.column(),
                            error,
                        })
                })
                .collect::<Vec<_>>()
        };
        of_kind(AuthoredToolKind::Private, private_tools)
            .into_iter()
            .chain(of_kind(AuthoredToolKind::Shared, shared_tools))
            .collect()
    }
}

/// A literal reference whose path cannot name a Private or Shared Tool.
#[derive(Debug)]
pub struct InvalidLinkedToolReference {
    /// The one-based authored source line.
    pub line: u64,
    /// The one-based authored source column.
    pub column: u64,
    /// Why the path names no Tool.
    pub error: Error,
}

/// The current definition of one linked Tool, selected in the admission
/// owner's read snapshot: the caller's current Private Tool, or the current
/// revision of a Shared Tool.
///
/// Its revision is acceptance evidence. Run retains the identity, revision
/// and source in its own durable accepted package.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkedToolSelection {
    path: AuthoredToolPath,
    key: ToolKey,
    revision: u64,
    source: AuthoredToolSource,
    default_access_state: ToolAccessState,
}

impl LinkedToolSelection {
    pub(crate) fn of_private_tool(tool: &PrivateTool) -> Self {
        Self {
            path: AuthoredToolPath::Private(tool.path().clone()),
            key: tool.id().tool_key(),
            revision: tool.revision(),
            source: tool.source().clone(),
            default_access_state: tool.default_access_state(),
        }
    }

    pub(crate) fn of_shared_tool(selection: shared_tool_repository::CurrentRevision) -> Self {
        Self {
            path: AuthoredToolPath::Shared(selection.path),
            key: selection.id.tool_key(),
            revision: selection.revision,
            source: selection.source,
            default_access_state: selection.default_access_state,
        }
    }

    /// Returns the kind of Tool selected.
    #[must_use]
    pub const fn kind(&self) -> AuthoredToolKind {
        self.path.kind()
    }

    /// Returns the selected path, for static linking.
    #[must_use]
    pub const fn path(&self) -> &AuthoredToolPath {
        &self.path
    }

    /// Returns the selected Tool's stable identity: its Tool Key in its
    /// kind's Provider, which an exact rule names.
    #[must_use]
    pub const fn tool_key(&self) -> &ToolKey {
        &self.key
    }

    /// Returns the revision the Run links.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the exact selected authored source.
    #[must_use]
    pub const fn source(&self) -> &AuthoredToolSource {
        &self.source
    }

    /// Returns the default Tool Access State of the selected revision,
    /// which applies when no exact rule matches: a Private Tool's author
    /// declared it, and a Shared Tool's publishing Owner chose it.
    #[must_use]
    pub const fn default_access_state(&self) -> ToolAccessState {
        self.default_access_state
    }

    /// Returns the code Execution Binding of the selected revision: the
    /// definition a Run that links it performs when the Program calls it.
    #[must_use]
    pub fn execution_binding(&self) -> CodeExecutionBinding {
        CodeExecutionBinding::new(self.key.as_str().to_owned(), self.revision)
    }
}

/// Selects the current definition at `path` for a Run to link: the
/// Private Tool of `principal`, or the Workspace's live Shared Tool. `None`
/// when no such Tool is there.
///
/// The caller must first authorize the Principal in this Workspace, use the
/// same snapshot for the complete closure, and hold every selection at
/// acceptance with [`hold_linked_tools_in_transaction`]. This facet neither
/// commits nor creates retained Run history.
///
/// # Errors
///
/// Returns a persistence or corrupt-definition failure.
pub async fn select_linked_tool_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    path: &AuthoredToolPath,
) -> Result<Option<LinkedToolSelection>, Error> {
    match path {
        AuthoredToolPath::Private(path) => {
            repository::find_current_in_transaction(transaction, workspace_id, principal, path)
                .await
                .map(|tool| tool.as_ref().map(LinkedToolSelection::of_private_tool))
        }
        AuthoredToolPath::Shared(path) => {
            shared_tool_repository::select_current_source(transaction, workspace_id, path)
                .await
                .map(|selection| selection.map(LinkedToolSelection::of_shared_tool))
        }
    }
}

/// Holds every selected definition unchanged through the caller's
/// acceptance commit. Returns the first selection that was replaced,
/// removed or republished since it was selected, in which case admission
/// must reject without a Run; `None` when every one holds.
///
/// The caller must hold current Workspace authority in this write
/// transaction and pass the Workspace and Principal used for selection.
///
/// The order and strength of the holds are this facet's. Every acceptance
/// takes them in path order, Private Tools first, so two never wait on each
/// other. A Private Tool row is held exclusively: only its owner links it.
/// A Shared Tool row is held shared, so a popular Shared Tool does not
/// serialize acceptance across Members, while a publish waits for them.
///
/// # Errors
///
/// Returns a persistence or corrupt-selection failure.
pub async fn hold_linked_tools_in_transaction<'a>(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    principal: &Principal,
    selections: &'a [LinkedToolSelection],
) -> Result<Option<&'a LinkedToolSelection>, Error> {
    let mut in_hold_order = selections.iter().collect::<Vec<_>>();
    in_hold_order.sort_by(|left, right| left.path.cmp(&right.path));
    for selection in in_hold_order {
        let held = match &selection.path {
            AuthoredToolPath::Private(path) => {
                repository::hold_current_revision(
                    transaction,
                    workspace_id,
                    principal,
                    &selection.key,
                    path,
                    selection.revision,
                )
                .await?
            }
            AuthoredToolPath::Shared(path) => {
                shared_tool_repository::hold_current(
                    transaction,
                    workspace_id,
                    &selection.key,
                    path,
                    selection.revision,
                )
                .await?
            }
        };
        if !held {
            return Ok(Some(selection));
        }
    }
    Ok(None)
}
