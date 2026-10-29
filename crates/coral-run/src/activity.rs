//! Summary-only chronological execution children. Exact output uses a separate read.
use crate::{Error, InvocationSummary, RunId, RunModule, RunSummary};
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};

/// Non-authorizing identity of one execution child.
#[derive(Clone, Debug)]
pub enum RunEntryAnchor {
    /// An immutable Event sequence.
    Event(u64),
    /// A Tool admission.
    Invocation(crate::InvocationId),
    /// The terminal Event.
    Final,
}
impl From<&RunEntryAnchor> for RunEntryWindow {
    fn from(anchor: &RunEntryAnchor) -> Self {
        match anchor {
            RunEntryAnchor::Event(sequence) => Self::Event(*sequence),
            RunEntryAnchor::Invocation(id) => Self::Invocation(id.clone()),
            RunEntryAnchor::Final => Self::Final,
        }
    }
}

/// Chronological child window, resolved against the exact authorized Run.
#[derive(Clone, Debug, Default)]
pub enum RunEntryWindow {
    /// Beginning of execution.
    #[default]
    Start,
    /// Strictly following an Event sequence.
    After(u64),
    /// Start with one exact Event.
    Event(u64),
    /// Start with one exact Tool admission.
    Invocation(crate::InvocationId),
    /// Terminal event and optional Final Value.
    Final,
    /// Bounded scroll viewport, reanchored in the same authorization snapshot.
    Viewport {
        /// First ordinal when the anchor is absent or unavailable.
        offset: u64,
        /// Currently visible child identity, never an authority grant.
        anchor: Option<RunEntryAnchor>,
        /// Predecessor budget, less than the requested page size.
        preceding: u16,
    },
}
/// One child row; Invocation end Events do not duplicate Tool rows.
pub enum RunEntry {
    /// Current Tool state, without arguments or result.
    Invocation(InvocationSummary),
    /// Immutable Event identity, without its payload.
    Event {
        /// Stable sequence in the Run.
        sequence: u64,
        /// Retained Event kind. Unknown kinds remain inspectable as unavailable.
        kind: String,
        /// Durable occurrence time.
        occurred_at: i64,
        /// Returned payload bytes. For run.ended this is the optional Final Value.
        context_impact_bytes: i64,
        /// Where authored source made an Emission or Log, when recorded.
        source_location: Option<crate::SourceLocation>,
    },
}
/// One bounded summary page and independently authorized parent metadata.
pub struct RunEntries {
    /// Parent summary.
    pub run: RunSummary,
    /// Chronological children.
    pub entries: Vec<RunEntry>,
    /// Last underlying sequence, never a pixel offset.
    pub next_after: u64,
    /// More children exist.
    pub has_more: bool,
    /// A successful terminal Run retained a Final Value, including explicit null.
    pub has_final_value: bool,
    /// Current child count, independent of the bounded reading window.
    pub total: i64,
    /// Current ordinal of the first returned child.
    pub offset: u64,
    /// The anchor disappeared; the viewport uses its requested ordinal instead.
    pub anchor_unavailable: bool,
}
impl RunModule {
    /// Lists source/arguments/output-free execution children.
    /// # Errors
    /// Conceals inaccessible Runs and rejects invalid bounds.
    pub async fn activity_entries(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &RunId,
        window: &RunEntryWindow,
        limit: u16,
        selection: Option<&coral_workspace::HistoryScope>,
    ) -> Result<RunEntries, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let page = self
            .activity_entries_in_transaction(
                &mut tx, actor, workspace, id, window, limit, selection,
            )
            .await?;
        tx.commit().await?;
        Ok(page)
    }
    /// Joins an upper owner's read snapshot for authorized source-free execution children.
    /// # Errors
    /// Conceals inaccessible Runs and rejects invalid page bounds.
    #[expect(
        clippy::too_many_arguments,
        reason = "the upper owner adds its snapshot to the same scoped child-read inputs"
    )]
    pub async fn activity_entries_in_transaction(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &RunId,
        window: &RunEntryWindow,
        limit: u16,
        selection: Option<&coral_workspace::HistoryScope>,
    ) -> Result<RunEntries, Error> {
        if !(1..=100).contains(&limit) {
            return Err(Error::InvalidInput("invalid activity page size".into()));
        }
        if let RunEntryWindow::Viewport { preceding, .. } = window
            && *preceding >= limit
        {
            return Err(Error::InvalidInput(
                "invalid viewport predecessor budget".into(),
            ));
        }
        if actor.authenticated_user().is_none() {
            return Err(Error::RunNotFound);
        }
        let agents = self
            .visibility_scope_in_transaction(tx, actor, workspace)
            .await?;
        let now = self.now()?;
        if let Some(selection) = selection {
            let authority = self
                .workspaces
                .history_authority_in_transaction(tx, actor, selection)
                .await?;
            crate::history_repository::run_summary(tx, &authority, workspace, id, now).await?;
        }
        let (summary, has_final_value) = crate::history_repository::visible_run_summary(
            tx,
            actor.principal(),
            agents,
            workspace,
            id,
            now,
        )
        .await?;
        let page =
            crate::inspection_repository::entry_summaries(tx, workspace, id, window, limit).await?;
        let has_more = page.rows.len() > usize::from(limit);
        let mut entries = Vec::new();
        let mut next_after = page.after;
        for row in page.rows.into_iter().take(usize::from(limit)) {
            next_after = row.sequence;
            entries.push(if let Some(invocation) = row.invocation {
                RunEntry::Invocation(
                    crate::history_repository::exact_invocation(
                        tx,
                        workspace,
                        id,
                        &invocation,
                        now,
                    )
                    .await?,
                )
            } else {
                RunEntry::Event {
                    sequence: row.sequence,
                    kind: row.kind,
                    occurred_at: row.occurred_at,
                    context_impact_bytes: row.context_impact_bytes,
                    source_location: row.source_location,
                }
            });
        }
        Ok(RunEntries {
            total: page.total,
            offset: page.offset,
            anchor_unavailable: page.anchor_unavailable,
            run: summary,
            entries,
            next_after,
            has_more,
            has_final_value,
        })
    }
}
