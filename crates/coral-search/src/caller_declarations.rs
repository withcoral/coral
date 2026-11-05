//! The Tool declarations a Saved Function save is typed from, read as one
//! caller discovers them.

use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};

use crate::{CallPath, UniversalSearchModule};

/// Reads Tool declarations as an exact lookup would for one caller, so
/// typing a saved source reveals nothing the caller cannot already discover
/// (ADR 0020).
pub struct CallerToolDeclarations<'a> {
    search: &'a UniversalSearchModule,
    actor: &'a AuthenticatedPrincipal,
    workspace_id: &'a WorkspaceId,
}

impl<'a> CallerToolDeclarations<'a> {
    /// The declarations `actor` can discover in `workspace_id`.
    #[must_use]
    pub const fn new(
        search: &'a UniversalSearchModule,
        actor: &'a AuthenticatedPrincipal,
        workspace_id: &'a WorkspaceId,
    ) -> Self {
        Self {
            search,
            actor,
            workspace_id,
        }
    }
}

#[async_trait::async_trait]
impl coral_functions::ToolDeclarationReader for CallerToolDeclarations<'_> {
    async fn read(
        &self,
        call_paths: &[String],
    ) -> Result<Vec<coral_functions::ToolDeclaration>, Box<dyn std::error::Error + Send + Sync>>
    {
        let call_paths = call_paths
            .iter()
            .filter_map(|call_path| CallPath::parse(call_path).ok())
            .collect::<Vec<_>>();
        Ok(self
            .search
            .describe_tools(self.actor, self.workspace_id, &call_paths)
            .await?
            .into_iter()
            .map(|description| {
                coral_functions::ToolDeclaration::new(
                    description.call_path().as_str().to_owned(),
                    description.typescript_declaration().to_owned(),
                )
            })
            .collect())
    }
}
