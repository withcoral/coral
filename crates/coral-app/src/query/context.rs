//! Query-scoped app context composed from request metadata and payload scope.

use tonic::{Request, Status};

use crate::episode::EpisodeId;
use crate::identity::UserPrincipal;
use crate::request_context::RequestContext;
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryContext {
    workspace_name: WorkspaceName,
    request_context: RequestContext,
}

impl QueryContext {
    pub(crate) fn new(workspace_name: WorkspaceName, request_context: RequestContext) -> Self {
        Self {
            workspace_name,
            request_context,
        }
    }

    pub(crate) fn from_request<T>(
        workspace_name: WorkspaceName,
        request: &Request<T>,
    ) -> Result<Self, Status> {
        Ok(Self::new(
            workspace_name,
            RequestContext::from_request(request)?,
        ))
    }

    pub(crate) fn workspace_name(&self) -> &WorkspaceName {
        &self.workspace_name
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "QueryContext intentionally carries principal state before query authorization consumes it."
        )
    )]
    pub(crate) fn principal(&self) -> &UserPrincipal {
        self.request_context.principal()
    }

    pub(crate) fn episode_id(&self) -> Option<&EpisodeId> {
        self.request_context.attribution().episode_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::UserPrincipal;
    use crate::query::QueryAttribution;
    use crate::request_context::RequestContext;

    #[test]
    fn builds_from_tonic_request_extension() {
        let episode_id = EpisodeId::parse("ep_context").expect("episode id");
        let request_context = RequestContext::with_attribution(
            UserPrincipal::local(),
            QueryAttribution::new(Some(episode_id.clone())),
        );
        let mut request = Request::new(());
        request.extensions_mut().insert(request_context);
        let workspace_name = WorkspaceName::default();

        let context =
            QueryContext::from_request(workspace_name.clone(), &request).expect("query context");

        assert_eq!(context.workspace_name(), &workspace_name);
        assert_eq!(context.principal(), &UserPrincipal::local());
        assert_eq!(context.episode_id(), Some(&episode_id));
    }
}
