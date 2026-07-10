//! Request context shared by query, catalog, source validation, and search.

use crate::identity::UserPrincipal;
use crate::request_context::RequestContext;
use crate::workspaces::WorkspaceName;
use tonic::{Request, Status};

#[derive(Clone, Debug)]
pub(crate) struct QueryContext {
    workspace_name: WorkspaceName,
    request_context: RequestContext,
}

impl QueryContext {
    fn new(workspace_name: WorkspaceName, request_context: RequestContext) -> Self {
        Self {
            workspace_name,
            request_context,
        }
    }

    pub(crate) fn workspace_name(&self) -> &WorkspaceName {
        &self.workspace_name
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

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "consumed by user-owned identity resolution in a later mission unit"
        )
    )]
    pub(crate) fn principal(&self) -> &UserPrincipal {
        self.request_context.principal()
    }

    #[cfg(test)]
    pub(crate) fn local_for_test(workspace_name: WorkspaceName) -> Self {
        Self::new(workspace_name, RequestContext::new(UserPrincipal::local()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_workspace_and_principal() {
        let workspace_name = WorkspaceName::parse("demo").expect("workspace");
        let principal = UserPrincipal::for_user("saul").expect("principal");
        let mut request = Request::new(());
        request
            .extensions_mut()
            .insert(RequestContext::new(principal.clone()));
        let context =
            QueryContext::from_request(workspace_name.clone(), &request).expect("query context");

        assert_eq!(context.workspace_name(), &workspace_name);
        assert_eq!(context.principal(), &principal);
    }
}
