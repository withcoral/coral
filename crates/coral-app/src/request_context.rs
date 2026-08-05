//! Request-scoped app context selected at the transport boundary.

use crate::identity::Principal;
use crate::task::id::TaskId;

/// Request-scoped data that domain services may need after authentication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestContext {
    principal: Principal,
    task_id: Option<TaskId>,
    tool_intent: Option<String>,
}

impl RequestContext {
    pub(crate) fn new(principal: Principal) -> Self {
        Self {
            principal,
            task_id: None,
            tool_intent: None,
        }
    }

    pub(crate) fn with_task_id(mut self, task_id: Option<TaskId>) -> Self {
        self.task_id = task_id;
        self
    }

    pub(crate) fn with_tool_intent(mut self, tool_intent: Option<String>) -> Self {
        self.tool_intent = tool_intent;
        self
    }

    pub(crate) fn principal(&self) -> &Principal {
        &self.principal
    }

    pub(crate) fn task_id(&self) -> Option<TaskId> {
        self.task_id
    }

    pub(crate) fn tool_intent(&self) -> Option<&str> {
        self.tool_intent.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::PrincipalKind;

    #[test]
    fn exposes_request_principal() {
        let principal = Principal::parse("product:principal:saul", PrincipalKind::User)
            .expect("valid principal");
        let context = RequestContext::new(principal.clone());

        assert_eq!(context.principal(), &principal);
        assert_eq!(context.principal().kind(), PrincipalKind::User);
        assert_eq!(context.task_id(), None);
    }

    #[test]
    fn carries_unvalidated_task_metadata_until_the_service_edge() {
        let principal = Principal::parse("product:principal:saul", PrincipalKind::User)
            .expect("valid principal");
        let task_id = crate::task::id::TaskId::parse("550e8400-e29b-41d4-a716-446655440000")
            .expect("valid task id");
        let context = RequestContext::new(principal).with_task_id(Some(task_id));

        assert_eq!(context.task_id(), Some(task_id));
    }

    #[test]
    fn carries_tool_intent_with_task_metadata() {
        let principal = Principal::parse("product:principal:saul", PrincipalKind::User)
            .expect("valid principal");
        let context =
            RequestContext::new(principal).with_tool_intent(Some("Find renewal risk".to_string()));

        assert_eq!(context.tool_intent(), Some("Find renewal risk"));
    }
}
