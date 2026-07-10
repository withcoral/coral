//! Request-scoped app context selected at the transport boundary.

use tonic::{Request, Status};

use crate::bootstrap::{AppError, app_status};
use crate::identity::UserPrincipal;

/// Request-scoped data that domain services may need after authentication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestContext {
    principal: UserPrincipal,
}

impl RequestContext {
    pub(crate) fn new(principal: UserPrincipal) -> Self {
        Self { principal }
    }

    pub(crate) fn from_request<T>(request: &Request<T>) -> Result<Self, Status> {
        request.extensions().get::<Self>().cloned().ok_or_else(|| {
            app_status(AppError::Unauthenticated(
                "missing request principal".to_string(),
            ))
        })
    }

    pub(crate) fn principal(&self) -> &UserPrincipal {
        &self.principal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_request_principal() {
        let principal = UserPrincipal::for_user("saul").expect("valid user");
        let context = RequestContext::new(principal.clone());

        assert_eq!(context.principal(), &principal);
    }

    #[test]
    fn rejects_requests_without_a_principal() {
        let status = RequestContext::from_request(&Request::new(()))
            .expect_err("missing context should fail closed");

        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }
}
