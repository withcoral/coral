//! Request-scoped app context selected at the transport boundary.

use tonic::{Request, Status};

use crate::bootstrap::{AppError, app_status};
use crate::identity::UserPrincipal;
use crate::query::QueryAttribution;

/// Request-scoped data that domain services may need after authentication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestContext {
    principal: UserPrincipal,
    attribution: QueryAttribution,
}

impl RequestContext {
    pub(crate) fn with_attribution(
        principal: UserPrincipal,
        attribution: QueryAttribution,
    ) -> Self {
        Self {
            principal,
            attribution,
        }
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

    pub(crate) fn attribution(&self) -> &QueryAttribution {
        &self.attribution
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::episode::EpisodeId;

    #[test]
    fn exposes_request_principal() {
        let principal = UserPrincipal::for_user("saul").expect("valid user");
        let context =
            RequestContext::with_attribution(principal.clone(), QueryAttribution::default());

        assert_eq!(context.principal(), &principal);
    }

    #[test]
    fn exposes_request_attribution() {
        let episode_id = EpisodeId::parse("ep_123").expect("episode id");
        let context = RequestContext::with_attribution(
            UserPrincipal::local(),
            QueryAttribution::new(Some(episode_id.clone())),
        );

        assert_eq!(context.attribution().episode_id(), Some(&episode_id));
    }
}
