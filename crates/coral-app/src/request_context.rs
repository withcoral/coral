//! Request-scoped app context selected at the transport boundary.

use crate::identity::Principal;

/// Request-scoped data that domain services may need after authentication.
///
/// This intentionally starts narrow. Query attribution still flows through its
/// existing path today, but can move here later without widening every
/// principal-aware call signature again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestContext {
    principal: Principal,
}

impl RequestContext {
    pub(crate) fn new(principal: Principal) -> Self {
        Self { principal }
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "request principals stay encapsulated for downstream authorization and attribution"
        )
    )]
    pub(crate) fn principal(&self) -> &Principal {
        &self.principal
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
    }
}
