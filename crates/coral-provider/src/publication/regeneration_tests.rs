use coral_catalog::{
    AuthenticationConfiguration, OAuthClientAuthenticationMethod, OAuthClientBootstrap,
    OAuthClientPolicy, ProviderCredentialContract as Contract,
};

use super::regeneration::{
    Effect::{
        self, ActivateGeneration, CancelAuthorizations, ChangeRoute, ClearClientFailure,
        DropClientSecret, ReplaceSharedAccount,
    },
    Facts, plan,
};
use crate::CredentialRoute::{self, Caller, Fixed, None as NoCredential};
use crate::Error;

fn bearer() -> Contract {
    AuthenticationConfiguration::Bearer.into()
}

/// An OAuth contract whose application authenticates as `authentication`.
fn oauth(authentication: OAuthClientAuthenticationMethod) -> Contract {
    let application = OAuthClientPolicy {
        authentication,
        ..OAuthClientPolicy::public(OAuthClientBootstrap::Manual, "application".into())
    };
    Contract::from(AuthenticationConfiguration::OAuthAuthorizationCode {
        authorization_url: "https://issuer.example/authorize".into(),
        token_url: "https://issuer.example/token".into(),
        scopes: Vec::new(),
        scope_format: coral_catalog::OAuthScopeFormat::default(),
        resource: None,
        issuer: None,
        client: None,
    })
    .with_oauth_client(&application)
}

/// An OAuth contract whose application authenticates with a secret.
fn confidential() -> Contract {
    oauth(OAuthClientAuthenticationMethod::ClientSecretPost)
}

/// A catalog-only regeneration of a Provider on `route`, whose active and
/// replacement contracts are both `contract`. Each case changes what it tests.
fn unchanged(route: CredentialRoute, contract: &Contract) -> Facts<'_> {
    Facts {
        allows_incompatible_configuration: false,
        account_setting: None,
        account_grant: false,
        current_route: route,
        active_contract: contract,
        replacement_contract: contract,
        same_catalog: false,
    }
}

/// The same regeneration, with the owner's confirmation of what it changes.
fn confirmed(facts: Facts<'_>) -> Facts<'_> {
    Facts {
        allows_incompatible_configuration: true,
        ..facts
    }
}

/// Whether an error is the refusal a case expects.
type Refusal = fn(&Error) -> bool;

/// Asserts the route and the set of effects; the plan promises no order.
#[track_caller]
fn assert_plans(facts: Facts<'_>, route: CredentialRoute, effects: &[Effect]) {
    let plan = plan(facts).unwrap_or_else(|error| panic!("{facts:?} is refused: {error:?}"));
    assert_eq!(plan.route, route, "{facts:?}");
    assert!(
        plan.effects.len() == effects.len() && effects.iter().all(|effect| plan.includes(*effect)),
        "{facts:?} plans {:?}, not {effects:?}",
        plan.effects
    );
}

#[test]
fn a_regeneration_changes_only_what_differs_from_the_active_provider() {
    let (none, bearer, confidential) = (Contract::NONE, bearer(), confidential());

    // The catalog alone: a new Generation, or none when it is the active one.
    assert_plans(
        unchanged(Caller, &bearer),
        Caller,
        &[ActivateGeneration, DropClientSecret],
    );
    assert_plans(
        Facts {
            same_catalog: true,
            ..unchanged(Caller, &bearer)
        },
        Caller,
        &[DropClientSecret],
    );
    // A shared Account stays while its route and contract do.
    assert_plans(
        unchanged(Fixed, &bearer),
        Fixed,
        &[ActivateGeneration, DropClientSecret],
    );
    // A confirmed new contract ends the sign-ins begun under the old one.
    assert_plans(
        confirmed(Facts {
            replacement_contract: &confidential,
            ..unchanged(Caller, &bearer)
        }),
        Caller,
        &[ActivateGeneration, CancelAuthorizations, ClearClientFailure],
    );
    // A confirmed new route moves the Provider, with or without a new catalog.
    assert_plans(
        confirmed(Facts {
            account_setting: Some(Caller),
            same_catalog: true,
            ..unchanged(Fixed, &bearer)
        }),
        Caller,
        &[ChangeRoute, DropClientSecret],
    );
    assert_plans(
        confirmed(Facts {
            account_setting: Some(NoCredential),
            replacement_contract: &none,
            ..unchanged(Caller, &bearer)
        }),
        NoCredential,
        &[
            ActivateGeneration,
            ChangeRoute,
            CancelAuthorizations,
            ClearClientFailure,
            DropClientSecret,
        ],
    );
    // Naming the route the Provider is on changes nothing and needs no confirmation.
    assert_plans(
        Facts {
            account_setting: Some(Caller),
            ..unchanged(Caller, &bearer)
        },
        Caller,
        &[ActivateGeneration, DropClientSecret],
    );
    // A reviewed grant replaces the shared Account: on the route it is on, and
    // on the way to it.
    assert_plans(
        Facts {
            account_grant: true,
            ..unchanged(Fixed, &bearer)
        },
        Fixed,
        &[ActivateGeneration, ReplaceSharedAccount, DropClientSecret],
    );
    assert_plans(
        confirmed(Facts {
            account_setting: Some(Fixed),
            account_grant: true,
            ..unchanged(Caller, &bearer)
        }),
        Fixed,
        &[
            ActivateGeneration,
            ChangeRoute,
            ReplaceSharedAccount,
            DropClientSecret,
        ],
    );
    // A grant carries the shared Account over a confirmed new contract.
    assert_plans(
        confirmed(Facts {
            account_grant: true,
            replacement_contract: &confidential,
            ..unchanged(Fixed, &bearer)
        }),
        Fixed,
        &[
            ActivateGeneration,
            ReplaceSharedAccount,
            CancelAuthorizations,
            ClearClientFailure,
        ],
    );
}

#[test]
fn a_regeneration_keeps_an_application_secret_only_for_a_confidential_application() {
    let confidential = confidential();
    assert_plans(
        unchanged(Caller, &confidential),
        Caller,
        &[ActivateGeneration],
    );
    // A public application and a contract with no application have none to keep.
    for contract in [oauth(OAuthClientAuthenticationMethod::None), bearer()] {
        assert_plans(
            unchanged(Caller, &contract),
            Caller,
            &[ActivateGeneration, DropClientSecret],
        );
    }
}

#[test]
fn a_regeneration_is_refused_before_it_changes_anything() {
    let (none, bearer, confidential) = (Contract::NONE, bearer(), confidential());
    let new_contract = Facts {
        replacement_contract: &confidential,
        ..unchanged(Caller, &bearer)
    };
    let new_route = Facts {
        account_setting: Some(Fixed),
        account_grant: true,
        ..unchanged(Caller, &bearer)
    };
    let cases: [(&str, Facts<'_>, Refusal); 9] = [
        (
            "a new contract the owner did not confirm",
            new_contract,
            |error| matches!(error, Error::IncompatibleConfigurationConfirmationRequired),
        ),
        (
            "a new route the owner did not confirm",
            new_route,
            |error| matches!(error, Error::IncompatibleConfigurationConfirmationRequired),
        ),
        (
            "no credential for a source that needs one",
            confirmed(Facts {
                account_setting: Some(NoCredential),
                ..unchanged(Caller, &bearer)
            }),
            |error| matches!(error, Error::AccountRequired),
        ),
        (
            "a credential for a source that needs none",
            confirmed(Facts {
                replacement_contract: &none,
                ..unchanged(Caller, &bearer)
            }),
            |error| matches!(error, Error::AccountInput),
        ),
        (
            "a route that does not fit is refused before its grant is judged",
            confirmed(Facts {
                account_setting: Some(NoCredential),
                account_grant: true,
                ..unchanged(Caller, &bearer)
            }),
            |error| matches!(error, Error::AccountRequired),
        ),
        (
            "a grant for a route with no shared Account",
            Facts {
                account_grant: true,
                ..unchanged(Caller, &bearer)
            },
            |error| matches!(error, Error::AccountInput),
        ),
        (
            "a move to the shared Account without a grant",
            confirmed(Facts {
                account_setting: Some(Fixed),
                ..unchanged(Caller, &bearer)
            }),
            |error| matches!(error, Error::AccountRequired),
        ),
        (
            "a shared Account under a new contract without a grant",
            confirmed(Facts {
                replacement_contract: &confidential,
                ..unchanged(Fixed, &bearer)
            }),
            |error| matches!(error, Error::AccountRequired),
        ),
        (
            "the unconfirmed contract is refused before the route is judged",
            Facts {
                account_setting: Some(NoCredential),
                replacement_contract: &confidential,
                ..unchanged(Caller, &bearer)
            },
            |error| matches!(error, Error::IncompatibleConfigurationConfirmationRequired),
        ),
    ];
    for (case, facts, expected) in cases {
        let refusal = plan(facts).expect_err(case);
        assert!(expected(&refusal), "{case}: {refusal:?}");
    }
}
