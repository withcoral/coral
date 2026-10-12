//! Which automatic OAuth client identity a new setup-scoped Authorization uses.
//!
//! A Client ID Metadata Document names Coral by a URL the authorization server
//! must fetch; Dynamic Client Registration sends Coral's details to the server
//! instead. A server that advertises document support cannot necessarily reach a
//! document published on a tailnet or a private network, so automatic selection
//! assesses what Coral's own metadata URL resolves to here before choosing. The
//! assessment is a compatibility heuristic over the deployment's normal resolver,
//! not an outbound security rule: a public answer does not prove the server can
//! fetch the document, and a non-public answer does not prove it cannot (split
//! DNS). An operator policy overrides the heuristic either way. None of this
//! touches an explicit client, a pinned contract, or a retained registration.

use std::time::Duration;

use coral_outbound::{AddressClass, classify_host, classify_resolved_address};

use super::consent::Resolver;
use crate::Error;

/// Operator policy for choosing a new automatic client identity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ClientBootstrapPolicy {
    /// A metadata document when the server supports it and the document URL
    /// resolves to public addresses; otherwise registration.
    #[default]
    Automatic,
    /// A metadata document whenever the server supports it, whatever the local
    /// resolver answers; registration only when the server offers no document
    /// support. For split DNS and servers with approved access to an internal
    /// document.
    PreferClientIdMetadataDocument,
    /// Registration only. Without a registration endpoint there is no
    /// automatic client, and a failed registration never falls back.
    DynamicClientRegistrationOnly,
}

/// What the deployment's resolver says about Coral's metadata document URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientMetadataAssessment {
    /// Every returned address is globally public.
    Public,
    /// The host is a non-public literal, a localhost name, or at least one
    /// returned address is private, loopback, CGNAT, link-local or otherwise
    /// special-use.
    NonPublic,
    /// The lookup timed out, failed, or returned no usable address. Not a
    /// confirmed non-public destination.
    Unknown,
}

/// The automatic bootstrap one policy selects for one discovered server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AutomaticBootstrap {
    /// The metadata document URL is the client identifier.
    MetadataDocument(String),
    /// Register a public client at the discovered registration endpoint.
    Registration(String),
}

/// Bound on one assessment lookup; a slower answer counts as unknown.
const ASSESSMENT_TIMEOUT: Duration = Duration::from_secs(3);

/// Chooses the automatic bootstrap for a server that advertises document
/// support (`document_supported`) and possibly a `registration_endpoint`,
/// given the metadata document Coral publishes, if any.
///
/// Only the automatic policy with a usable document consults the resolver,
/// once, through one bounded lookup. Nothing is persisted.
///
/// # Errors
/// `ClientBootstrapUnavailable` when the policy and the server leave nothing
/// to bootstrap from; `ClientMetadataNotPublic` or `ClientMetadataUnresolved`
/// when the document is the server's only offer and its assessment cannot
/// establish a public address.
pub(crate) async fn select(
    policy: ClientBootstrapPolicy,
    document: Option<&str>,
    document_supported: bool,
    registration_endpoint: Option<&str>,
    resolver: &dyn Resolver,
) -> Result<AutomaticBootstrap, Error> {
    let document = document.filter(|_| document_supported);
    let registration = || {
        registration_endpoint
            .map(str::to_owned)
            .map(AutomaticBootstrap::Registration)
    };
    let metadata = |document: &str| AutomaticBootstrap::MetadataDocument(document.to_owned());
    match policy {
        ClientBootstrapPolicy::DynamicClientRegistrationOnly => {
            registration().ok_or(Error::ClientBootstrapUnavailable)
        }
        ClientBootstrapPolicy::PreferClientIdMetadataDocument => document
            .map(metadata)
            .or_else(registration)
            .ok_or(Error::ClientBootstrapUnavailable),
        ClientBootstrapPolicy::Automatic => {
            let Some(document) = document else {
                return registration().ok_or(Error::ClientBootstrapUnavailable);
            };
            match assess(resolver, document, ASSESSMENT_TIMEOUT).await {
                ClientMetadataAssessment::Public => Ok(metadata(document)),
                ClientMetadataAssessment::NonPublic => {
                    registration().ok_or(Error::ClientMetadataNotPublic)
                }
                ClientMetadataAssessment::Unknown => {
                    registration().ok_or(Error::ClientMetadataUnresolved)
                }
            }
        }
    }
}

/// Whether a published document may be offered as an automatic choice before
/// any lookup: the policy admits it and the URL is not a non-public literal or
/// localhost name. A name's resolver verdict is only known when a flow starts.
pub(crate) fn may_offer_metadata_document(policy: ClientBootstrapPolicy, document: &str) -> bool {
    match policy {
        ClientBootstrapPolicy::DynamicClientRegistrationOnly => false,
        ClientBootstrapPolicy::PreferClientIdMetadataDocument => true,
        ClientBootstrapPolicy::Automatic => {
            literal_assessment(document) != Some(ClientMetadataAssessment::NonPublic)
        }
    }
}

/// Assesses the metadata document URL. An IP literal or a localhost name is
/// judged without DNS; a name goes through one lookup bounded by `timeout`,
/// and every returned address must be public.
async fn assess(
    resolver: &dyn Resolver,
    document: &str,
    timeout: Duration,
) -> ClientMetadataAssessment {
    if let Some(verdict) = literal_assessment(document) {
        return verdict;
    }
    let Some(name) = host_name(document) else {
        return ClientMetadataAssessment::Unknown;
    };
    let answers = match tokio::time::timeout(timeout, resolver.resolve(&name)).await {
        Ok(Ok(answers)) if !answers.is_empty() => answers,
        Ok(Ok(_) | Err(())) | Err(_) => return ClientMetadataAssessment::Unknown,
    };
    if answers
        .into_iter()
        .all(|ip| classify_resolved_address(ip) == AddressClass::Public)
    {
        ClientMetadataAssessment::Public
    } else {
        ClientMetadataAssessment::NonPublic
    }
}

/// The verdict a URL carries on its face: an unparsable URL is unknown, a
/// literal address is judged directly, a localhost name is non-public without
/// consulting DNS, and any other name has no verdict yet.
fn literal_assessment(document: &str) -> Option<ClientMetadataAssessment> {
    let url = url::Url::parse(document).ok()?;
    let class = match url.host()? {
        url::Host::Ipv4(ip) => classify_resolved_address(ip.into()),
        url::Host::Ipv6(ip) => classify_resolved_address(ip.into()),
        host @ url::Host::Domain(_) => match classify_host(&host) {
            AddressClass::Loopback => return Some(ClientMetadataAssessment::NonPublic),
            _ => return None,
        },
    };
    Some(if class == AddressClass::Public {
        ClientMetadataAssessment::Public
    } else {
        ClientMetadataAssessment::NonPublic
    })
}

fn host_name(document: &str) -> Option<String> {
    match url::Url::parse(document).ok()?.host()? {
        url::Host::Domain(name) => Some(name.to_owned()),
        url::Host::Ipv4(_) | url::Host::Ipv6(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use super::*;

    /// A resolver with one scripted outcome.
    enum Dns {
        Answers(Vec<&'static str>),
        Failure,
        Hang,
    }

    #[async_trait::async_trait]
    impl Resolver for Dns {
        async fn resolve(&self, _host: &str) -> Result<Vec<IpAddr>, ()> {
            match self {
                Self::Answers(answers) => {
                    Ok(answers.iter().map(|ip| ip.parse().unwrap()).collect())
                }
                Self::Failure => Err(()),
                Self::Hang => std::future::pending().await,
            }
        }
    }

    const DOCUMENT: &str = "https://coral.example/.well-known/oauth-client";

    #[tokio::test]
    async fn every_returned_address_must_be_public() {
        use ClientMetadataAssessment::{NonPublic, Public, Unknown};
        let cases: [(&str, Dns, ClientMetadataAssessment); 15] = [
            ("public IPv4", Dns::Answers(vec!["93.184.216.34"]), Public),
            (
                "public IPv6",
                Dns::Answers(vec!["2606:2800:220:1:248:1893:25c8:1946"]),
                Public,
            ),
            (
                "dual stack public",
                Dns::Answers(vec!["93.184.216.34", "2606:2800:220:1::1"]),
                Public,
            ),
            (
                "mapped public IPv4",
                Dns::Answers(vec!["::ffff:93.184.216.34"]),
                Public,
            ),
            ("RFC1918", Dns::Answers(vec!["10.1.2.3"]), NonPublic),
            ("loopback", Dns::Answers(vec!["127.0.0.1"]), NonPublic),
            ("CGNAT", Dns::Answers(vec!["100.64.0.7"]), NonPublic),
            (
                "IPv6 unique-local",
                Dns::Answers(vec!["fd12::1"]),
                NonPublic,
            ),
            ("IPv6 link-local", Dns::Answers(vec!["fe80::1"]), NonPublic),
            (
                "mapped private IPv4",
                Dns::Answers(vec!["::ffff:192.168.0.2"]),
                NonPublic,
            ),
            (
                "mixed public and CGNAT",
                Dns::Answers(vec!["93.184.216.34", "100.64.0.7"]),
                NonPublic,
            ),
            ("unspecified", Dns::Answers(vec!["0.0.0.0"]), NonPublic),
            ("empty answer", Dns::Answers(vec![]), Unknown),
            ("resolver failure", Dns::Failure, Unknown),
            ("timeout", Dns::Hang, Unknown),
        ];
        for (case, dns, expected) in cases {
            let verdict = assess(&dns, DOCUMENT, Duration::from_millis(20)).await;
            assert_eq!(verdict, expected, "{case}");
        }
    }

    #[tokio::test]
    async fn literals_and_localhost_are_judged_without_dns() {
        use ClientMetadataAssessment::{NonPublic, Public, Unknown};
        let cases = [
            ("https://93.184.216.34/.well-known/oauth-client", Public),
            (
                "https://[2606:2800:220:1::1]/.well-known/oauth-client",
                Public,
            ),
            (
                "https://[::ffff:93.184.216.34]/.well-known/oauth-client",
                Public,
            ),
            ("https://10.0.0.1/.well-known/oauth-client", NonPublic),
            ("https://100.64.0.1/.well-known/oauth-client", NonPublic),
            ("https://[fd00::1]/.well-known/oauth-client", NonPublic),
            ("https://127.0.0.1/.well-known/oauth-client", NonPublic),
            ("https://localhost/.well-known/oauth-client", NonPublic),
            (
                "https://coral.localhost/.well-known/oauth-client",
                NonPublic,
            ),
            ("https://LOCALHOST./.well-known/oauth-client", NonPublic),
            ("not a url", Unknown),
        ];
        // A hanging resolver proves none of these consult DNS.
        for (document, expected) in cases {
            let verdict = assess(&Dns::Hang, document, Duration::from_secs(30)).await;
            assert_eq!(verdict, expected, "{document}");
        }
        assert!(may_offer_metadata_document(
            ClientBootstrapPolicy::Automatic,
            DOCUMENT
        ));
        assert!(!may_offer_metadata_document(
            ClientBootstrapPolicy::Automatic,
            "https://localhost/.well-known/oauth-client"
        ));
        assert!(may_offer_metadata_document(
            ClientBootstrapPolicy::PreferClientIdMetadataDocument,
            "https://localhost/.well-known/oauth-client"
        ));
        assert!(!may_offer_metadata_document(
            ClientBootstrapPolicy::DynamicClientRegistrationOnly,
            DOCUMENT
        ));
    }

    /// What Coral publishes and what the server offers.
    #[derive(Clone, Copy)]
    struct Offer {
        published: bool,
        supported: bool,
        registration: bool,
    }
    const BOTH: Offer = Offer {
        published: true,
        supported: true,
        registration: true,
    };
    const DOCUMENT_ONLY: Offer = Offer {
        published: true,
        supported: true,
        registration: false,
    };
    const UNSUPPORTED: Offer = Offer {
        published: true,
        supported: false,
        registration: true,
    };
    const UNPUBLISHED: Offer = Offer {
        published: false,
        supported: true,
        registration: true,
    };
    const UNPUBLISHED_ONLY: Offer = Offer {
        published: false,
        supported: true,
        registration: false,
    };
    const NOTHING: Offer = Offer {
        published: true,
        supported: false,
        registration: false,
    };

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one table is the whole selection matrix"
    )]
    async fn the_selection_matrix() {
        use AutomaticBootstrap::{MetadataDocument, Registration};
        use ClientBootstrapPolicy::{
            Automatic, DynamicClientRegistrationOnly, PreferClientIdMetadataDocument,
        };
        const REGISTER: &str = "https://issuer.example/register";
        let public = || Dns::Answers(vec!["93.184.216.34"]);
        let private = || Dns::Answers(vec!["100.64.0.7"]);
        let failing = || Dns::Failure;
        let document = || Ok(MetadataDocument(DOCUMENT.to_owned()));
        let registration = || Ok(Registration(REGISTER.to_owned()));
        let cases = [
            ("public + both", Automatic, BOTH, public(), document()),
            (
                "public + document only",
                Automatic,
                DOCUMENT_ONLY,
                public(),
                document(),
            ),
            (
                "non-public + both",
                Automatic,
                BOTH,
                private(),
                registration(),
            ),
            ("unknown + both", Automatic, BOTH, failing(), registration()),
            (
                "non-public, document only",
                Automatic,
                DOCUMENT_ONLY,
                private(),
                Err(Error::ClientMetadataNotPublic),
            ),
            (
                "unknown, document only",
                Automatic,
                DOCUMENT_ONLY,
                failing(),
                Err(Error::ClientMetadataUnresolved),
            ),
            (
                "unsupported document",
                Automatic,
                UNSUPPORTED,
                private(),
                registration(),
            ),
            (
                "no document published",
                Automatic,
                UNPUBLISHED,
                private(),
                registration(),
            ),
            (
                "nothing offered",
                Automatic,
                NOTHING,
                public(),
                Err(Error::ClientBootstrapUnavailable),
            ),
            (
                "unpublished, no registration",
                Automatic,
                UNPUBLISHED_ONLY,
                public(),
                Err(Error::ClientBootstrapUnavailable),
            ),
            (
                "prefer document despite non-public",
                PreferClientIdMetadataDocument,
                BOTH,
                private(),
                document(),
            ),
            (
                "prefer document despite unknown",
                PreferClientIdMetadataDocument,
                DOCUMENT_ONLY,
                failing(),
                document(),
            ),
            (
                "prefer document, unsupported",
                PreferClientIdMetadataDocument,
                UNSUPPORTED,
                private(),
                registration(),
            ),
            (
                "prefer document, nothing",
                PreferClientIdMetadataDocument,
                UNPUBLISHED_ONLY,
                public(),
                Err(Error::ClientBootstrapUnavailable),
            ),
            (
                "registration only, public",
                DynamicClientRegistrationOnly,
                BOTH,
                public(),
                registration(),
            ),
            (
                "registration only, no endpoint",
                DynamicClientRegistrationOnly,
                DOCUMENT_ONLY,
                public(),
                Err(Error::ClientBootstrapUnavailable),
            ),
        ];
        for (case, policy, offer, dns, expected) in cases {
            let chosen = select(
                policy,
                offer.published.then_some(DOCUMENT),
                offer.supported,
                offer.registration.then_some(REGISTER),
                &dns,
            )
            .await;
            match (&chosen, &expected) {
                (Ok(chosen), Ok(expected)) => assert_eq!(chosen, expected, "{case}"),
                (Err(chosen), Err(expected)) => assert_eq!(
                    std::mem::discriminant(chosen),
                    std::mem::discriminant(expected),
                    "{case}: {chosen:?}"
                ),
                _ => panic!("{case}: {chosen:?}, expected {expected:?}"),
            }
        }
    }
}
