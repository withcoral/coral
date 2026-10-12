//! Destination policy shared by publication and outbound transports.

use url::Url;

/// Permitted transports for one non-public destination category.
/// HTTPS always requires normal certificate verification.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NetworkAccess {
    /// No access.
    #[default]
    Deny,
    /// Verified HTTPS only.
    Https,
    /// HTTP or verified HTTPS.
    HttpAndHttps,
}

impl NetworkAccess {
    const fn permits(self, https: bool) -> bool {
        match self {
            Self::Deny => false,
            Self::Https => https,
            Self::HttpAndHttps => true,
        }
    }
}

/// Resolved access to untrusted destinations, independent of identity and listeners.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DestinationPolicy {
    loopback: NetworkAccess,
    private_networks: NetworkAccess,
}

impl DestinationPolicy {
    /// Public destinations remain HTTPS-only; special addresses remain blocked.
    #[must_use]
    pub const fn new(loopback: NetworkAccess, private_networks: NetworkAccess) -> Self {
        Self {
            loopback,
            private_networks,
        }
    }

    /// Selects loopback access without changing private-network permissions.
    #[must_use]
    pub const fn with_loopback(mut self, access: NetworkAccess) -> Self {
        self.loopback = access;
        self
    }

    /// Selects private-network access without changing loopback permissions.
    #[must_use]
    pub const fn with_private_networks(mut self, access: NetworkAccess) -> Self {
        self.private_networks = access;
        self
    }

    /// Checks transport and literal hosts. DNS must also be checked at connection time.
    /// # Errors
    /// Returns a safe rejection without including the destination.
    pub fn validate(self, url: &Url) -> Result<(), DestinationRejected> {
        if super::check_shape(url).is_err() {
            return Err(DestinationRejected);
        }
        let https = match url.scheme() {
            "https" => true,
            "http" => false,
            _ => return Err(DestinationRejected),
        };
        let host = url.host().ok_or(DestinationRejected)?;
        let class = classify_host(&host);
        let permitted = if super::is_explicit_loopback(url) {
            self.loopback.permits(https)
        } else if matches!(host, url::Host::Domain(_)) && class == AddressClass::Public {
            // Names are unresolved here. HTTP requires every connection-time
            // answer to be private; publication alone cannot prove reachability.
            https || self.private_networks.permits(false)
        } else {
            self.permits_remote_address(class, https)
        };
        if permitted {
            Ok(())
        } else {
            Err(DestinationRejected)
        }
    }

    #[cfg(feature = "transport")]
    pub(crate) const fn permits_loopback(self, https: bool) -> bool {
        self.loopback.permits(https)
    }

    pub(crate) fn permits_remote_address(self, class: AddressClass, https: bool) -> bool {
        match class {
            AddressClass::Public => https,
            AddressClass::Private => self.private_networks.permits(https),
            _ => false,
        }
    }
}

/// A safe destination-policy rejection; contains no URL, IP address, or upstream text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DestinationRejected;

impl std::fmt::Display for DestinationRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("outbound destination is not permitted")
    }
}
impl std::error::Error for DestinationRejected {}

#[cfg(test)]
mod tests {
    use super::*;

    /// Which permission, if any, admits a destination.
    #[derive(Clone, Copy, Debug)]
    enum Admits {
        /// Public HTTPS: always.
        Public,
        /// The loopback permission for this scheme.
        Loopback,
        /// The private-network permission for this scheme.
        Private,
        /// An unresolved name over HTTP: every answer must be private, so it
        /// needs private HTTP.
        PrivateHttpName,
        /// Nothing.
        Never,
    }

    #[test]
    fn each_destination_needs_only_its_own_category_permission() {
        use Admits::{Loopback, Never, Private, PrivateHttpName, Public};
        use NetworkAccess::{Deny, HttpAndHttps, Https};
        let mut destinations = [
            ("https://example.com/", Public),
            ("https://8.8.8.8/", Public),
            ("http://example.com/", PrivateHttpName),
            ("http://sidecar:5000/", PrivateHttpName),
            ("http://8.8.8.8/", Never),
            ("https://user:secret@example.com/", Never),
            ("https://user:secret@10.0.0.1/", Never),
            ("https://10.0.0.1/#fragment", Never),
        ]
        .map(|(address, admits)| (address.to_owned(), admits))
        .to_vec();
        for scheme in ["http", "https"] {
            for host in ["localhost", "127.0.0.1", "[::1]", "[::ffff:127.0.0.1]"] {
                destinations.push((format!("{scheme}://{host}/"), Loopback));
            }
            for host in ["10.0.0.1", "172.16.0.1", "192.168.1.1", "[fd00::1]"] {
                destinations.push((format!("{scheme}://{host}/"), Private));
            }
            for host in [
                "a.localhost",
                "169.254.169.254",
                "100.64.0.1",
                "[fe80::1]",
                "[::ffff:10.0.0.1]",
                "[64:ff9b::a00:1]",
                "224.0.0.1",
            ] {
                destinations.push((format!("{scheme}://{host}/"), Never));
            }
        }
        for loopback in [Deny, Https, HttpAndHttps] {
            for private in [Deny, Https, HttpAndHttps] {
                let policy = DestinationPolicy::new(loopback, private);
                for (address, admits) in &destinations {
                    let url = Url::parse(address).expect("URL");
                    let https = url.scheme() == "https";
                    let expected = match admits {
                        Public => true,
                        Loopback => loopback.permits(https),
                        Private => private.permits(https),
                        PrivateHttpName => private.permits(false),
                        Never => false,
                    };
                    assert_eq!(
                        policy.validate(&url).is_ok(),
                        expected,
                        "{address} under {policy:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn resolved_answers_are_judged_as_the_address_they_carry() {
        use AddressClass::{LinkLocal, Loopback, Private, Public, Special};
        let answers = [
            ("8.8.8.8", Public),
            ("2001:4860:4860::8888", Public),
            ("::ffff:8.8.8.8", Public),
            ("10.0.0.1", Private),
            ("172.16.0.1", Private),
            ("192.168.1.1", Private),
            ("::ffff:10.0.0.1", Private),
            ("fd00::1", Private),
            ("127.0.0.1", Loopback),
            ("::1", Loopback),
            ("::ffff:127.0.0.1", Loopback),
            ("169.254.169.254", LinkLocal),
            ("fe80::1", LinkLocal),
            ("100.64.0.1", Special),
            ("::ffff:100.64.0.1", Special),
            ("0.0.0.0", Special),
            ("::", Special),
            ("64:ff9b::a00:1", Special),
        ];
        for (answer, class) in answers {
            let ip: std::net::IpAddr = answer.parse().expect("address");
            assert_eq!(classify_resolved_address(ip), class, "{answer}");
        }
        // The connection-time guard keeps refusing every mapped answer.
        for mapped in ["::ffff:8.8.8.8", "::ffff:10.0.0.1"] {
            let ip: std::net::IpAddr = mapped.parse().expect("address");
            assert_eq!(classify_address(ip), Special, "{mapped}");
        }
    }
}

/// Address class used by URL validation and the connection-time DNS guard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AddressClass {
    /// Ordinary unicast or an IANA globally reachable allocation.
    Public,
    /// An explicit loopback name or address.
    Loopback,
    /// An address scoped to its network link.
    LinkLocal,
    /// Private-use address space.
    Private,
    /// Other special-purpose, multicast, or IPv4-transition addresses.
    Special,
}

/// Classifies a parsed URL host without resolving a hostname.
/// A public hostname still needs DNS validation when connecting.
#[must_use]
pub fn classify_host(host: &url::Host<&str>) -> AddressClass {
    match host {
        url::Host::Domain(name) => {
            let name = name.trim_end_matches('.').to_ascii_lowercase();
            if name == "localhost" || name.ends_with(".localhost") {
                AddressClass::Loopback
            } else {
                AddressClass::Public
            }
        }
        url::Host::Ipv4(ip) => classify_address((*ip).into()),
        url::Host::Ipv6(ip) => classify_address((*ip).into()),
    }
}

/// Classifies one IP address using the shared special-purpose denylist.
#[must_use]
pub fn classify_address(ip: std::net::IpAddr) -> AddressClass {
    use std::net::IpAddr;
    if crate::is_loopback_ip(ip) {
        return AddressClass::Loopback;
    }
    match ip {
        IpAddr::V4(ip) if ip.is_link_local() => AddressClass::LinkLocal,
        IpAddr::V6(ip) if ip.is_unicast_link_local() => AddressClass::LinkLocal,
        IpAddr::V4(ip) if ip.is_private() => AddressClass::Private,
        IpAddr::V6(ip) if ip.is_unique_local() => AddressClass::Private,
        ip if crate::public_metadata_ip_is_blocked(ip) => AddressClass::Special,
        _ => AddressClass::Public,
    }
}

/// Classifies one DNS answer for a reachability assessment, judging an
/// IPv4-mapped IPv6 answer as the IPv4 address it carries.
///
/// A dual-stack resolver may spell `10.0.0.1` as `::ffff:10.0.0.1`; the verdict
/// must not depend on that spelling. This is a heuristic input, not a
/// connection-time guard: [`classify_address`] still refuses every mapped
/// address as a destination, so a mapped public answer cannot smuggle an
/// IPv4 destination past the IPv4 table.
#[must_use]
pub fn classify_resolved_address(ip: std::net::IpAddr) -> AddressClass {
    classify_address(ip.to_canonical())
}
