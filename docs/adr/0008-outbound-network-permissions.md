---
status: accepted
---

# Separate outbound permissions from identity and listener exposure

Authenticated Coral must be able to call operator-approved HTTP sidecars without changing the Invoking Principal.
Resolve two independent settings, `outbound.loopback` and `outbound.private_networks`, each accepting exactly `deny`,
`https`, or `http-and-https`. Both base defaults are `deny`. Explicit operator fields override composition defaults
independently; omitted fields are not persisted as operator choices. HTTPS always verifies certificates.

The runtime's implicit-user composition and Desktop launcher supply `http-and-https` defaults for both categories.
Ordinary `ServerBuilder` instances supply no override, even without authentication. The authenticated `./dev` launcher
also supplies no override. Tests that need fixture reachability select it explicitly. This keeps local use convenient
without making authentication a hidden outbound policy switch. Explicit restrictive settings win in every composition.
Incoming auth-disabled listeners remain loopback-only; the implicit Organization Owner and normal authorization remain.
Operator-configured sign-in issuer handling retains its separate endpoint contract.

For **Coral server requests**, public destinations remain HTTPS-only, enforced at connection time. Private means
RFC1918 IPv4 and unique-local IPv6, not CGNAT, metadata, link-local, or other special ranges. Loopback access remains limited to explicit localhost/literals, not arbitrary DNS names that
resolve to loopback. HTTP private hostnames are provisional at publication: every actual DNS answer must satisfy the
request's scheme/category permissions at connection time. Server-request redirects revalidate destinations and isolate credentials.

Provider consent navigation has independent permissions and issuance-time screening, not a guarded browser connection.
[ADR 0009](0009-provider-consent-navigation.md) supersedes the earlier unqualified public-HTTPS statement for consent.
It preserves intentional local/private HTTP navigation while rejecting known public HTTP destinations at issuance.
Browser DNS and subsequent redirects remain outside Coral's control.

Enabling a category trusts Provider administrators and imported Provider documents with that network reachability.
Internal services can be unauthenticated; private HTTP can be intercepted. Address classification is not network
isolation or complete lateral-movement prevention: a public IP can host operationally internal services.

## Superseded decisions

This decision supersedes the local-only loopback/public-HTTPS restriction and the boolean private-network
interface that preceded it, not their SSRF, DNS-rebinding, blocked-address or cross-origin credential protections. No old keys,
aliases or compatibility readers remain.

It also supersedes the earlier native host alias and persistent trust prerequisite for E2E. Browser and server E2E now
runs inside a disposable Linux container. [Reel](../../apps/reef/e2e/README.md) records that topology and how to remove
the previous host setup. See [development configuration](../development.md#outbound-provider-access) for operator
examples.
