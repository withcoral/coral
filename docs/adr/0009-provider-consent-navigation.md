---
status: accepted
---

# Screen Provider consent separately from server requests

Provider authorization endpoints are browser destinations, not Coral fetch targets. A browser can intentionally visit
an HTTP consent page while Coral requires HTTPS for discovery and token exchange; those endpoints need not share a
transport. No new configuration switches are introduced. `[outbound]` governs server requests only.

This clarifies and supersedes ADR 0008's unqualified “public destinations remain HTTPS-only” statement:

- **Server requests:** public HTTPS-only is enforced at connection time by the guarded client, including DNS answers.
- **Consent navigation:** Coral screens each initial URL at issuance time. This does not guarantee the browser's
  resolution, its eventual connection address, or a provider's subsequent redirects.

## Initial consent navigation contract

The endpoint comes from confirmed Provider configuration and an explicitly initiated authorization flow.

| Destination | Issuance rule |
| --- | --- |
| HTTPS | Accept safe URL shapes, subject to the blocked-literal rule below. No Coral-side DNS prerequisite. The browser retains normal certificate verification. |
| HTTP loopback literal | Accept. |
| HTTP explicit `localhost` | Require a fresh lookup whose answers are all loopback. |
| HTTP RFC1918 IPv4 or unique-local IPv6 literal | Accept. |
| Other HTTP hostname, including `sidecar` | Require a fresh Coral-side lookup whose answers are exclusively private. |
| HTTP public literal or public/mixed DNS answers | Reject. |
| Blocked-special-address literal | Reject **regardless of scheme**, including HTTPS. |
| Credentials, fragments, unsupported schemes or malformed URLs | Reject. |

The shared address classifier continues to block metadata/link-local, CGNAT, multicast and other special ranges.
Arbitrary names resolving to loopback receive no localhost exception. HTTP lookups have a three-second deadline and
at most 1,024 answers; failures, empty answers and oversized answers fail closed. No lookup result is retained as
navigation authority. HTTPS names are not classified by Coral DNS; blocked literal rejection is not a claim to
control what an HTTPS hostname resolves to in the browser.

Private HTTP consent can be intercepted and can reach services in the browser's network. It is an intentional
transport choice, not a network-isolation guarantee.

Coral DNS is evidence for issuance, not proof of browser reachability. Split DNS, rebinding, later record changes and
subsequent redirects can differ. Preserving private HTTP hostnames means accepting that limitation, not disguising a
preflight lookup as connection enforcement. Coral does not proxy consent or rewrite hostnames to IP addresses.
Registered OAuth callback URI rules and operator-configured sign-in endpoints retain their separate contracts.

## Enforceable issuance interface

Provider authorization produces an opaque `ConsentNavigation` only after screening. It has no raw URL constructor or
deserializer. Repository rows retain untrusted strings, never reusable checked authority. Pending Setup and Account
reads, starts and management-request replays screen the stored URL again before returning navigation evidence. DNS
runs outside database transactions. Terminal projections contain no consent navigation.

The private gRPC adapter can project navigation only from that domain type. The wire `ProviderConsentNavigation`
message is distinct from an authorization endpoint in configuration. Reef's reference-based redirect interface fetches
fresh authorization state and returns an HTTP response; it neither accepts nor exports a navigation URL string.
An architecture test confines wire consent consumption to that adapter, with only boolean availability permitted in
account presentation. Setup callback cookies and Desktop's external-browser return marker are attached after checked
issuance. Reuse follows the same path; the Desktop host does not evaluate a second destination policy.

A previous response is evidence of a check at that time, not permanent permission. Neither the domain type nor the
wire message claims that Coral controls a URL after it reaches the browser. A failed check returns refused issuance
and no navigation URL. Progress remains readable, so the User can edit, retry or cancel; refusal is not persisted as
an OAuth failure. The redirect adapter rejects that navigation with a safe error. Neither refusal nor successful
issuance relaxes server-egress permissions or changes the Invoking Principal.
