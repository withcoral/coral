---
status: accepted
---

# Separate outbound permissions from identity and listener exposure

Coral must be able to call operator-approved sidecars on loopback or a private network, and that permission must not
follow from who is signed in or whether authentication is on. Two independent settings, `outbound.loopback` and
`outbound.private_networks`, each take `deny`, `https` or `http-and-https`. Both default to `deny`, and public
destinations are always HTTPS-only. A composition may supply its own defaults: the implicit-user local runtime and
Desktop allow both, while ordinary servers do not, even without authentication. An operator's explicit setting wins in
every composition. This replaced a local-only loopback rule and a boolean private-network switch, which together made
authentication a hidden network-policy switch.

The guard runs at connection time against every DNS answer and every redirect, so a name that later resolves elsewhere
is still checked. Private means RFC 1918 and unique-local IPv6, never metadata, link-local or CGNAT ranges. Loopback
means explicit `localhost` or a literal, not any name that resolves there.

Enabling a category trusts Provider administrators and imported Provider documents with that reach. Address
classification is not network isolation: private HTTP can be intercepted, and a public address can host an internal
service.

Provider consent pages are browser destinations and follow ADR 0009, not these settings.
