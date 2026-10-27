---
status: accepted
---

# Screen Provider consent at issuance, separately from server requests

A Provider's authorization endpoint is where a browser goes, not something Coral fetches. Coral cannot enforce anything
about the browser's connection, so it screens each consent URL when it issues it and claims nothing beyond that. Server
requests keep the connection-time guard of ADR 0008, and `[outbound]` does not govern consent.

Consent may use plain HTTP to loopback or a private destination, because self-hosted services legitimately serve
consent pages there while Coral's own discovery and token requests stay on HTTPS. Coral rejects HTTP to a public
destination, a blocked special address under any scheme, and URLs that carry credentials or fragments. An HTTP hostname
must resolve, at issuance, only to the matching private or loopback range, and a failed lookup refuses issuance. HTTPS
names are not resolved; the browser verifies certificates as usual.

Coral's lookup is evidence at issuance, not proof of what the browser reaches: split DNS, rebinding and the provider's
own redirects can differ. Coral does not proxy consent or rewrite hostnames to addresses to close that gap.

A screened URL is an opaque value with no raw constructor. A stored URL is screened again every time it is issued, so an
earlier check never becomes permanent permission.
