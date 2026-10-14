---
status: accepted
---

# Safe HTTP methods are enabled by default

OpenAPI/HTTP-sourced Operations declare no Operation Hints, so every one of them needed approval until a Workspace Owner
enabled it. Most of those Operations only read, and approving each read buried the approvals that matter. An Operation
with no exact rule now defaults to `enabled` when it declares itself read-only or when its HTTP method is safe in the
RFC 9110 sense: GET, HEAD, OPTIONS, or TRACE. Every other Operation still defaults to `approval_required`.

The method is a protocol fact, not a hint. Operation Hints remain provider claims, and Coral still never assumes one,
so a GET Operation shows no "Read-only" label. We rejected synthesizing a read-only hint from the method because it
would present Coral's guess as the Provider's claim, everywhere hints appear.

The default applies to every Operation without an exact rule, including those of existing Providers, with no migration.
On upgrade, those Operations stop asking for approval, and waiting Approval Requests for them proceed as if an Owner had
enabled them. We rejected writing explicit `approval_required` rules to keep the old behavior: they would appear as
Owner decisions that no Owner made. An upstream that changes state on a safe method needs an exact rule from its
Workspace Owner; there is no per-Provider opt-out.
