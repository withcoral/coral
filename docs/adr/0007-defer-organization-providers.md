---
status: accepted
---

# Use Workspace-local Providers and Accounts in v1

V1 has one independently identified, Workspace-owned Provider with its own immutable namespace, configured catalog, account setting, Operation policy, and lifecycle. Central catalog distribution and shared organization credentials reduce repeated setup, but their ownership, availability, account-selection, update, and administration concepts impose implementation and UX costs that are not justified for v1. Accept separate setup and maintenance in each Workspace.

This removed Organization Providers, availability and distribution, the separate Provider Installation resource, independently selectable Provider Identities, and Provider Identity Bindings. No dormant ownership variants, compatibility interfaces, flags, or future-library scaffolding remain. Organization membership, inherited Workspace Owner authority, and private-work visibility were left unchanged. [ADR 0015](0015-workspace-authority-from-membership.md) later removed inherited Workspace Owner authority; it supersedes that clause and the inherited-access exception under Removal and metadata.

## Account locality instead of reuse

A Provider uses no account, one shared Provider Account, or one Provider Account per caller User or Agent. The Account is a local authorization record with protected credentials, not a verified upstream identity or selectable inventory. An additional simultaneous account for the same User or Agent—or a second shared account—requires another Provider. The immutable Provider namespace identifies its Program call paths; a mutable display name supplies presentation. Removing and recreating a namespace transfers neither identity, policy, nor accounts.

Effective Workspace Owners manage shared and Agent accounts; Users manage their own caller accounts. An owner completes OAuth or secret entry for an explicitly selected Agent without impersonating a Coral User or copying that User's stored credentials. Reef presents "One shared account" or "Separate accounts" and makes Agent detail the primary provisioning surface. The provisioning human's departure does not disable shared or Agent accounts.

## Separate discovery authorization from runtime use

Add first discovers operations, authenticating only if discovery requires it, and then asks how callers authenticate for execution. Discovery authentication is temporary setup material, not a permanent discovery Account. Explicitly retaining compatible discovery credentials as the shared or creating User's Account avoids signing in twice; it never silently provisions an Agent or turns discovery into shared runtime authority.

Shared-account creation must connect the shared account before publication. Caller-mode creation need not connect the creator's personal account, because that User may be configuring the Provider for others or an Agent. One owner-reviewed catalog is shared within the Workspace; upstream permission differences do not create per-caller catalogs or promise every caller can execute every published Operation.

Update may explicitly borrow the existing shared Account or the updating owner's own caller Account when compatible, but never another User's or an Agent's Account. New temporary grants are scrubbed after use; existing borrowed runtime accounts are not temporary material to delete.

## Atomic changes, not hidden account switching

Owners may change account settings without replacing the Provider or its policy. Commit a switch to shared only after compatible authorization succeeds, then clear credentials belonging to the previous setting with an explicit warning. Retaining dormant accounts for a later switch back was rejected: it recreates hidden credential inventory.

Reconnect preserves current credentials until a successful atomic replacement; failure leaves them unchanged from Coral's perspective. Disconnect is immediate and distinct. Concurrent flows complete only against their expected account/configuration evidence; first successful replacement wins and late callbacks cannot reverse Disconnect. Upstream services may independently affect older grants during authorization, so Coral does not promise upstream isolation or verified account continuity.

An authentication-changing shared-account update publishes its replacement Account and configuration atomically. Separate-account updates warn and require affected Users and Agents to reconnect individually. Changed authentication requirements end obsolete connection requests and fail their waiters rather than storing an immediately incompatible Account. Already-authorized Invocations retain their selected Generation and normal live authority checks.

Routine OAuth refresh stays within the same authorization and does not invalidate pending Invocations or approvals, including the Invocation that triggered refresh. This replaced a generation fence that failed the triggering Invocation after a successful refresh. Explicit reconnect, Disconnect, and account-setting changes still fence affected pending work, and conditional refresh completion cannot resurrect removed credentials. No already-dispatched Operation is retried.

## Removal and metadata

When membership removal ends effective Workspace access, clear that User's stored Provider Account credentials there; inherited Organization Owner access prevents treating an explicit membership removal as loss of access. Temporary disablement retains credentials but blocks use. Provider or Agent removal clears its account credentials while preserving historical attribution.

`coral-administration::AccessLifecycleModule` owns Workspace membership removal/leave, Organization membership removal, and Agent deletion above Workspace, Provider Accounts, and Functions. Each is one domain-owned transaction. It replaced Agent deletion inside Run, a Workspace membership removal that committed on its own, and a separate Saved Function membership cascade. Workspace supplies opaque prepared authority/effective-access evidence; Accounts and Functions join its transaction for cleanup and lifecycle events, then Workspace applies the prepared authority removal before Administration commits. Lower modules do not depend on Administration, and adapters call one command. This places the cross-domain rule in one module instead of creating separate account and Saved Function offboarding paths.

Record latest successful connection time and permitted configuring-User attribution without calling that User the upstream account. Incorporate upstream account ID/label only when an existing trustworthy source makes it straightforward. The current OAuth result carries no upstream identity, so provider-specific lookups and identity verification remain follow-up work.
