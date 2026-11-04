---
status: accepted
---

# Credential Fields read from a Secret Store by reference

Self-hosting customers want credentials kept in their own secrets system, not in Coral's database. A Credential Field
can now hold a Secret Reference instead of a value: the value stays in the Secret Store, Coral reads it on every
Invocation, keeps it in memory for that call only, and never stores a copy. The first Secret Store adapter is HashiCorp
Vault and OpenBao KV v2; cloud secret managers fit behind the same interface later.

Rejected: Coral writing secrets to the customer's store (the customer still would not own the secret, and Coral would
need write access), and the customer holding Coral's encryption key in their KMS (Coral would still store every secret).
The second remains the planned answer for OAuth (below).

Write access fails hardest on Users' own credentials. A customer who wants every credential at rest under their control
would need Coral to write each User's OAuth tokens to the store too, and the store's sharing rules decide who else can
read them. Coral cannot see those rules: a Vault token reports only its own capabilities, and a shared 1Password vault
would expose one person's Slack token to every colleague with access. Only the operator's word would keep it private,
and a mistake there leaks personal credentials silently. Writing also adds failure modes that one database transaction
avoids today: a rotated refresh token lost when the store write fails, and a departed Member's tokens left behind when
the store is down at cleanup. A customer-held key meets the same need, every credential under a key the customer can
revoke, without Coral deciding where anyone's token lives. If a customer's rule demands the secret itself sit in their
store, revisit this decision, starting with Users' credentials.

## One reference per field

A reference names one field's value, not one entry for the whole Account. Vault KV entries and 1Password items hold
named keys, but Azure Key Vault, GCP Secret Manager and CyberArk Conjur hold one value per secret, so a per-Account
entry would force JSON into a single secret on those stores. A field may still be typed directly, such as Jira's
account email. A reference always reads the latest version, so rotation in the store takes effect on the next call.

## Static credentials only

OAuth Accounts keep their tokens in Coral. A refresh writes new tokens, and Coral does not write to a Secret Store. A
customer whose rule bars long-lived secrets outside their store can be met later by a customer-held key wrapping
Coral's encryption key, or by an OAuth client-credentials method whose client secret is a Secret Reference.

## Operator-defined stores, granted per Workspace

The operator defines each Secret Store at bootstrap: its address, how Coral authenticates (workload identity such as
Kubernetes, AppRole or AWS IAM) and a path prefix. Coral stores no credential for a store. An Organization Owner grants
stores to Workspaces in Reef, and a reference may name only a path under a granted store's prefix, so one Workspace
cannot read another's secrets through Coral's own access. Several stores allow one deployment to serve units with
separate Vault namespaces or cloud accounts. Removing a grant or a store makes its Accounts unusable without deleting
their references; granting it again restores them. Hosted Coral is out of scope: it would need the customer's store open
to our network and a stored credential for it.

## Only a Workspace Owner, in Reef

A Workspace Owner sets references, on a `fixed` Account or an Agent's `caller` Account. A User's own `caller` Account
does not take one, because a User could otherwise name a path holding another person's secret and Coral would read it.
A Program cannot set a reference, so an agent-written Program cannot probe the prefix through the setup check.

## Fail closed

Saving a reference reads it once to check it exists, then discards the value. At Invocation, an unreachable store, a
missing entry or a missing key fails the Invocation as an unusable Account, with an error that does not name the path;
Coral never falls back to another credential. Deleting an Account, or removing its Member or Agent, deletes only the
references; Coral never changes the store.
