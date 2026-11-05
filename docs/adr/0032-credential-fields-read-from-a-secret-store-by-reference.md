---
status: accepted
---

# Credential Fields read from a Secret Store by reference

Self-hosting customers want credentials kept in their own secrets system, not in Coral's database. A Credential Field
can hold a Secret Reference instead of a value: the value stays in the Secret Store, Coral reads it for each upstream
dispatch after the Policy Enforcement Point permits it, keeps it in memory for that call only, and never stores a copy.
An Invocation that is denied never touches the store. Each secrets system's adapter sits behind one interface.

Rejected: Coral writing secrets to the customer's store (the customer still would not own the secret, and Coral would
need write access), and the customer holding Coral's encryption key in their KMS as the answer to this request (Coral
would still store every secret). The second remains the planned answer for OAuth (below).

Write access fails hardest on Users' own credentials. A customer who wants every credential at rest under their control
would need Coral to write each User's OAuth tokens to the store too, and the store's sharing rules decide who else can
read them. Coral cannot see those rules: a Vault token reports only its own capabilities, and a shared 1Password vault
would expose one person's Slack token to every colleague with access. Only the store administrator's word would keep it
private, and a mistake there leaks personal credentials silently. A store write also adds failure points of its own: a
rotated refresh token lost when that write fails, and a departed Member's tokens left behind when the store is down at
cleanup. A customer-held key gives customer-controlled encryption, every credential under a key the customer can revoke,
without Coral deciding where anyone's token lives. It leaves the encrypted secret in Coral's database. If a customer's
rule demands the secret itself sit in their store, revisit this decision, starting with Users' credentials.

## One reference per field

A reference names one field's value, not one entry for the whole Account. Vault KV entries and 1Password items hold
named keys, but Azure Key Vault, GCP Secret Manager and CyberArk Conjur hold one value per secret, so a per-Account
entry would force JSON into a single secret on those stores. A field may still be typed directly, such as Jira's
account email. A reference always reads the latest version, so rotation in the store takes effect on the next call.

## Changing a reference, and rotating a secret

Changing a reference is explicit Account replacement: it gives the Account a new authorization version and stops pending
work, because an Invocation Approval binds the exact authorization. Rotating a secret in the store is not, because Coral
cannot see it. Rotation behaves like a routine token refresh under the same authorization, so an approved Invocation
sends the value current at dispatch. That is a known limit: if the new value belongs to a different upstream account, the
approval does not catch it.

## Static credentials only

OAuth Accounts keep their tokens in Coral. A refresh writes new tokens, and Coral does not write to a Secret Store. A
customer who wants control of those tokens can be met later by a customer-held key wrapping Coral's encryption key,
which is customer-controlled encryption and still stores them in Coral's database. An OAuth client-credentials method
whose client secret is a Secret Reference would store no long-lived token, but it suits machine access only, not a
User's own tokens.

## Operator-defined stores, granted per Workspace

The operator defines each Secret Store at bootstrap: its address, how Coral authenticates (workload identity such as
Kubernetes, AppRole or AWS IAM) and any setting needed to locate secrets, such as a KV mount. Coral stores no credential
for a store. An Organization Owner grants stores to Workspaces in Reef, and a reference may name only a granted store.
Granting shows the Organization Owner nothing inside the Workspace, so it sits with their governance surface in ADR
0015. Rejected: Workspace authority for the grant, which would let a Workspace Owner grant themselves any store.

The store's own policy is the security boundary, because the customer's security team must be able to own and understand
it without auditing Coral's code. Each store is one trust boundary with its own least-privilege identity, so Workspaces
that must not see each other's secrets use separate stores, and a grant covers everything that store's identity can
read. Coral cannot detect an identity that is too broad; that stays with the customer's policy.

Rejected: a path prefix that Coral checks on each store. Not every secrets system has hierarchical paths, so the setting
would not carry across stores, and a string check in Coral looks like a boundary without being one the customer can
verify. It would add clearer errors and fewer denials in the store's audit log, which is little gain for that illusion.

Several stores also allow one deployment to serve units with separate Vault namespaces or cloud accounts. Removing a
grant or a store makes its Accounts unusable without deleting their references; granting it again restores them. Hosted
Coral is out of scope: it would need the customer's store open to our network and a stored credential for it.

## Only a Workspace Owner, in Reef

A Workspace Owner sets references, on a shared Account (the `fixed` Credential Route) or an Agent's Account (`caller`).
A User's own Account does not take one, because a User could otherwise name a path holding another person's secret and
Coral would read it. A Program cannot set a reference, so an agent-written Program cannot probe the store through the
setup check.

## Fail closed

Saving a reference reads it once to check it exists, then discards the value. At dispatch, an unreachable store, a
missing entry or a missing key fails the Invocation as an unusable Account, with an error that does not name the path;
Coral never falls back to another credential. Deleting an Account, or removing its Member or Agent, deletes its typed
values and references; Coral never changes the store.
