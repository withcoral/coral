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
would still store every secret). A customer-held key still suits OAuth (below).

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
sends the value current at dispatch. That is a known limit: if the new value belongs to a different upstream account,
the approval does not catch it.

## Static credentials only

OAuth Accounts keep their tokens in Coral. A refresh writes new tokens, and Coral does not write to a Secret Store. A
customer who wants control of those tokens can be met later by a customer-held key wrapping Coral's encryption key,
which is customer-controlled encryption and still stores them in Coral's database. An OAuth client-credentials method
whose client secret is a Secret Reference would store no long-lived token, but it suits machine access only, not a
User's own tokens.

## Operator-defined stores, granted per Workspace

The operator defines each Secret Store at bootstrap: its address, how Coral authenticates (workload identity such as
Kubernetes, AppRole or AWS IAM) and any setting needed to locate secrets, such as a KV mount. Coral stores no credential
for a store. An Organization Owner grants stores to Workspaces in Reef and revokes those grants, and a reference may name
only a granted store.
Granting shows the Organization Owner nothing inside the Workspace, so it sits with their governance surface in ADR
0015. Rejected: Workspace authority for the grant, which would let a Workspace Owner grant themselves any store.

The store's own policy is the security boundary, because the customer's security team must be able to own and understand
it without auditing Coral's code. Each store is one trust boundary with its own least-privilege identity, so Workspaces
that must not see each other's secrets use separate stores, and a grant covers everything that store's identity can
read. Coral cannot detect an identity that is too broad; that stays with the customer's policy.

Rejected: a path prefix that Coral checks on each store. Not every secrets system has hierarchical paths, so the setting
would not carry across stores, and a string check in Coral looks like a boundary without being one the customer can
verify. It would add clearer errors and fewer denials in the store's audit log, which is little gain for that illusion.

A store's name stands for one trust boundary. An operator may change a store's address, for example to move the same
store to a new host, and Coral neither detects that nor stops pending work: approved work reads from the new address. To
point at a different store, an operator defines a new store under a new name. Like rotation, this stays outside what an
approval binds, and the operator already controls Coral. Rejected: pinning a store's settings. Coral would keep
deployment config in its database and refuse to start on a routine move, and it still could not see repointed DNS or
changed contents in the store.

Several stores also allow one deployment to serve units with separate Vault namespaces or cloud accounts. Removing a
grant or a store makes its Accounts unusable without deleting their references; granting it again restores them. Hosted
Coral is out of scope: it would need the customer's store open to our network and a stored credential for it.

## Store kinds, and which need the Enterprise Edition

Each store has a kind, the secrets system its adapter speaks, such as Vault KV version 2. Core keeps Secret Stores,
grants, Secret References and their schema. A build registers its kinds, and core parses only a store's name and kind
and hands the rest of its settings to that kind. A kind may be an Enterprise Feature (ADR 0033).
Vault KV version 2 is one, and lives in `ee/`. It checks the Edition on every read and, while the Edition is Community,
refuses with its own reason and deletes nothing. A store whose kind the build lacks still boots and shows as unavailable,
with its grants and references kept, so a deployment can move between the released binary and a source build. Each
store reports whether it is available, so Reef can show it before anyone saves a reference; the read still decides.

Core has one kind of its own, `env`, for deployments that inject secrets into the environment, such as from Kubernetes
Secrets. Its store names a prefix, and a reference names one whole variable that starts with it and is longer than it. It reads the variable on each
use, as other kinds read their store, through a reader bootstrap hands it: the one place Coral reads the environment
after bootstrap. Boot refuses a prefix that could cover Coral's own `CORAL_` variables or a variable any `*_env`
setting names, such as `credentials.key_env`, so a reference cannot read Coral's own secrets.

## Only a Workspace Owner, in Reef

A Workspace Owner sets references, on a shared Account (the `fixed` Credential Route) or an Agent's Account (`caller`).
A User's own Account does not take one, because a User could otherwise name a path holding another person's secret and
Coral would read it. Provider Setup's temporary use for discovery takes secret entry or OAuth, never a reference.
A Program cannot set a reference, so an agent-written Program cannot probe the store through the setup check.

Provider setup can set references too, only for the shared Account it would publish. Setup holds the references beside
the sealed typed values, reads each once to check it and seals nothing it read, and Publication copies them to the
Account. Discovery refuses one, because it has no account choice yet and would have to read a store to list tools, and
Publication refuses a grant with references for a `caller` Account without spending the Setup.

Who can inspect an Account in Reef can read its references. A reference is configuration, not a secret, and it stays
out of Programs, Agents, telemetry and Invocation errors.

## Fail closed

A reference must resolve when it is saved. At dispatch, an unreachable store, a
missing entry or a missing key fails the Invocation as an unusable Account, with an error that does not name the path;
Coral never falls back to another credential. Deleting an Account, or removing its Member or Agent, deletes its typed
values and references; Coral never changes the store.
