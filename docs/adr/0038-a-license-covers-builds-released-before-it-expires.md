---
status: proposed
---

# A License covers builds released before it expires

A License is a Commercial License or an Evaluation License. A self-hosted deployment runs as the Enterprise Edition
when its License's signature checks out and the build's Release Date is on or before the License's expiry. For a
Commercial License, Coral never compares the expiry with the clock. A customer who does not renew keeps the Enterprise
Features of the builds they already run, and loses support and new releases: a build released after the expiry
refuses to start with that License. Starting it as Community would be worse: a rolling deploy would see it pass its
health checks, retire the covered build, and leave every Enterprise Feature, such as each Secret Store read, failing.
A refusal keeps the running deployment alive until the operator runs a covered build, renews, or removes the License
to run as Community. There is no license server, no phone-home and no revocation of a License, so checking a
License sends nothing out of the customer's machine.

An Evaluation License is a trial, which nobody has paid for, so it gets no such fallback: trials run on the clock, paid
Licenses on the Release Date. It also needs the UTC date of the clock to be on or before its expiry. After that, Coral
refuses to start with it, and a running Coral stops: a periodic pass rechecks the License once a minute, and when the
trial has ended it logs why, shuts the server down, and exits with an error. Coral never drops to Community while it
runs, because Enterprise Features failing one by one would break a deployment quietly: Providers that read from a Secret
Store, or a sign-in that needs SAML. A deployment that stops is plain to see, and the operator either adds a Commercial
License or removes the License to run as Community. From the first day, startup warns of the expiry date, since a
trial is short. The signer, not the binary, sets how long a trial lasts.

The release workflow fixes the Release Date into every build it publishes, as the committer date of the merge-base of
the release tag and `main`. A release tagged on `main` gets its own commit's date. A security fix backported to an
older release line by cherry-picks keeps that line's Release Date, so it stays covered by the Licenses that covered
the line.

A License is one line of text: a PASETO `v4.public` token (Ed25519) signed with the implicit assertion
`coral-license`, so no other token signed with the same key passes as a License. Its payload holds a format version
`v`, its `kind` (`commercial` or `evaluation`), a license id `lid`, a `customer` name for people to read, its issue
time `iat` and its expiry `exp_date`, a calendar date. The expiry is not PASETO's registered `exp` claim, because generic validators compare `exp` with the
clock. A License covers a build when the UTC date of the build's Release Date is on or before `exp_date`. The footer
names the signing key by its PASERK id (`k4.pid`), and the crate keeps each public key it trusts as a PASERK
`k4.public` string. A License binds to no Organization, deployment or Coral Instance, and carries no tier, feature list
or seat count: the Edition is all it sets. A binary refuses a format version or a `kind` it does not know.

A build with a Release Date trusts only the production public keys. A build without one, any source or development
build, trusts only a test public key, and counts as released on its own commit's date, or on the current date when git
is unavailable. The test key's private half stays in the Coral team's password vault, outside this repository and CI. The
repository keeps three Licenses signed with it: a Commercial License that covers every development build, which Reel and
developers load to run as Enterprise, one that has lapsed, and an Evaluation License that never expires, to show the
trial path. Tests that need other Licenses sign them with keys they generate, which they pass to the check directly. So
no published binary ever accepts a test License, and CI holds no long-lived signing key. ADR 0033 protects `ee/` code
with its licence, not with access to it: anyone with the source can already change the check, so the test key grants
nothing the licence does not already govern.

## Considered options

- **Phone home on a timer** (Sourcegraph, Metabase, n8n). Rejected: it breaks offline and air-gapped use, and sends
  data out of the customer's machine.
- **Short-lived License files renewed from a server** (Keygen, Grafana, GitLab cloud licensing). Rejected for the same
  reasons, and a renewal server is one more service to run.
- **Expiry against the clock, or Vault's refusal to start.** Rejected for Commercial Licenses: a server's clock can be
  set back, and switching off features a customer paid for is a harsher deal than the one we want to sell. Evaluation
  Licenses take the clock, since nobody paid for them.
- **One kind of License for trials and sales.** Rejected: a short trial License would enable Enterprise forever on the
  builds released during the trial.
- **Start a build released after a Commercial License's expiry as Community.** Rejected: a rolling deploy replaces
  the covered build with one whose Enterprise Features fail.
- **An Evaluation License checked only at startup.** Rejected: a deployment that never restarts would keep its trial
  forever.
- **Drop to Community when an Evaluation License expires, at startup or while Coral runs.** Rejected: Enterprise
  Features would stop working without a clear sign, such as Providers that read from Vault or a SAML sign-in.
- **The Release Date as the build machine's clock, or the release commit's own date.** Rejected: the first changes
  when an old tag is rebuilt, and the second drops Enterprise Features from a backported security fix.
- **A JWT signed with EdDSA.** `jsonwebtoken` is already a dependency, but a JWT names its own algorithm, which the
  verifier must pin, and the validator checks `exp` against the clock by default. Turning default validation back on
  would silently end the fallback.
- **A format of our own** (signed JSON, as Mattermost and Elastic do). Rejected: every reviewer would have to audit an
  invented format, where PASETO fixes one algorithm per version and specifies how the signed bytes are built.

## Consequences

- A leaked License unlocks Enterprise on every build released up to its expiry. The contract covers that misuse.
- A leaked signing key is revoked only in builds whose source lists it as revoked. We move the key from the trusted keys
  to a list of revoked keys, so every build made after that change refuses its Licenses with a message that names the
  revoked key, and issue every customer a new License under a new key. No date decides this: a fix backported to an
  older release line revokes the key there too, though it keeps that line's Release Date. Builds already released go on
  trusting it. Builds trust several keys at once to make this possible. A cut-off date cannot save the Licenses signed
  before the leak: whoever holds the key can sign any `iat`.
- Code signing does not guard against the licensee. Release builds are signed, except the Linux AppImage, so that
  customers can check where a binary came from. But a customer can run a patched copy of their own binary, and anyone
  with the source can change the check. The License terms and the contract cover that.
- Setting the server's clock back extends an Evaluation License. We accept that: a trial is short and unpaid, and its
  terms cover the misuse. An Evaluation License still covers no build released after its expiry.
- The Edition does not change while Coral runs. An Evaluation License that expires stops Coral, within a minute of
  midnight UTC at the end of its expiry date, in every Coral Instance at once. An orchestrator that restarts it then
  sees it refuse to start.
- The EULA must grant continued use of Enterprise Features on covered builds after the term ends, and state that
  backported fixes keep their release line's Release Date.
