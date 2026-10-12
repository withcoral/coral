---
status: accepted
---

# State facts about an Upstream Service in a Provider Recipe, not opinions about its use

Coral distributes Provider Recipes and presents them when someone adds a Provider, where a Recipe pre-fills the
Provider Configuration that the owner reviews and confirms. A Recipe therefore speaks with Coral's voice about somebody
else's service, and everything it contains reads to that person as a property of the Upstream Service rather than as a
choice Coral made. A Recipe records only what is true of the Upstream Service: where its descriptor lives, which
destinations its server declarations use, which headers it requires, and how a person authorizes against it. It
records nothing about which parts of that service are worth using.

The consequence that matters most is that a Recipe selects no Operations. GitHub's OpenAPI description carries roughly
nine hundred operations and a well-judged working set is a small fraction of that, so an `include` or `exclude` list is
the most tempting field the format could have. It is also the one that would do the most damage. A Recipe that ships
forty operations does not present as a curated subset; it presents as the whole of what GitHub offers through Coral,
and the person who needed the eight-hundred-and-sixty-first has no way to know it exists or that Coral decided against
it. Coral expects to offer deselection when a Provider is added, where the choice belongs to the person making it, is
visible as a choice, and is reversible per installation rather than baked into a file shipped to everyone.

The same reasoning removes renaming. A Recipe assigns no Operation names, because the Materialized Operation Catalog
already derives them and an authored name is Coral's opinion about what an Operation should be called. It also removes
pagination and response shaping. Those are facts, but they are facts about the descriptor rather than about the
service, and the descriptor is the thing that changes without warning. Inferring them keeps a Recipe correct when an
upstream edits its specification; authoring them means a Recipe rots silently and nobody learns of it until a query
returns the wrong rows.

An input may not supply a default value for an Operation parameter. Sentry motivates the idea — every path contains an
organization slug, and typing it on each call is tedious — and the previous DSL declared a `SENTRY_ORG` input that no
part of that manifest ever referenced. Binding an input to a parameter requires naming which parameter, on which
Operations, matched how, which is authored knowledge of the catalog arriving through a side door immediately after this
decision turned it away at the front. If the need is real it is better met where Coral already has the person present,
at Provider Setup or on the Provider Installation, than by a static file guessing which Operations take an organization.

Server destinations are facts, and a Recipe states them where the descriptor declares them. Datadog serves most of its
v1 API from `api.<site>` but its IP-ranges and log-intake Operations from other hosts, and its OpenAPI document declares
those servers on the Operations themselves. A Recipe configures each declaration by its document pointer rather than
replacing every destination with one base URL. That is not authored knowledge of the catalog: it selects no Operations
and names none, and a pointer that no longer matches the document is a review error rather than a silent fallback.

## What this removes from the previous format

Test queries are gone. They were SQL written against a projection, which is an opinion about how a service should be
queried, and they encoded a query surface Coral no longer presents. The honest replacement is a scheduled check that
each Recipe's descriptor URL still resolves and still parses, which tests a fact instead.

Request headers survive, narrowed to headers the Upstream Service requires. `X-GitHub-Api-Version: 2022-11-28` is a
fact about GitHub and belongs in the Recipe. The `User-Agent` the previous manifests carried is Coral identifying
itself, which is true of Coral rather than of GitHub, and the runtime sets it.

That distinction generalizes beyond opinions. A Recipe also excludes Coral's own configuration, which is neither a fact
about the Upstream Service nor an opinion about its use, and which the previous manifests carried without remarking on
it. The OAuth redirect URI is the clearest case: it names where Coral listens for a callback, and it differs between a
hosted deployment and a local one reading the same Recipe. An OAuth client secret is the same. A public client id is
narrower: a Recipe may carry one as an editable default for an application Coral registered, which the owner confirms
and which is never offered to another authority. ADR 0002 records the detail.

Rate-limit headers are gone too. They are facts about the service, but the format no longer carries anything Coral
does not yet act on, and a field nobody reads is a field nobody notices going stale.

Method labels survive; long hint prose does not. Most hints in the previous manifests explained how to configure Coral
— which input to set to use your own OAuth application, and what that input is called. That is documentation about
Coral wearing the costume of a fact about the provider. A short label naming the method, a sentence of genuine upstream
requirement, and a link to the provider's own documentation are enough.
