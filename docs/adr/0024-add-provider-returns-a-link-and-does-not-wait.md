---
status: accepted
---

# addProvider returns a link and does not wait for the person

When `coral.workspace.addProvider` needs a person to sign in or enter a credential, the Invocation returns at once with
`{ status: "needs_sign_in", url, namespace }`. The url opens Reef's Add provider steps for the Setup the call prepared,
and the person publishes the Provider there. The Run goes on, or ends, without them. Coral publishes the Provider itself
only when nobody needs to sign in (the `none` route on a ready Setup), and then returns
`{ status: "published", namespace, … }` with what the Program needs to call it.

Issue numbers prefixed `lagoon` are in withcoral/lagoon, Coral's previous repository; bare numbers are in this one.

## Why

The person often outlasts the Run. A Run lasts at most 15 minutes (`RUN_TIMEOUT_MAX_MS`). A Setup stays open for an
hour, and finishing one can mean registering an OAuth application or finding a token.

Waiting also needs its own machinery, which a link does not. Lagoon #613, #614, #615 and #697 planned it: a column that
links the Invocation to its Setup, a `provider_setup` required action with its own wake-ups, an MCP Action Prompt, and
rules that end the Run when the Setup fails, expires or is declined. With a link, the Setup is the only state. Once the
call returns the link, the Setup expires on its own deadline, and the Run's end does not decide it.

## Consequences

- The Program does not learn in the same Run whether the person published the Provider. It calls
  `coral.workspace.listProviders`, which reports the open Setups that the caller's `addProvider` calls prepared until
  each is published, expires or is canceled.
- The person can change the Provider Namespace before they publish. Nothing compares the published Provider with the
  request.
- A Run that ended before the call returns can no longer hand anyone the link, so Coral ends the Setup it prepared. A
  worker lost after the Dispatch Fence settles from the Publication Coral attempted under the Invocation: its
  Provider is the result, and otherwise the Run ended first.
- Rejected: parking the Invocation on a Provider Setup Request until the person publishes (lagoon #613 and #697), and
  offering that request as an MCP Action Prompt (lagoon #615). Those issues still describe the wait.
