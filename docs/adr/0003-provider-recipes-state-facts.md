---
status: accepted
---

# State facts about an Upstream Service in a Provider Recipe, not opinions about its use

A Provider Recipe pre-fills the Provider Configuration that an owner reviews when adding a Provider. It speaks with
Coral's voice about someone else's service, and a person reads everything in it as a property of that service. A Recipe
therefore records only what is true of the Upstream Service: where its descriptor lives, which destinations its server
declarations use, which headers it requires, and how a person authorizes against it. It records nothing about which
parts of the service are worth using, and nothing about Coral's own configuration, such as a redirect URI, a client
secret or Coral's User-Agent.

The consequence that matters most is that a Recipe selects no Operations. An `include` list is the most tempting field
the format could have: GitHub's description has about nine hundred operations, and a good working set is a fraction of
that. But a Recipe that ships forty operations presents as the whole of GitHub through Coral, and the person who needs
another one has no way to learn it exists. Deselection belongs to the person adding the Provider, where it is visible
as a choice and reversible.

For the same reason a Recipe assigns no Operation names and supplies no default for an Operation parameter: binding an
input to a parameter is authored knowledge of the catalog arriving by a side door. Pagination and response shaping are
facts about the descriptor, which changes without warning, so Coral infers them rather than letting a Recipe rot
silently.

A Recipe may configure a server declaration by its document pointer, because some services serve a few Operations from
other hosts. That selects and names no Operation, and a pointer that no longer matches the document is a review error,
not a silent fallback.

A field that nothing reads goes stale unnoticed, so the format carries nothing Coral does not act on yet.
