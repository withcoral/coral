---
status: accepted
---

# Offer Provider Recipe authentication as named options with typed methods

A Provider Recipe describes authentication as named options, each with a label and exactly one method from a fixed
list: `none`, `bearer`, `header_credentials`, `basic_auth`, `query_credentials`, and the OAuth authorization-code,
device-code and discovery flows. The owner confirms one option when adding the Provider, and it becomes the Provider
Credential Contract. Options are not a runtime fallback, a per-account choice, or a menu offered to Programs.

Rejected: an `obtain`/`send` split that described how a person acquires a credential and how it reaches the service.
It allowed combinations that cannot work, such as OAuth acquisition sent as HTTP Basic, and only one way to send per
Recipe. It also did not match the server, which describes each authentication choice by its method. Rejected too: a
value or template language over credentials, as the previous DSL had. The 207 previous manifests needed a handful of
named behaviors, not templates, and a template language hands credential positions to the author.

## Credentials travel only where the host puts them

The host owns every position a credential occupies. A Recipe never contains a credential token, and a credential never
becomes part of stored configuration, a URL preview, a diagnostic or telemetry. No method puts a credential in the URL
path. Telegram's Bot API is the one service found that wants it, Coral cannot describe that API anyway, and credentials
in URLs are a larger surface than one API's convention warrants.

A value that is half of what authorizes a request, such as Jira's account email, is a Credential Field of the Account,
never a Provider-wide input, so one account's identifier is never shared with every account. A literal is accepted as
typed, because Coral cannot tell a protocol constant from a secret; where a constant has no use, the choice is not
offered at all (an HTTP Basic password is a field or empty).

The Provider Credential Contract holds only each field's name and where it is sent. A field's label, description and
masking sit beside the contract, so changing them never makes people sign in again.

## OAuth

A Recipe carries no client secret and no redirect URI: those are Coral's configuration, not facts about the service
(ADR 0003). A public client id is valid only for the authority it was registered with, so a self-hosted service such as
self-managed GitLab gets its own Recipe with no client id. In a device-code flow the server polls the token endpoint,
so tokens never reach Reef and nothing depends on the page staying open.

## SigV4 is deferred

AWS SigV4 signs each request, and its temporary credentials expire. It needs a host implementation and lifecycle rules
rather than a method name, so no method is reserved for it until that work is designed.
