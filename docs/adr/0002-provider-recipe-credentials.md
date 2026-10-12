---
status: accepted
---

# Offer Provider Recipe authentication as named options with typed methods

A Provider Recipe describes authentication as `authentication.options`: a map of recipe-local choices, each with a
label and exactly one `method` from a fixed list. `default` may name the option to pre-select. The owner reviews the
options while configuring the Provider and confirms exactly one; that option's method and settings define the Provider
Credential Contract. Options are not a runtime fallback, a per-account choice, or a menu offered to Programs.

An earlier draft of this decision split authentication into `obtain`, describing how a person acquires a credential,
and `send`, describing how it reaches the Upstream Service. That split had three faults, and a later design proposed the shape
adopted here. It permitted combinations that cannot work, such as acquiring a credential by OAuth while sending it as an
HTTP Basic pair. It offered one `send` per Recipe, so a service accepting either a bearer token or a username and
password could describe only one of them. And it did not match how Coral itself models authentication: the server
describes each authentication choice by its method, and Reef renders the Add provider flow from those choices, so a
method-shaped Recipe option lines up with one of them, while an acquisition-and-injection pair would have to be
reassembled into a method before Reef could use it. ADR 0004 records how Reef applies a Recipe.

The previous source DSL shows what the method list replaces. It gave each surface its own auth grammar: an OpenAPI
surface accepted a twenty-variant tagged value type, an MCP surface accepted a single bearer token naming a declared
secret input, and MCP over stdio had no auth block at all. Those grammars were the Rust types the document
deserialized into, which is why a YAML file contained `type: HeaderAuth`. A survey of the 207 shipped v3 manifests
found that credential positions used only `template` (111), `input` (16), `literal` (1) and `bearer` (1). A Recipe
needs neither a value language nor templates over credentials; it needs a small set of named behaviors.

| Method | Behavior |
| --- | --- |
| `none` | No Provider Account. |
| `bearer` | One secret, sent as a bearer token. |
| `header_credentials` | One or more credential fields bound to headers, each with an optional literal `prefix`. |
| `basic_auth` | Username and password bindings, encoded by the host as HTTP Basic. |
| `query_credentials` | One or more credential fields bound to query parameters. |
| `oauth_authorization_code` | Authored authorization and token endpoints; bearer injection. |
| `oauth_device_code` | Authored device authorization and token endpoints; bearer injection. |
| `oauth_discovery` | MCP authorization metadata supplies the endpoints; the Recipe selects the flow. |

Until September 2026 this table also had `header_api_key` and `query_api_key`, for a single secret in one header or
query parameter. Each was only the one-field case of `header_credentials` or `query_credentials`.
Two ways to write the same thing made Recipes harder to write and review, and gave the server, the API and Reef an extra
method each. A single API key is now a `header_credentials` or `query_credentials` option with one field. `bearer` stays
separate even though it is a header with a `Bearer ` prefix, because MCP sign-in and every OAuth method send their token
that way.

`path_token`, for one secret between a literal path prefix and the Operation path, left the table in September 2026
before the server ever offered it. Telegram's Bot API is the only service we found that needs it, and Coral cannot
describe that API at all: Telegram publishes no OpenAPI document, and no current third-party one exists. A path
credential therefore unblocked nothing on its own, so the work stopped there. Meanwhile the Recipe format described
a method a recipe author could write and Reef could only skip, which is worse than not offering it. Coral adds a path
credential back when a service it can describe needs one.

Before this decision, the Coral server supported `none`, `bearer`, `header_api_key` and `oauth_authorization_code`. Its
`header_api_key` becomes a one-field `header_credentials`, the other methods follow, and ADR 0005 records how the API carries
them. SigV4 is deliberately absent from the list. Until the server offers a method, Reef cannot pre-fill a Recipe option
that uses it.

## Credential fields

A method that needs more than one value declares `fields`, each with a label, an optional description, and `sensitive`,
which defaults to true and controls only whether entry is masked. Fields describe one Provider Account's credential
record and never carry values or defaults. Methods bind them by reference, as `{field: api_token}`, or bind a protocol
constant, as `{literal: api}`. Every declared field must be referenced. This covers the twelve v3 manifests that
combine two secrets, the twenty-one using HTTP Basic, and the three sending more than one credential header.

Where a service pairs a secret with an account identifier, the identifier is a field and not an input. Jira
authenticates with an Atlassian account email and an API token, and the email is half of what authorizes the request;
making it a Provider-wide input would share one account's email with every account connecting through that Provider.
It is declared with `sensitive: false` so that it is visible while typed, and it remains private to that account.
Datadog's API key and application key are two labelled fields in one record, because a person pasting both needs to
know which box is which. Mailgun's Basic username is the constant `api`, so its Recipe binds a literal and collects one
field.

A header binding may carry a literal `prefix`. Twelve v3 manifests format their Authorization header with something
other than `Bearer`, such as PagerDuty's `Token token=`, and a literal prefix covers them without asking a person to
assemble a header value by hand, and without reintroducing a template language.

## Where a credential travels

The host owns every position a credential occupies. A Recipe never contains a `{{credential}}` token, and a credential
never becomes part of stored configuration, a URL preview, a diagnostic or telemetry. Query credentials occupy parameter
positions the host fills at dispatch and that ordinary Operation arguments cannot supply or override.

No credential occupies the URL path. Telegram's Bot API is the service that asks for one, embedding the token in the
path itself as `https://api.telegram.org/bot<token>/getUpdates`. An earlier draft allowed a credential inside
`base_url` to express this, which would have put a secret into the resolved destination Coral stores and shows for
review; `path_token` replaced it with a prefix the host rendered the token into at dispatch, and the method table
records why that method is gone too. Both rejections hold for the same reason: a general rule allowing credentials in
URLs would be a much larger surface than one API's path convention warrants.

A literal is a protocol constant, and Coral cannot prove that it is one. An owner types fixed values by hand, so a
literal can hold anything, and a check that guessed at secrets by their shape would refuse honest values while still
missing dishonest ones. Where a constant earns its place, Coral accepts what is typed and shows it back: Mailgun's Basic
username is `api`, a header may send a fixed API version, and PagerDuty's prefix is `Token token=`. Where a constant
earns nothing, Coral removes the choice rather than guess. An HTTP Basic password is a Credential Field or empty,
because Stripe sends a key with no password, no service we support sends a constant one, and a box for a fixed password
invites a secret into stored configuration, the difference list and the preview.

Every method a person types values into is for OpenAPI Providers only: `header_credentials`, `basic_auth` and
`query_credentials`. An MCP source authenticates with nothing, a bearer token, or OAuth, because none of the others has
an MCP injection recipe. The server offers no other method for such a source, and rejects a discovered contract or a
request that names one anyway, because Reef and the server can run different versions (ADR 0004).

## OAuth

An OAuth option may carry a `client` with a public `id` and an `authentication` of `none`, `client_secret_basic` or
`client_secret_post`. The public id is an editable default for an application Coral registered; the owner confirms it
into Provider Configuration. A client secret never appears in a Recipe: choosing a secret-bearing client authentication
makes Setup collect it through the host credential subsystem. The redirect URI is not authorable at all, because it
names where Coral listens for the callback and differs between a hosted deployment and a local one reading the same
Recipe.

A public client id is valid only for the authority it was registered with. A self-hosted service therefore gets its own
Recipe with no client id, and its OAuth endpoints derive from the instance input: GitLab.com and self-hosted GitLab are
separate Recipes so that a GitLab.com registration is never offered to another instance.

Authorization-code flows use S256 PKCE unless an option with a confidential client sets `pkce: disabled`. `scopes`
remains a logical list, and `scope_format` states the request and response separators for services such as Strava that
do not use spaces ([ADR 0010](0010-reported-grant-recorded-verbatim.md) supersedes the response separator: the token
endpoint's `scope` is recorded verbatim, so a Recipe states only the request separator). Non-secret
`authorization_parameters`, such as Google's `access_type: offline`, are literal and may not override host-controlled
parameters. An MCP Recipe cannot author endpoints, issuer or resource; `oauth_discovery` takes them from the server's
authorization metadata. That is why the GitHub and GitHub MCP Recipes offer the same sign-in with different options:
one authors its device-flow endpoints and the other discovers them.

In a device-code flow, the server starts device authorization and polls the token endpoint as part of the Provider
Account Authorization, and Reef only shows the code and waits. That keeps tokens out of Reef and does not depend on the
page staying open. A device-code option takes the same `client` settings as an authorization-code one. The server offers
`oauth_discovery` with `flow: device_code` only when the authorization server's metadata has a
`device_authorization_endpoint`.

## SigV4 is deferred

Four v3 manifests use AWS SigV4. Unlike every method above, it signs the method, URL, headers, payload and time of each
request, and temporary credentials add a session token with an expiry. That needs a host implementation and lifecycle
rules rather than a Recipe method name, so none is reserved until that work is designed.
