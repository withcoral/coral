Source: https://docs.slack.dev/reference/scopes/users.read.email

# users:read.email scope

View email addresses of people in a workspace

## Facts

**Supported token types**

[`Bot`](/authentication/tokens#bot)

[`User`](/authentication/tokens#user)

[`Legacy Bot`](/authentication/tokens#legacy-bot)

**Compatible API methods**

[`users.lookupByEmail`](/reference/methods/users.lookupbyemail)

## Usage info {#usage-info}

Your Slack app may gain access to email addresses belonging to team members in a workspace by asking for `users:read.email`. This scope is now required to access the `email` field in user profiles retrieved with the Web API when using user or workspace tokens. Classic [`bot`](/authentication/tokens) tokens are granted access to the `email` field without needing further scopes.

This scope must be requested at the same time as [`users:read`](/reference/scopes/users.read).

Accessing Email Addresses

The [`users:read.email`](/reference/scopes/users.read.email) OAuth scope is now required to access the `email` field in user objects returned by the [`users.list`](/reference/methods/users.list) and [`users.info`](/reference/methods/users.info) web API methods. [`users:read`](/reference/scopes/users.read) is no longer a sufficient scope for this data field. [Learn more](/changelog/2017-04-narrowing-email-access).
