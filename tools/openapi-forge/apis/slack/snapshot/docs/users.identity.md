Source: https://docs.slack.dev/reference/methods/users.identity

# users.identity method

DocsCall generator

## Facts {#facts}

**Description**Get a user's identity.

**Method Access**

*   HTTP
*   Slack CLI
*   JavaScript
*   Python
*   Java

```
GET https://slack.com/api/users.identity
```

[![slack-cli](/img/logos/slack-cli.png)](/tools/slack-cli)

```
slack api users.identity
```

[![bolt-js](/img/logos/bolt-js-logo.svg)](/tools/bolt-js)

```
app.client.users.identity
```

[![bolt-py](/img/logos/bolt-py-logo.svg)](/tools/bolt-python)

```
app.client.users_identity
```

[![bolt-java](/img/logos/bolt-java-logo.svg)](/tools/java-slack-sdk/guides/getting-started-with-bolt)

```
app.client().usersIdentity
```

**Scopes**

User token:

[`identity:read`](/reference/scopes/identity.read)

**Content types**

`application/x-www-form-urlencoded`

`application/json`

**Rate Limits**[Tier 3: 50+ per minute](/apis/web-api/rate-limits)

## Arguments {#arguments}

### Required arguments

**`token`**`string`Required

Authentication token bearing required scopes. Tokens should be passed as an HTTP Authorization header or alternatively, as a POST parameter.

_Example:_ `xxxx-xxxxxxxxx-xxxx`

## Usage info {#usage-info}

After yourSlack app is awarded an identity token through [Sign in with Slack](/authentication/sign-in-with-slack/), use this method to retrieve a user's identity.

The returned fields depend on any additional [authorization scopes](#authorization_scopes) you've requested.

With traditional Slack apps, this method must be called by individual user tokens with the `identity.basic` scope, as provided in the [Sign in with Slack](/authentication/sign-in-with-slack/) process.

* * *

## Response {#response}

#### You will receive at a minimum the following information:

```
{  "ok": true,  "user": {    "name": "Sonny Whether",    "id": "U0G9QF9C6"  },  "team": {    "id": "T0G9PQBBK"  }}
```

#### The identity.email scope provides the member's email address, if available:

```
{  "ok": true,  "user": {    "name": "Sonny Whether",    "id": "U0G9QF9C6",    "email": "bobby@example.com"  },  "team": {    "id": "T0G9PQBBK"  }}
```

#### Using with the identity.avatar scope yields the member's avatar images. Available sizes may vary in the future.

```
{  "ok": true,  "user": {    "name": "Sonny Whether",    "id": "U0G9QF9C6",    "image_24": "https://cdn.example.com/sonny_24.jpg",    "image_32": "https://cdn.example.com/sonny_32.jpg",    "image_48": "https://cdn.example.com/sonny_48.jpg",    "image_72": "https://cdn.example.com/sonny_72.jpg",    "image_192": "https://cdn.example.com/sonny_192.jpg"  },  "team": {    "id": "T0G9PQBBK"  }}
```

#### Use with the identity.team scope to retrieve the user's workspace name:

```
{  "ok": true,  "user": {    "name": "Sonny Whether",    "id": "U0G9QF9C6"  },  "team": {    "name": "Captain Fabian's Naval Supply",    "id": "T0G9PQBBK"  }}
```

#### Typical error response

```
{  "ok": false,  "error": "account_inactive"}
```

Note: When users sign into Slack via Apple, their emails appear as anonymized relay addresses. [Sign in with Slack](/authentication/sign-in-with-slack/#exceptions) won't work with these users.

User IDs are now globally unique. Unless you are in an Enterprise organization, the same user on two unrelated workspaces will have different user IDs.

See the [Sign in with Slack](/authentication/sign-in-with-slack/) docs for even more information on these responses.

In addition, you can request access to additional profile fields by adding the following [authorization scopes](/authentication/installing-with-oauth) to your OAuth request, detailed in the above examples.

## Errors {#errors}

This table lists the expected errors that this method could return. However, other errors can be returned in the case where the service is down or other unexpected factors affect processing. Callers should always check the value of the `ok` parameter in the response.

Error

Description

`access_denied`

Access to a resource specified in the request is denied.

`accesslimited`

Access to this method is limited on the current network

`account_inactive`

Authentication token is for a deleted user or workspace when using a `bot` token.

`deprecated_endpoint`

The endpoint has been deprecated.

`ekm_access_denied`

Administrators have suspended the ability to post a message.

`email_not_verified`

user email has not been verified

`enterprise_is_restricted`

The method cannot be called from an Enterprise.

`fatal_error`

The server could not complete your operation(s) without encountering a catastrophic error. It's possible some aspect of the operation succeeded before the error was raised.

`internal_error`

Internal error

`internal_error`

The server could not complete your operation(s) without encountering an error, likely due to a transient issue on our end. It's possible some aspect of the operation succeeded before the error was raised.

`invalid_arg_name`

The method was passed an argument whose name falls outside the bounds of accepted or expected values. This includes very long names and names with non-alphanumeric characters other than `_`. If you get this error, it is typically an indication that you have made a _very_ malformed API call.

`invalid_arguments`

The method was called with invalid arguments.

`invalid_array_arg`

The method was passed an array as an argument. Please only input valid strings.

`invalid_auth`

Some aspect of authentication cannot be validated. Either the provided token is invalid or the request originates from an IP address disallowed from making the request.

`invalid_charset`

The method was called via a `POST` request, but the `charset` specified in the `Content-Type` header was invalid. Valid charset names are: `utf-8` `iso-8859-1`.

`invalid_form_data`

The method was called via a `POST` request with `Content-Type` `application/x-www-form-urlencoded` or `multipart/form-data`, but the form data was either missing or syntactically invalid.

`invalid_post_type`

The method was called via a `POST` request, but the specified `Content-Type` was invalid. Valid types are: `application/json` `application/x-www-form-urlencoded` `multipart/form-data` `text/plain`.

`invalid_user_id`

Invalid user id provided

`method_deprecated`

The method has been deprecated.

`missing_post_type`

The method was called via a `POST` request and included a data payload, but the request did not include a `Content-Type` header.

`missing_scope`

The token used is not granted the specific scope permissions required to complete this request.

`no_permission`

The workspace token used in this request does not have the permissions necessary to complete the request. Make sure your app is a member of the conversation it's attempting to post a message to.

`not_allowed_token_type`

The token type used in this request is not allowed.

`not_authed`

No authentication token provided.

`org_login_required`

The workspace is undergoing an enterprise migration and will not be available until migration is complete.

`ratelimited`

The request has been ratelimited. Refer to the `Retry-After` header for when to retry the request.

`request_timeout`

The method was called via a `POST` request, but the `POST` data was either missing or truncated.

`service_unavailable`

The service is temporarily unavailable

`team_access_not_granted`

The token used is not granted the specific workspace access required to complete this request.

`team_added_to_org`

The workspace associated with your request is currently undergoing migration to an Enterprise Organization. Web API and other platform operations will be intermittently unavailable until the transition is complete.

`token_expired`

Authentication token has expired

`token_revoked`

Authentication token is for a deleted user or workspace or the app has been removed when using a `user` token.

`two_factor_setup_required`

Two factor setup is required.
