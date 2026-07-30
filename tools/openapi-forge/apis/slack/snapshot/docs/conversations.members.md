Source: https://docs.slack.dev/reference/methods/conversations.members

# conversations.members method

DocsCall generator

## Facts {#facts}

**Description**Retrieve members of a conversation.

**Method Access**

*   HTTP
*   Slack CLI
*   JavaScript
*   Python
*   Java

```
GET https://slack.com/api/conversations.members
```

[![slack-cli](/img/logos/slack-cli.png)](/tools/slack-cli)

```
slack api conversations.members
```

[![bolt-js](/img/logos/bolt-js-logo.svg)](/tools/bolt-js)

```
app.client.conversations.members
```

[![bolt-py](/img/logos/bolt-py-logo.svg)](/tools/bolt-python)

```
app.client.conversations_members
```

[![bolt-java](/img/logos/bolt-java-logo.svg)](/tools/java-slack-sdk/guides/getting-started-with-bolt)

```
app.client().conversationsMembers
```

**Scopes**

Bot token:

[`channels:read`](/reference/scopes/channels.read)[`groups:read`](/reference/scopes/groups.read)[`im:read`](/reference/scopes/im.read)[`mpim:read`](/reference/scopes/mpim.read)

User token:

[`channels:read`](/reference/scopes/channels.read)[`groups:read`](/reference/scopes/groups.read)[`im:read`](/reference/scopes/im.read)[`mpim:read`](/reference/scopes/mpim.read)

**Content types**

`application/x-www-form-urlencoded`

`application/json`

**Rate Limits**[Tier 4: 100+ per minute](/apis/web-api/rate-limits)

## Arguments {#arguments}

### Required arguments

**`token`**`string`Required

Authentication token bearing required scopes. Tokens should be passed as an HTTP Authorization header or alternatively, as a POST parameter.

_Example:_ `xxxx-xxxxxxxxx-xxxx`

**`channel`**`string`Required

ID of the conversation to retrieve members for

### Optional arguments

**`cursor`**`string`Optional

Paginate through collections of data by setting the `cursor` parameter to a `next_cursor` attribute returned by a previous request's `response_metadata`. Default value fetches the first "page" of the collection. See [pagination](/apis/web-api/pagination) for more detail.

_Example:_ `dXNlcjpVMDYxTkZUVDI=`

**`limit`**`number`Optional

The maximum number of items to return. Fewer than the requested number of items may be returned, even if the end of the users list hasn't been reached.

_Default:_ `100`

_Example:_ `20`

## Usage info {#usage-info}

This [Conversations API](/apis/web-api/using-the-conversations-api) method returns a paginated list of members party to a [conversation](/reference/objects/conversation-object).

* * *

## Response {#response}

#### Typical paginated success response

```
{  "ok": true,  "members": [    "U023BECGF",    "U061F7AUR",    "W012A3CDE"  ],  "response_metadata": {    "next_cursor": "e3VzZXJfaWQ6IFcxMjM0NTY3fQ=="  }}
```

#### Typical error response when an invalid cursor is provided

```
{  "ok": false,  "error": "invalid_cursor"}
```

Returns a list of user IDs belonging to the members in a conversation.

### Pagination {#pagination}

This method uses cursor-based pagination to make it easier to incrementally collect information. To begin pagination, specify a `limit` value under `1000`. We recommend no more than `200` results at a time.

Responses will include a top-level `response_metadata` attribute containing a `next_cursor` value. By using this value as a `cursor` parameter in a subsequent request, along with `limit`, you may navigate through the collection page by virtual page.

See [pagination](/apis/web-api/pagination) for more information.

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

`channel_not_found`

Value passed for `channel` was invalid.

`deprecated_endpoint`

The endpoint has been deprecated.

`ekm_access_denied`

Administrators have suspended the ability to post a message.

`enterprise_is_restricted`

The method cannot be called from an Enterprise.

`fatal_error`

The server could not complete your operation(s) without encountering a catastrophic error. It's possible some aspect of the operation succeeded before the error was raised.

`fetch_members_failed`

Failed to fetch members for the conversation.

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

`invalid_cursor`

Value passed for `cursor` was invalid.

`invalid_form_data`

The method was called via a `POST` request with `Content-Type` `application/x-www-form-urlencoded` or `multipart/form-data`, but the form data was either missing or syntactically invalid.

`invalid_limit`

Value passed for `limit` was invalid.

`invalid_post_type`

The method was called via a `POST` request, but the specified `Content-Type` was invalid. Valid types are: `application/json` `application/x-www-form-urlencoded` `multipart/form-data` `text/plain`.

`method_deprecated`

The method has been deprecated.

`method_not_supported_for_channel_type`

This type of conversation cannot be used with this method.

`missing_post_type`

The method was called via a `POST` request and included a data payload, but the request did not include a `Content-Type` header.

`missing_scope`

The calling token is not granted the necessary scopes to complete this operation.

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
