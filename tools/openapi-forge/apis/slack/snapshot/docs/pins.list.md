Source: https://docs.slack.dev/reference/methods/pins.list

# pins.list method

DocsCall generator

## Facts {#facts}

**Description**Lists items pinned to a channel.

**Method Access**

*   HTTP
*   Slack CLI
*   JavaScript
*   Python
*   Java

```
GET https://slack.com/api/pins.list
```

[![slack-cli](/img/logos/slack-cli.png)](/tools/slack-cli)

```
slack api pins.list
```

[![bolt-js](/img/logos/bolt-js-logo.svg)](/tools/bolt-js)

```
app.client.pins.list
```

[![bolt-py](/img/logos/bolt-py-logo.svg)](/tools/bolt-python)

```
app.client.pins_list
```

[![bolt-java](/img/logos/bolt-java-logo.svg)](/tools/java-slack-sdk/guides/getting-started-with-bolt)

```
app.client().pinsList
```

**Scopes**

Bot token:

[`pins:read`](/reference/scopes/pins.read)

User token:

[`pins:read`](/reference/scopes/pins.read)

**Content types**

`application/x-www-form-urlencoded`

`application/json`

**Rate Limits**[Tier 2: 20+ per minute](/apis/web-api/rate-limits)

## Arguments {#arguments}

### Required arguments

**`token`**`string`Required

Authentication token bearing required scopes. Tokens should be passed as an HTTP Authorization header or alternatively, as a POST parameter.

_Example:_ `xxxx-xxxxxxxxx-xxxx`

**`channel`**`string`Required

Channel to get pinned items for.

## Usage info {#usage-info}

This method lists the items pinned to a channel.

* * *

## Response {#response}

#### Typical success response

```
{  "items": [    {      "channel": "C123ABC456",      "created": 1508881078,      "created_by": "U123ABC456",      "message": {        "permalink": "https://hitchhikers.slack.com/archives/C2U86NC6H/p1508197641000151",        "pinned_to": [          "C2U86NC6H"        ],        "text": "What is the meaning of life?",        "ts": "1508197641.000151",        "type": "message",        "user": "U123ABC456"      },      "type": "message"    },    {      "channel": "C123ABC456",      "created": 1508880991,      "created_by": "U123ABC456",      "message": {        "permalink": "https://hitchhikers.slack.com/archives/C2U86NC6H/p1508284197000015",        "pinned_to": [          "C123ABC456"        ],        "text": "The meaning of life, the universe, and everything is 42.",        "ts": "1503289197.000015",        "type": "message",        "user": "U123ABC456"      },      "type": "message"    }  ],  "ok": true}
```

#### Typical error response

```
{  "ok": false,  "error": "invalid_auth"}
```

The response contains a list of pinned items in a channel. Different item types can be pinned. Every item in the list has a `type` property, and the other properties depend on the type of item. The possible types are:

*   **`message`**: the item will have a `message` property containing a [message object](/messaging) and a `channel` property containing the channel ID for the message.
*   **`file`**: this item will have a `file` property containing a [file object](/reference/objects/file-object).
*   **`file_comment`**: the item will have a `file` property containing the [file object](/reference/objects/file-object) and a `comment` property containing the file comment.

The `created` property on each item is a Unix timestamp representing when the item was pinned. The `created_by` property on each item is a string representing the encoded user id of the user who pinned the item.

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

`restricted_action`

The user does not have permission to view the channel.

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
