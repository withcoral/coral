Source: https://docs.slack.dev/reference/methods/reactions.list

# reactions.list method

DocsCall generator

## Facts {#facts}

**Description**Lists reactions made by a user.

**Method Access**

*   HTTP
*   Slack CLI
*   JavaScript
*   Python
*   Java

```
GET https://slack.com/api/reactions.list
```

[![slack-cli](/img/logos/slack-cli.png)](/tools/slack-cli)

```
slack api reactions.list
```

[![bolt-js](/img/logos/bolt-js-logo.svg)](/tools/bolt-js)

```
app.client.reactions.list
```

[![bolt-py](/img/logos/bolt-py-logo.svg)](/tools/bolt-python)

```
app.client.reactions_list
```

[![bolt-java](/img/logos/bolt-java-logo.svg)](/tools/java-slack-sdk/guides/getting-started-with-bolt)

```
app.client().reactionsList
```

**Scopes**

Bot token:

[`reactions:read`](/reference/scopes/reactions.read)

User token:

[`reactions:read`](/reference/scopes/reactions.read)

**Content types**

`application/x-www-form-urlencoded`

`application/json`

**Rate Limits**[Tier 2: 20+ per minute](/apis/web-api/rate-limits)

## Arguments {#arguments}

### Required arguments

**`token`**`string`Required

Authentication token bearing required scopes. Tokens should be passed as an HTTP Authorization header or alternatively, as a POST parameter.

_Example:_ `xxxx-xxxxxxxxx-xxxx`

### Optional arguments

**`user`**`string`Optional

Show reactions made by this user. Defaults to the authed user.

**`full`**`boolean`Optional

If true always return the complete reaction list.

**`count`**`integer`Optional

_Default:_ `100`

**`page`**`integer`Optional

_Default:_ `1`

**`cursor`**`string`Optional

Parameter for pagination. Set `cursor` equal to the `next_cursor` attribute returned by the previous request's `response_metadata`. This parameter is optional, but pagination is mandatory: the default value simply fetches the first "page" of the collection. See [pagination](/apis/web-api/pagination) for more details.

_Example:_ `dXNlcjpVMDYxTkZUVDI=`

**`limit`**`integer`Optional

The maximum number of items to return. Fewer than the requested number of items may be returned, even if the end of the list hasn't been reached.

0

_Example:_ `20`

**`team_id`**`string`Optional

encoded team id to list reactions in, required if org token is used

## Usage info {#usage-info}

This method returns a list of all items (file, file comment, channel message, group message, or direct message) with reactions made by the user.

The `team_id` is only relevant when using an org-level token. This field will be ignored if the API call is sent using a workspace-level token.

* * *

## Response {#response}

#### Typical success response

```
{  "items": [    {      "type": "message",      "channel": "C123ABC456",      "message": {        "bot_id": "B123ABC456",        "reactions": [          {            "count": 1,            "name": "robot_face",            "users": [              "U123ABC456"            ]          }        ],        "subtype": "bot_message",        "text": "Hello from Python! :tada:",        "ts": "1507849573.000090",        "username": "Shipit Notifications"      }    },    {      "comment": {        "type": "file_comment",        "comment": "This is a file comment",        "created": 1508286096,        "id": "Fc123ABC456",        "reactions": [          {            "count": 1,            "name": "white_check_mark",            "users": [              "U123ABC456"            ]          }        ],        "timestamp": 1508286096,        "user": "U123ABC456"      },      "file": {        "channels": [          "C123ABC456"        ],        "comments_count": 1,        "created": 1507850315,        "reactions": [          {            "count": 1,            "name": "stuck_out_tongue_winking_eye",            "users": [              "U123ABC456"            ]          }        ],        "title": "computer.gif",        "user": "U123ABC456",        "username": ""      }    },    {      "file": {        "channels": [          "C123ABC456"        ],        "comments_count": 1,        "created": 1507850315,        "id": "F123ABC456",        "name": "computer.gif",        "reactions": [          {            "count": 1,            "name": "stuck_out_tongue_winking_eye",            "users": [              "U123ABC456"            ]          }        ],        "size": 1639034,        "title": "computer.gif",        "user": "U123ABC456",        "username": ""      },      "type": "file"    }  ],  "ok": true,  "response_metadata": {    "next_cursor": "dGVhbTpDMUg5UkVTR0w="  }}
```

#### Typical error response

```
{  "ok": false,  "error": "invalid_auth"}
```

Every item in the list has a `type` property, while the other properties depend on the type of item. The possible types are:

*   **`message`**: the item will have a `message` property containing a [message object](/messaging) and a `channel` property containing the channel ID for the message
*   **`file`**: this item will have a `file` property containing a [file object](/reference/objects/file-object)
*   **`file_comment`**: the item will have a `file` property containing the [file object](/reference/objects/file-object) and a `comment` property containing the file comment

The `users` array in the `reactions` property will always contain the authenticated user, but might not always contain all users that have reacted. The value of `count`, however, will always represent the count of all users who made that reaction (i.e. it may be greater than `users.length`).

If the user has multiple reactions on a single message, then the relevant message will appear as multiple identical items in the list - one item for each reaction.

Pagination information follows the returned list of items, and contains:

*   the `count` of items returned, up to 1000
*   the `total` number of items reacted to
*   the `page` of results returned in this response, up to 100
*   the total number of `pages` available

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

`user_not_found`

Value passed for `user` was invalid.
