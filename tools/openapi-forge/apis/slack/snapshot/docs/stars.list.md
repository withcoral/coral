Source: https://docs.slack.dev/reference/methods/stars.list

# stars.list method

DocsCall generator

## Facts {#facts}

**Description**Listed a user's saved items, formerly known as stars.

**Method Access**

*   HTTP
*   Slack CLI
*   JavaScript
*   Python
*   Java

```
GET https://slack.com/api/stars.list
```

[![slack-cli](/img/logos/slack-cli.png)](/tools/slack-cli)

```
slack api stars.list
```

[![bolt-js](/img/logos/bolt-js-logo.svg)](/tools/bolt-js)

```
app.client.stars.list
```

[![bolt-py](/img/logos/bolt-py-logo.svg)](/tools/bolt-python)

```
app.client.stars_list
```

[![bolt-java](/img/logos/bolt-java-logo.svg)](/tools/java-slack-sdk/guides/getting-started-with-bolt)

```
app.client().starsList
```

**Scopes**

User token:

[`stars:read`](/reference/scopes/stars.read)

**Content types**

`application/x-www-form-urlencoded`

`application/json`

**Rate Limits**[Tier 3: 50+ per minute](/apis/web-api/rate-limits)

## Arguments {#arguments}

### Required arguments

**`token`**`string`Required

Authentication token bearing required scopes. Tokens should be passed as an HTTP Authorization header or alternatively, as a POST parameter.

_Example:_ `xxxx-xxxxxxxxx-xxxx`

### Optional arguments

**`count`**`integer`Optional

_Default:_ `100`

**`cursor`**`string`Optional

Parameter for pagination. Set `cursor` equal to the `next_cursor` attribute returned by the previous request's `response_metadata`. This parameter is optional, but pagination is mandatory: the default value simply fetches the first "page" of the collection. See [pagination](/apis/web-api/pagination) for more details.

_Example:_ `dXNlcjpVMDYxTkZUVDI=`

**`limit`**`integer`Optional

The maximum number of items to return. Fewer than the requested number of items may be returned, even if the end of the list hasn't been reached.

0

_Example:_ `20`

**`page`**`integer`Optional

_Default:_ `1`

**`team_id`**`string`Optional

encoded team id to list stars in, required if org token is used

## Usage info {#usage-info}

Stars can still be listed via `stars.list` but they can no longer be viewed or interacted with by end-users.

We recommend retiring any app functionality that relies on `stars` APIs.

End-users can use the [new Later view](https://slack.com/help/articles/13453851074067-Save-it-for-Later), but Later APIs are not currently available. As a result of this transition, the `stars.list` method will no longer reflect anything new that users are saving.

See [this changelog](/changelog/2023-07-its-later-already-for-stars-and-reminders) for more information.

This method lists the items starred by the authed user.

* * *

## Response {#response}

#### Typical success response

```
{  "ok": true,  "items": [    {      "type": "message",      "channel": "C123ABC456",      "message": {        "type": "message",        "subtype": "bot_message",        "text": "",        "ts": "1655762568.324229",        "username": "username",        "icons": {          "emoji": ":test:"        },        "bot_id": "BSLACKBOT",        "attachments": [          {            "color": "ecb438",            "ts": 1655762568,            "id": 1,            "fallback": "some text",            "text": "some text",            "pretext": "*chat.postMessage*",            "mrkdwn_in": [              "pretext",              "text"            ]          }        ],        "permalink": "https://your-workspace.slack.com/archives/C123ABC456/p123456789"      },      "date_create": 1656014995    }  ],  "paging": {    "per_page": 100,    "spill": 0,    "page": 1,    "total": 1,    "pages": 1  }}
```

#### Typical error response

```
{  "ok": false,  "error": "invalid_auth"}
```

The response contains a list of starred items followed by pagination information.

Different item types can be starred. Every item in the list has a `type` property, the other property depend on the type of item. The possible types are:

*   **`message`**: the item will have a `message` property containing a [message object](/messaging)
*   **`file`**: this item will have a `file` property containing a [file object](/reference/objects/file-object).
*   **`file_comment`**: the item will have a `file` property containing the [file object](/reference/objects/file-object) and a `comment` property containing the file comment.
*   **`channel`**: the item will have a `channel` property containing the channel ID.
*   **`im`**: the item will have a `channel` property containing the channel ID for this direct message.
*   **`group`**: the item will have a `group` property containing the channel ID for the private group.

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

`missing_argument`

A required argument is missing.

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
