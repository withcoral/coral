Source: https://docs.slack.dev/reference/methods/search.messages

# search.messages method

DocsCall generator

## Facts {#facts}

**Description**Searches for messages matching a query.

**Method Access**

*   HTTP
*   Slack CLI
*   JavaScript
*   Python
*   Java

```
GET https://slack.com/api/search.messages
```

[![slack-cli](/img/logos/slack-cli.png)](/tools/slack-cli)

```
slack api search.messages
```

[![bolt-js](/img/logos/bolt-js-logo.svg)](/tools/bolt-js)

```
app.client.search.messages
```

[![bolt-py](/img/logos/bolt-py-logo.svg)](/tools/bolt-python)

```
app.client.search_messages
```

[![bolt-java](/img/logos/bolt-java-logo.svg)](/tools/java-slack-sdk/guides/getting-started-with-bolt)

```
app.client().searchMessages
```

**Scopes**

User token:

[`search:read`](/reference/scopes/search.read)

**Content types**

`application/x-www-form-urlencoded`

`application/json`

**Rate Limits**[Tier 2: 20+ per minute](/apis/web-api/rate-limits)

## Arguments {#arguments}

### Required arguments

**`token`**`string`Required

Authentication token bearing required scopes. Tokens should be passed as an HTTP Authorization header or alternatively, as a POST parameter.

_Example:_ `xxxx-xxxxxxxxx-xxxx`

**`query`**`string`Required

Search query.

_Example:_ `pickleface`

### Optional arguments

**`count`**`integer`Optional

Pass the number of results you want per "page". Maximum of `100`.

_Default:_ `20`

**`highlight`**`boolean`Optional

Pass a value of `true` to enable query highlight markers (see below).

_Example:_ `true`

**`page`**`integer`Optional

_Default:_ `1`

**`cursor`**`string`Optional

Use this when getting results with cursormark pagination. For first call send `*` for subsequent calls, send the value of `next_cursor` returned in the previous call's results

**`sort`**`string`Optional

Return matches sorted by either `score` or `timestamp`.

_Default:_ `score`

_Example:_ `timestamp`

**`sort_dir`**`string`Optional

Change sort direction to ascending (`asc`) or descending (`desc`).

_Default:_ `desc`

_Acceptable values:_ `asc` `desc`

_Example:_ `asc`

**`team_id`**`string`Optional

encoded team id to search in, required if org token is used

## Usage info {#usage-info}

This is a legacy method

We recommend using the [Real-time Search API](/apis/web-api/real-time-search-api) ([`assistant.search.context`](/reference/methods/assistant.search.context) method) instead.

This method returns messages matching a search query.

The `team_id` is only relevant when using an org-level token. This field will be ignored if the API call is sent using a workspace-level token.

* * *

## Response {#response}

#### Typical success response

```
{  "messages": {    "matches": [      {        "channel": {          "id": "C12345678",          "is_ext_shared": false,          "is_mpim": false,          "is_org_shared": false,          "is_pending_ext_shared": false,          "is_private": false,          "is_shared": false,          "name": "general",          "pending_shared": []        },        "iid": "cb64bdaa-c1e8-4631-8a91-0f78080113e9",        "permalink": "https://hitchhikers.slack.com/archives/C12345678/p1508284197000015",        "team": "T12345678",        "text": "The meaning of life the universe and everything is 42.",        "ts": "1508284197.000015",        "type": "message",        "user": "U2U85N1RV",        "username": "roach"      },      {        "channel": {          "id": "C12345678",          "is_ext_shared": false,          "is_mpim": false,          "is_org_shared": false,          "is_pending_ext_shared": false,          "is_private": false,          "is_shared": false,          "name": "random",          "pending_shared": []        },        "iid": "9a00d3c9-bd2d-45b0-988b-6cff99ae2a90",        "permalink": "https://hitchhikers.slack.com/archives/C12345678/p1508795665000236",        "team": "T12345678",        "text": "The meaning of life the universe and everything is 101010",        "ts": "1508795665.000236",        "type": "message",        "user": "",        "username": "robot overlord"      }    ],    "pagination": {      "first": 1,      "last": 2,      "page": 1,      "page_count": 1,      "per_page": 20,      "total_count": 2    },    "paging": {      "count": 20,      "page": 1,      "pages": 1,      "total": 2    },    "total": 2  },  "ok": true,  "query": "The meaning of life the universe and everything"}
```

#### Typical error response

```
{  "error": "No query passed",  "ok": false}
```

The matching items are returned as hashes containing contextual messages. This response envelope also contains paging and result information.

When using a user token with this method, search results will be affected by the search filters set in the Slack UI. When a search query matches multiple messages in close proximity to one another, only one match will be returned. Using the `highlights=true` parameter, you can identify which items match the query, and which are provided for context only.

Note: Previously, search results might be returned with matches on the `previous`, `previous_2`, `next`, or `next_2` messages, but not the message itself. However, the `previous`, `previous_2`, `next`, or `next_2` fields are now deprecated and will no longer be provided in responses beginning December 3, 2020.

If more than one search term is provided, users and channels are also matched at a lower priority. To specifically search within a channel, group, or DM, add `in:channel_name`, `in:group_name`, or `in:<@UserID>`. To search for messages from a specific speaker, add `from:<@UserID>` or `from:botname`.

For IM results, the `type` is set to `"im"` and the `channel.name` property contains the user ID of the target user. For private group results, type is set to `"group"`.

All search methods support the `highlight` parameter. If specified, the matching query terms will be marked up in the results so that clients may replace them with appropriate highlighting markers (e.g. `<span class="highlight"></span>`). The UTF-8 markers we use are:

```
start: "\xEE\x80\x80"; # U+E000 (private-use)end  : "\xEE\x80\x81"; # U+E001 (private-use)
```

Please note that the max `count` value is `100` and the max `page` value is `100`.

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

`no_query`

No query was provided.

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
