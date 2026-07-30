Source: https://docs.slack.dev/reference/methods/conversations.replies

# conversations.replies method

DocsCall generator

## Facts {#facts}

**Description**Retrieve a thread of messages posted to a conversation

**Method Access**

*   HTTP
*   Slack CLI
*   JavaScript
*   Python
*   Java

```
GET https://slack.com/api/conversations.replies
```

[![slack-cli](/img/logos/slack-cli.png)](/tools/slack-cli)

```
slack api conversations.replies
```

[![bolt-js](/img/logos/bolt-js-logo.svg)](/tools/bolt-js)

```
app.client.conversations.replies
```

[![bolt-py](/img/logos/bolt-py-logo.svg)](/tools/bolt-python)

```
app.client.conversations_replies
```

[![bolt-java](/img/logos/bolt-java-logo.svg)](/tools/java-slack-sdk/guides/getting-started-with-bolt)

```
app.client().conversationsReplies
```

**Scopes**

Bot token:

[`channels:history`](/reference/scopes/channels.history)[`groups:history`](/reference/scopes/groups.history)[`im:history`](/reference/scopes/im.history)[`mpim:history`](/reference/scopes/mpim.history)

User token:

[`channels:history`](/reference/scopes/channels.history)[`groups:history`](/reference/scopes/groups.history)[`im:history`](/reference/scopes/im.history)[`mpim:history`](/reference/scopes/mpim.history)

**Content types**

`application/x-www-form-urlencoded`

`application/json`

**Rate Limits**[Tier 3: 50+ per minute](/apis/web-api/rate-limits)

## Arguments {#arguments}

### Required arguments

**`token`**`string`Required

Authentication token bearing required scopes. Tokens should be passed as an HTTP Authorization header or alternatively, as a POST parameter.

_Example:_ `xxxx-xxxxxxxxx-xxxx`

**`channel`**`string`Required

Conversation ID to fetch thread from.

**`ts`**`string`Required

Unique identifier of either a thread’s parent message or a message in the thread. `ts` must be the timestamp of an existing message with 0 or more replies. If there are no replies then just the single message referenced by `ts` will return - it is just an ordinary, unthreaded message.

### Optional arguments

**`cursor`**`string`Optional

Paginate through collections of data by setting the `cursor` parameter to a `next_cursor` attribute returned by a previous request's `response_metadata`. Default value fetches the first "page" of the collection. See [pagination](/apis/web-api/pagination) for more detail.

_Example:_ `dXNlcjpVMDYxTkZUVDI=`

**`include_all_metadata`**`boolean`Optional

Return all metadata associated with this message.

0

_Example:_ `true`

**`inclusive`**`boolean`Optional

Include messages with `oldest` or `latest` timestamps in results. Ignored unless either timestamp is specified.

0

_Example:_ `true`

**`latest`**`string`Optional

Only messages before this Unix timestamp will be included in results.

_Default:_ `now`

**`limit`**`number`Optional

The maximum number of items to return. Fewer than the requested number of items may be returned, even if the end of the users list hasn't been reached.

_Default:_ `1000`

_Example:_ `20`

**`oldest`**`string`Optional

Only messages after this Unix timestamp will be included in results.

_Default:_ `0`

## Usage info {#usage-info}

This [Conversations API](/apis/web-api/using-the-conversations-api) method returns a cursor-paginated thread of messages posted to a conversation.

This method has different rates for non-Marketplace commercially distributed apps.

As of May 29, 2025, for new applications and installation commercially distributed outside of the Marketplace, this method is rate limited to 1 request per minute. The maximum and default values for the `limit` parameter have both been reduced to 15 objects.

For Marketplace and internal customer-built applications, this method has Tier 3 rate limits.

Existing installations of applications published and distributed outside the Slack Marketplace will not be subject to the new posted limits.

The `channel` and `ts` arguments are always required. `ts` must be the timestamp of an existing message. If no replies, just the single message referenced by `ts` will return.

The `reply_users` field returned by this method sometimes contains bot IDs rather than user IDs. This is essentially a fallback that can occur depending on how the message was posted. It is recommended to check for this either on the prefix or by implementing a retry mechanism if user lookup fails.

The `thread_not_found` error shown in the example error response can also apply to the [`channel_leave`](/reference/events/message/channel_leave) and [`channel_join`](/reference/events/message/channel_join) message subtypes, as these message subtypes cannot be threaded.

* * *

## Response {#response}

#### Typical success response

```
{  "messages": [    {      "type": "message",      "user": "U061F7AUR",      "text": "island",      "thread_ts": "1482960137.003543",      "reply_count": 3,      "subscribed": true,      "last_read": "1484678597.521003",      "unread_count": 0,      "ts": "1482960137.003543"    },    {      "type": "message",      "user": "U061F7AUR",      "text": "one island",      "thread_ts": "1482960137.003543",      "parent_user_id": "U061F7AUR",      "ts": "1483037603.017503"    },    {      "type": "message",      "user": "U061F7AUR",      "text": "two island",      "thread_ts": "1482960137.003543",      "parent_user_id": "U061F7AUR",      "ts": "1483051909.018632"    },    {      "type": "message",      "user": "U061F7AUR",      "text": "three for the land",      "thread_ts": "1482960137.003543",      "parent_user_id": "U061F7AUR",      "ts": "1483125339.020269"    }  ],  "has_more": true,  "ok": true,  "response_metadata": {    "next_cursor": "bmV4dF90czoxNDg0Njc4MjkwNTE3MDkx"  }}
```

#### Typical error response

```
{  "ok": false,  "error": "thread_not_found"}
```

### Pagination {#pagination}

This method uses cursor-based pagination to make it easier to incrementally collect information. To begin pagination, specify a `limit` value under `1000`. We recommend no more than `200` results at a time.

Responses will include a top-level `response_metadata` attribute containing a `next_cursor` value. By using this value as a `cursor` parameter in a subsequent request, along with `limit`, you may navigate through the collection page by virtual page.

See [pagination](/apis/web-api/pagination) for more information.

### Pagination by time {#pagination-by-time}

This form of pagination can be used in conjunction with cursors.

The `messages` array contains up to 1000 messages between the `oldest` and `latest` timestamps. The earliest messages in the time range are returned first.

If there were more than 1000 messages between `oldest` and `latest`, then `has_more` will be `true` in the response. In an additional call, set the `ts` value of the final message as `latest` to get the next page of messages.

If a message has the same timestamp as `oldest` or `latest` it will not be included in the list. This functionality allows you to use the timestamps of specific messages as boundaries for the results. You can, however, have both timestamps be included in the time range by setting `inclusive` to `true`. The `inclusive` parameter is ignored when `oldest` or `latest` is not specified.

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

Value for `channel` was missing or invalid.

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

`invalid_cursor`

Value passed for `cursor` was not valid or is no longer valid.

`invalid_form_data`

The method was called via a `POST` request with `Content-Type` `application/x-www-form-urlencoded` or `multipart/form-data`, but the form data was either missing or syntactically invalid.

`invalid_metadata_filter_keys`

Value passed for `metadata_keys_to_include` was invalid. Must be valid json array of strings.

`invalid_post_type`

The method was called via a `POST` request, but the specified `Content-Type` was invalid. Valid types are: `application/json` `application/x-www-form-urlencoded` `multipart/form-data` `text/plain`.

`invalid_ts_latest`

Value passed for `latest` was invalid

`invalid_ts_oldest`

Value passed for `oldest` was invalid

`list_record_comment_fetch_failed`

Failed to fetch list record comments.

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

`thread_not_found`

Value for `ts` was missing or invalid.

`token_expired`

Authentication token has expired

`token_revoked`

Authentication token is for a deleted user or workspace or the app has been removed when using a `user` token.

`two_factor_setup_required`

Two factor setup is required.
