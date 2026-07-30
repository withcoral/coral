Source: https://docs.slack.dev/reference/methods/conversations.info

# conversations.info method

DocsCall generator

## Facts {#facts}

**Description**Retrieve information about a conversation.

**Method Access**

*   HTTP
*   Slack CLI
*   JavaScript
*   Python
*   Java

```
GET https://slack.com/api/conversations.info
```

[![slack-cli](/img/logos/slack-cli.png)](/tools/slack-cli)

```
slack api conversations.info
```

[![bolt-js](/img/logos/bolt-js-logo.svg)](/tools/bolt-js)

```
app.client.conversations.info
```

[![bolt-py](/img/logos/bolt-py-logo.svg)](/tools/bolt-python)

```
app.client.conversations_info
```

[![bolt-java](/img/logos/bolt-java-logo.svg)](/tools/java-slack-sdk/guides/getting-started-with-bolt)

```
app.client().conversationsInfo
```

**Scopes**

Bot token:

[`channels:read`](/reference/scopes/channels.read)[`groups:read`](/reference/scopes/groups.read)[`im:read`](/reference/scopes/im.read)[`mpim:read`](/reference/scopes/mpim.read)

User token:

[`channels:read`](/reference/scopes/channels.read)[`groups:read`](/reference/scopes/groups.read)[`im:read`](/reference/scopes/im.read)[`mpim:read`](/reference/scopes/mpim.read)

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

Conversation ID to learn more about

### Optional arguments

**`include_locale`**`boolean`Optional

Set this to `true` to receive the locale for this conversation. Defaults to `false`

**`include_num_members`**`boolean`Optional

Set to `true` to include the member count for the specified conversation. Defaults to `false`

_Default:_ `false`

_Example:_ `true`

## Usage info {#usage-info}

This [Conversations API](/apis/web-api/using-the-conversations-api) method returns information about a workspace [conversation](/reference/objects/conversation-object).

* * *

## Response {#response}

#### Typical success response for a public channel. A response from a private channel and a multi-party IM is very similar to this example. Note that if the properties.tabs parameter is an empty set, it will not be included in the channel object.

```
{  "ok": true,  "channel": {    "id": "C012AB3CD",    "name": "general",    "is_channel": true,    "is_group": false,    "is_im": false,    "is_mpim": false,    "is_private": false,    "created": 1654868334,    "is_archived": false,    "is_general": true,    "unlinked": 0,    "name_normalized": "general",    "is_shared": false,    "is_frozen": false,    "is_org_shared": false,    "is_pending_ext_shared": false,    "pending_shared": [],    "context_team_id": "T123ABC456",    "updated": 1723130875818,    "parent_conversation": null,    "creator": "U123ABC456",    "is_ext_shared": false,    "shared_team_ids": [      "T123ABC456"    ],    "pending_connected_team_ids": [],    "topic": {      "value": "For public discussion of generalities",      "creator": "W012A3BCD",      "last_set": 1449709364    },    "purpose": {      "value": "This part of the workspace is for fun. Make fun here.",      "creator": "W012A3BCD",      "last_set": 1449709364    },    "properties": {      "tabs": [        {          "id": "workflows",          "label": "",          "type": "workflows"        },        {          "id": "files",          "label": "",          "type": "files"        },        {          "id": "bookmarks",          "label": "",          "type": "bookmarks"        }      ]    },    "previous_names": []  }}
```

#### Typical success response for a 1:1 direct message

```
{  "ok": true,  "channel": {    "id": "C012AB3CD",    "created": 1507235627,    "is_im": true,    "is_org_shared": false,    "user": "U27FFLNF4",    "last_read": "1513718191.000038",    "latest": {      "type": "message",      "user": "U5R3PALPN",      "text": "Psssst!",      "ts": "1513718191.000038"    },    "unread_count": 0,    "unread_count_display": 0,    "is_open": true,    "locale": "en-US",    "priority": 0.043016851216706  }}
```

#### When using the method with the include_num_members parameter, we return a num_members field

```
{  "ok": true,  "channel": {    "id": "C012AB3CD",    "created": 1507235627,    "is_im": true,    "is_org_shared": false,    "user": "U27FFLNF4",    "last_read": "1513718191.000038",    "latest": {      "type": "message",      "user": "U5R3PALPN",      "text": "Psssst!",      "ts": "1513718191.000038"    },    "unread_count": 0,    "unread_count_display": 0,    "is_open": true,    "locale": "en-US",    "priority": 0.043016851216706,    "num_members": 2  }}
```

#### Typical error response when a channel cannot be found

```
{  "ok": false,  "error": "channel_not_found"}
```

Returns a [conversation object](/reference/objects/conversation-object), which could be a public channel, private channel, direct message, multi-person direct message, depending completely on the `channel` ID and the permissions granted to your token. Some fields in the response, like `unread_count` and `unread_count_display`, are included for DM conversations only.

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

`channel_is_limited_access`

The user has no access to the channel. Only applicable to Salesforce limited access channels.

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
