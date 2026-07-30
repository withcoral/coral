Source: https://docs.slack.dev/reference/methods/users.list

# users.list method

DocsCall generator

## Facts {#facts}

**Description**Lists all users in a Slack team.

**Method Access**

*   HTTP
*   Slack CLI
*   JavaScript
*   Python
*   Java

```
GET https://slack.com/api/users.list
```

[![slack-cli](/img/logos/slack-cli.png)](/tools/slack-cli)

```
slack api users.list
```

[![bolt-js](/img/logos/bolt-js-logo.svg)](/tools/bolt-js)

```
app.client.users.list
```

[![bolt-py](/img/logos/bolt-py-logo.svg)](/tools/bolt-python)

```
app.client.users_list
```

[![bolt-java](/img/logos/bolt-java-logo.svg)](/tools/java-slack-sdk/guides/getting-started-with-bolt)

```
app.client().usersList
```

**Scopes**

Bot token:

[`users:read`](/reference/scopes/users.read)

User token:

[`users:read`](/reference/scopes/users.read)

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

**`cursor`**`string`Optional

Paginate through collections of data by setting the `cursor` parameter to a `next_cursor` attribute returned by a previous request's `response_metadata`. Default value fetches the first "page" of the collection. See [pagination](/apis/web-api/pagination) for more detail.

_Example:_ `dXNlcjpVMDYxTkZUVDI=`

**`include_locale`**`boolean`Optional

Set this to `true` to receive the locale for users. Defaults to `false`

**`limit`**`number`Optional

The maximum number of items to return. Fewer than the requested number of items may be returned, even if the end of the users list hasn't been reached. Providing no `limit` value will result in Slack attempting to deliver you the entire result set. If the collection is too large you may experience `limit_required` or HTTP 500 errors.

0

_Example:_ `20`

**`team_id`**`string`Optional

encoded team id to list users in, required if org token is used

## Usage info {#usage-info}

This method returns a list of all users in the workspace. This includes both invited users and deleted/deactivated users.

The `team_id` is only relevant when using an org-level token. This field will be ignored if the API call is sent using a workspace-level token.

* * *

## Response {#response}

#### Typical success response

```
{  "ok": true,  "members": [    {      "id": "W012A3CDE",      "team_id": "T012AB3C4",      "name": "spengler",      "deleted": false,      "color": "9f69e7",      "real_name": "spengler",      "tz": "America/Los_Angeles",      "tz_label": "Pacific Daylight Time",      "tz_offset": -25200,      "profile": {        "avatar_hash": "ge3b51ca72de",        "status_text": "Print is dead",        "status_emoji": ":books:",        "real_name": "Egon Spengler",        "display_name": "spengler",        "real_name_normalized": "Egon Spengler",        "display_name_normalized": "spengler",        "email": "spengler@ghostbusters.example.com",        "image_24": "https://.../avatar/e3b51ca72dee4ef87916ae2b9240df50.jpg",        "image_32": "https://.../avatar/e3b51ca72dee4ef87916ae2b9240df50.jpg",        "image_48": "https://.../avatar/e3b51ca72dee4ef87916ae2b9240df50.jpg",        "image_72": "https://.../avatar/e3b51ca72dee4ef87916ae2b9240df50.jpg",        "image_192": "https://.../avatar/e3b51ca72dee4ef87916ae2b9240df50.jpg",        "image_512": "https://.../avatar/e3b51ca72dee4ef87916ae2b9240df50.jpg",        "team": "T012AB3C4"      },      "is_admin": true,      "is_owner": false,      "is_primary_owner": false,      "is_restricted": false,      "is_ultra_restricted": false,      "is_bot": false,      "updated": 1502138686,      "is_app_user": false,      "has_2fa": false    },    {      "id": "W07QCRPA4",      "team_id": "T0G9PQBBK",      "name": "glinda",      "deleted": false,      "color": "9f69e7",      "real_name": "Glinda Southgood",      "tz": "America/Los_Angeles",      "tz_label": "Pacific Daylight Time",      "tz_offset": -25200,      "profile": {        "avatar_hash": "8fbdd10b41c6",        "image_24": "https://a.slack-edge.com...png",        "image_32": "https://a.slack-edge.com...png",        "image_48": "https://a.slack-edge.com...png",        "image_72": "https://a.slack-edge.com...png",        "image_192": "https://a.slack-edge.com...png",        "image_512": "https://a.slack-edge.com...png",        "image_1024": "https://a.slack-edge.com...png",        "image_original": "https://a.slack-edge.com...png",        "first_name": "Glinda",        "last_name": "Southgood",        "title": "Glinda the Good",        "phone": "",        "skype": "",        "real_name": "Glinda Southgood",        "real_name_normalized": "Glinda Southgood",        "display_name": "Glinda the Fairly Good",        "display_name_normalized": "Glinda the Fairly Good",        "email": "glenda@south.oz.coven"      },      "is_admin": true,      "is_owner": false,      "is_primary_owner": false,      "is_restricted": false,      "is_ultra_restricted": false,      "is_bot": false,      "updated": 1480527098,      "has_2fa": false    }  ],  "cache_ts": 1498777272,  "response_metadata": {    "next_cursor": "dXNlcjpVMEc5V0ZYTlo="  }}
```

#### Typical error response

```
{  "ok": false,  "error": "invalid_cursor"}
```

Returns a list of [paginated](/apis/web-api/pagination) [user objects](/reference/objects/user-object), in no particular order.

## Profile {#profile}

The profile hash contains as much information as the user has supplied in the default profile fields: `first_name`, `last_name`, `real_name`, `email`, `skype`, and the `image_*` fields. Data that has not been supplied may not be present at all, may be null or may contain an empty string (`""`).

A user's custom profile fields may be discovered using [`users.profile.get`](/reference/methods/users.profile.get).

## Email addresses {#email-addresses}

Accessing Email Addresses

The [`users:read.email`](/reference/scopes/users.read.email) OAuth scope is now required to access the `email` field in user objects returned by the [`users.list`](/reference/methods/users.list) and [`users.info`](/reference/methods/users.info) web API methods. [`users:read`](/reference/scopes/users.read) is no longer a sufficient scope for this data field. [Learn more](/changelog/2017-04-narrowing-email-access).

Apps created after January 4th, 2017 must request _both_ the `users:read` and `users:read.email` OAuth permission scopes when using the [OAuth app installation flow](/authentication/installing-with-oauth) to enable access to the `email` field of user objects returned by this method.

### Pagination {#pagination}

This method uses cursor-based pagination to make it easier to incrementally collect information. To begin pagination, specify a `limit` value under `1000`. We recommend no more than `200` results at a time.

Responses will include a top-level `response_metadata` attribute containing a `next_cursor` value. By using this value as a `cursor` parameter in a subsequent request, along with `limit`, you may navigate through the collection page by virtual page.

See [pagination](/apis/web-api/pagination) for more information.

## Presence {#presence}

Retrieving a workspace's presence with this method is now deprecated. Use [`users.getPresence`](/reference/methods/users.getPresence) or [presence subscriptions](/apis/web-api/user-presence-and-status#subcriptions) instead.

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

`invalid_cursor`

Value passed for `cursor` was not valid or is no longer valid.

`invalid_form_data`

The method was called via a `POST` request with `Content-Type` `application/x-www-form-urlencoded` or `multipart/form-data`, but the form data was either missing or syntactically invalid.

`invalid_post_type`

The method was called via a `POST` request, but the specified `Content-Type` was invalid. Valid types are: `application/json` `application/x-www-form-urlencoded` `multipart/form-data` `text/plain`.

`limit_required`

For large teams a limit is required.

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
