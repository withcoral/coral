Source: https://docs.slack.dev/reference/methods/team.accessLogs

# team.accessLogs method

DocsCall generator

## Facts {#facts}

**Description**Gets the access logs for the current team.

**Method Access**

*   HTTP
*   Slack CLI
*   JavaScript
*   Python
*   Java

```
GET https://slack.com/api/team.accessLogs
```

[![slack-cli](/img/logos/slack-cli.png)](/tools/slack-cli)

```
slack api team.accessLogs
```

[![bolt-js](/img/logos/bolt-js-logo.svg)](/tools/bolt-js)

```
app.client.team.accessLogs
```

[![bolt-py](/img/logos/bolt-py-logo.svg)](/tools/bolt-python)

```
app.client.team_accessLogs
```

[![bolt-java](/img/logos/bolt-java-logo.svg)](/tools/java-slack-sdk/guides/getting-started-with-bolt)

```
app.client().teamAccessLogs
```

**Scopes**

User token:

[`admin`](/reference/scopes/admin)

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

**`before`**Optional

End of time range of logs to include in results (inclusive).

_Default:_ `now`

_Acceptable values:_ `now`

_Example:_ `1457989166`

**`count`**`string`Optional

_Default:_ `100`

**`page`**`string`Optional

_Default:_ `1`

**`cursor`**`string`Optional

Parameter for pagination. Set `cursor` equal to the `next_cursor` attribute returned by the previous request's `response_metadata`. This parameter is optional, but pagination is mandatory: the default value simply fetches the first "page" of the collection. See [pagination](/apis/web-api/pagination) for more details.

_Example:_ `dXNlcjpVMDYxTkZUVDI=`

**`limit`**`integer`Optional

The maximum number of items to return. Fewer than the requested number of items may be returned, even if the end of the list hasn't been reached. If specified, result is returned using a cursor-based approach instead of a classic one.

0

_Example:_ `20`

**`team_id`**`string`Optional

encoded team id to get logs from, required if org token is used

## Usage info {#usage-info}

This method is used to retrieve the "access logs" for users on a workspace.

Each access log entry represents a user accessing Slack from a specific user, IP address, and user agent combination.

The `team_id` is only relevant when using an org-level token. This field will be ignored if the API call is sent using a workspace-level token. On Enterprise Grid, passing a workspace ID returns only team site and 3rd-party app access. Passing no `team_id` returns complete access data for the org (desktop, mobile, web).

Need to time travel? Set the `before` parameter to the oldest timestamp returned by the method to browse access logs from further back in time.

Need richer login event data (e.g., `user_login`, `user_login_failed` with auth method and account type)? Refer to our [Audit Logs API](/reference/audit-logs-api/methods-actions-reference) docs.

* * *

## Response {#response}

#### This response demonstrates pagination and two access log entries.

```
{  "ok": true,  "logins": [    {      "user_id": "U45678",      "username": "alice",      "date_first": 1422922864,      "date_last": 1422922864,      "count": 1,      "ip": "127.0.0.1",      "user_agent": "SlackWeb Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.35 Safari/537.36",      "isp": "BigCo ISP",      "country": "US",      "region": "CA"    },    {      "user_id": "U12345",      "username": "white_rabbit",      "date_first": 1422922493,      "date_last": 1422922493,      "count": 1,      "ip": "127.0.0.1",      "user_agent": "SlackWeb Mozilla/5.0 (iPhone; CPU iPhone OS 8_1_3 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Version/8.0 Mobile/12B466 Safari/600.1.4",      "isp": "BigCo ISP",      "country": "US",      "region": "CA"    }  ],  "paging": {    "count": 100,    "total": 2,    "page": 1,    "pages": 1  }}
```

#### A workspace must be on a paid plan to use this method, otherwise the paid_only error is thrown:

```
{  "ok": false,  "error": "paid_only"}
```

The method's paginated response contains a list of access log entries. Each item in the `logins` array represents a number of possible user interactions, collated by the user, IP address, and user agent combination. These include actual logins as well as other API calls that are typically made when accessing Slack. Each access log entry contains the user `id` and `username` that accessed Slack.

`date_first` is a Unix timestamp of the first access log entry for this user, IP address, and user agent combination.

`date_last` is the most recent for that combination.

`count` is the total number of access log entries for that combination.

`ip` is the IP address of the device used.

`user_agent` is the reported user agent string from the browser or client application.

`isp` is our _best guess_ at the internet service provider owning the IP address.

`country` and `region` are also our best guesses on where the access originated, based on the IP address.

### Pagination {#pagination}

The paging information contains the `count` of items returned, the `total` number of items reacted to, the `page` of results returned in this response, and the total number of `pages` available. Please note that the max `count` value is `1000` and the max `page` value is `100`.

This method also supports cursor-based pagination to make it easier to incrementally collect information. To begin pagination, specify a `limit` value under `1000`. Responses will include a top-level `response_metadata` attribute containing a `next_cursor` value. Use this value as a `cursor` parameter in a subsequent request along with `limit` and you may navigate through the collection page by virtual page.

If you encounter a `500` status or `fatal_error` code when calling this method, it is recommended to use cursor-based pagination.

Refer to [pagination](/apis/web-api/pagination) for more information.

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

`invalid_limit`

The value passed for `limit` was not valid.

`invalid_post_type`

The method was called via a `POST` request, but the specified `Content-Type` was invalid. Valid types are: `application/json` `application/x-www-form-urlencoded` `multipart/form-data` `text/plain`.

`invalid_team_id`

The value passed for `team_id` is not valid given the token context.

`method_deprecated`

The method has been deprecated.

`missing_argument`

A required argument is missing.

`missing_post_type`

The method was called via a `POST` request and included a data payload, but the request did not include a `Content-Type` header.

`missing_scope`

The provided token hasn't obtained the necessary scopes to use this method.

`missing_scope`

The token used is not granted the specific scope permissions required to complete this request.

`no_permission`

The workspace token used in this request does not have the permissions necessary to complete the request. Make sure your app is a member of the conversation it's attempting to post a message to.

`not_allowed_token_type`

Method was called with an invalid token type

`not_allowed_token_type`

The token type used in this request is not allowed.

`not_authed`

No authentication token provided.

`org_login_required`

The workspace is undergoing an enterprise migration and will not be available until migration is complete.

`over_pagination_limit`

It is not possible to request more than 1000 items per page or more than 100 pages.

`paid_only`

This is only available to paid teams.

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

token revoked (generated)

`token_revoked`

Authentication token is for a deleted user or workspace or the app has been removed when using a `user` token.

`two_factor_setup_required`

Two factor setup is required.
