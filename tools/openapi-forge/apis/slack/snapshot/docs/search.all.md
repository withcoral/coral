Source: https://docs.slack.dev/reference/methods/search.all

# search.all method

DocsCall generator

## Facts {#facts}

**Description**Searches for messages and files matching a query.

**Method Access**

*   HTTP
*   Slack CLI
*   JavaScript
*   Python
*   Java

```
GET https://slack.com/api/search.all
```

[![slack-cli](/img/logos/slack-cli.png)](/tools/slack-cli)

```
slack api search.all
```

[![bolt-js](/img/logos/bolt-js-logo.svg)](/tools/bolt-js)

```
app.client.search.all
```

[![bolt-py](/img/logos/bolt-py-logo.svg)](/tools/bolt-python)

```
app.client.search_all
```

[![bolt-java](/img/logos/bolt-java-logo.svg)](/tools/java-slack-sdk/guides/getting-started-with-bolt)

```
app.client().searchAll
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

Search query. May contains booleans, etc.

_Example:_ `pickleface`

### Optional arguments

**`count`**`integer`Optional

_Default:_ `20`

**`highlight`**`boolean`Optional

Pass a value of `true` to enable query highlight markers (see below).

_Example:_ `true`

**`page`**`integer`Optional

_Default:_ `1`

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

This method allows users and applications to search both messages and files in a single call.

The `team_id` is only relevant when using an org-level token. This field will be ignored if the API call is sent using a workspace-level token.

* * *

## Response {#response}

#### Typical success response

```
{  "files": {    "matches": [      {        "channels": [],        "comments_count": 1,        "created": 1508804330,        "display_as_bot": false,        "editable": false,        "external_type": "",        "filetype": "png",        "groups": [],        "id": "F7PKF1NR7",        "image_exif_rotation": 1,        "ims": [],        "initial_comment": {          "comment": "Sure! Here's the workflow diagram!",          "created": 1508804330,          "id": "Fc7NLL52E7",          "is_intro": true,          "timestamp": 1508804330,          "user": "U2U85N1RZ"        },        "is_external": false,        "is_public": true,        "mimetype": "image/png",        "mode": "hosted",        "name": "slack workflow diagram.png",        "original_h": 117,        "original_w": 128,        "permalink": "https://example.slack.com/files/U2U85N1RZ/F7PKF1NR7/slack_workflow_diagram.png",        "permalink_public": "https://slack-files.com/T2U81E2FZ-F7PKF1NR7-bea9143f18",        "pretty_type": "PNG",        "preview": null,        "public_url_shared": false,        "score": "0.99982661240974",        "size": 35705,        "thumb_160": "https://files.slack.com/files-tmb/T2U81E2FZ-F7PKF1NR7-19f33fc256/slack_workflow_diagram_160.png",        "thumb_360": "https://files.slack.com/files-tmb/T2U81E2FZ-F7PKF1NR7-19f33fc256/slack_workflow_diagram_360.png",        "thumb_360_h": 117,        "thumb_360_w": 128,        "thumb_64": "https://files.slack.com/files-tmb/T2U81E2FZ-F7PKF1NR7-19f33fc256/slack_workflow_diagram_64.png",        "thumb_80": "https://files.slack.com/files-tmb/T2U81E2FZ-F7PKF1NR7-19f33fc256/slack_workflow_diagram_80.png",        "timestamp": 1508804330,        "title": "slack workflow diagram",        "top_file": false,        "url_private": "https://files.slack.com/files-pri/T2U81E2FZ-F7PKF1NR7/slack_workflow_diagram.png",        "url_private_download": "https://files.slack.com/files-pri/T2U81E2FZ-F7PKF1NR7/download/slack_workflow_diagram.png",        "user": "U2U85N1RZ",        "username": "amy"      }    ],    "pagination": {      "first": 1,      "last": 1,      "page": 1,      "page_count": 1,      "per_page": 20,      "total_count": 1    },    "paging": {      "count": 20,      "page": 1,      "pages": 1,      "total": 1    },    "total": 1  },  "messages": {    "matches": [      {        "channel": {          "id": "C2U86NC6M",          "is_ext_shared": false,          "is_mpim": false,          "is_org_shared": false,          "is_pending_ext_shared": false,          "is_private": false,          "is_shared": false,          "name": "general",          "pending_shared": []        },        "iid": "35692677-e60e-43d9-ac45-1987cea88975",        "next": {          "iid": "6f510ea1-e1d3-4f3f-bdb9-f9c6f6e9d609",          "text": "Thanks!",          "ts": "1508804378.000219",          "type": "message",          "user": "U2U85HJ7R",          "username": "john"        },        "permalink": "https://example.slack.com/archives/C2U86NC6M/p1508804330000296",        "previous": {          "iid": "aba8603c-0543-4fb2-9118-a5ac85f3d138",          "text": "Can you send me the Slack workflow diagram?",          "ts": "1508804301.000026",          "type": "message",          "user": "U2U85HJ7R",          "username": "john"        },        "team": "T2U81E2FZ",        "text": "uploaded a file: <https://example.slack.com/files/U2U85N1RZ/F7PKF1NR7/slack_workflow_diagram.png|slack workflow diagram> and commented: Sure! Here's the workflow diagram!",        "ts": "1508804330.000296",        "type": "message",        "user": "U2U85N1RZ",        "username": "amy"      }    ],    "pagination": {      "first": 1,      "last": 1,      "page": 1,      "page_count": 1,      "per_page": 20,      "total_count": 1    },    "paging": {      "count": 20,      "page": 1,      "pages": 1,      "total": 1    },    "total": 1  },  "ok": true,  "posts": {    "matches": [],    "total": 0  },  "query": "diagram"}
```

#### Typical error response

```
{  "error": "missing_scope",  "needed": "search:read",  "ok": false,  "provided": "identify,bot:basic"}
```

The response returns matches broken down by their type of content, similar to the facebook/gmail auto-completed search widgets.

When using a user token with this method, search results will be affected by the search filters set in the Slack UI. Within each content group, data is returned in the following format:

```
{    "matches": [],    "paging": {        "count": 100,        "total": 15,        "page": 1,        "pages": 1    }}
```

*   `count` - number of records per page
*   `total` - total records matching query
*   `page` - page of records returned
*   `pages` - total pages matching query

This block gives the (estimated) total number of matches of this type, then has an array containing the specified page of the top matches. The format of matches depends on the match type, as described in the documentation for [search.messages](/reference/methods/search.messages) and [search.files](/reference/methods/search.files). These methods can be used to fetch further pages of messages or files.

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
