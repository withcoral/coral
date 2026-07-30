Source: https://docs.slack.dev/reference/methods/team.externalTeams.list

# team.externalTeams.list method

DocsCall generator

## Facts {#facts}

**Description**Returns a list of all the external teams connected and details about the connection.

**Method Access**

*   HTTP
*   Slack CLI
*   JavaScript
*   Python
*   Java

```
GET https://slack.com/api/team.externalTeams.list
```

[![slack-cli](/img/logos/slack-cli.png)](/tools/slack-cli)

```
slack api team.externalTeams.list
```

[![bolt-js](/img/logos/bolt-js-logo.svg)](/tools/bolt-js)

```
app.client.team.externalTeams.list
```

[![bolt-py](/img/logos/bolt-py-logo.svg)](/tools/bolt-python)

```
app.client.team_externalTeams_list
```

[![bolt-java](/img/logos/bolt-java-logo.svg)](/tools/java-slack-sdk/guides/getting-started-with-bolt)

```
app.client().teamExternalTeamsList
```

**Scopes**

Bot token:

[`conversations.connect:manage`](/reference/scopes/conversations.connect.manage)[`team:read`](/reference/scopes/team.read)

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

**`limit`**`integer`Optional

The maximum number of items to return per page

_Default:_ `20`

**`cursor`**`string`Optional

Paginate through collections of data by setting parameter to the `team_id` attribute returned by a previous request's `response_metadata`. If not provided, the first page of the collection is returned. See [pagination](/apis/web-api/pagination#cursors) for more detail.

_Example:_ `T123ABC456`

**`sort_field`**`string`Optional

Name of the parameter that we are sorting by

_Default:_ `team_name`

_Acceptable values:_ `team_name` `last_active_timestamp` `connection_status`

**`sort_direction`**`string`Optional

Direction to sort in asc or desc

_Default:_ `asc`

_Acceptable values:_ `asc` `desc`

**`slack_connect_pref_filter`**`array`Optional

Filters connected orgs by Slack Connect pref override(s). Value can be: `approved_orgs_only` `allow_sc_file_uploads` `profile_visibility` `away_team_sc_invite_permissions` `accept_sc_invites` `sc_mpdm_to_private` `require_sc_channel_for_sc_dm` `external_awareness_context_bar`

**`workspace_filter`**`array`Optional

Shows connected orgs which are connected on a specified encoded workspace ID

**`connection_status_filter`**`string`Optional

Status of the connected team.

_Acceptable values:_ `CONNECTED` `DISCONNECTED` `BLOCKED` `IN_REVIEW`

## Usage info {#usage-info}

The features within are only available to Slack workspaces on an Enterprise plan.

Don't have a paid plan? Join the [Developer Program](https://api.slack.com/developer-program) and provision a fully-featured sandbox for free.

Use this method to return information about teams connected via [Slack Connect](/apis/slack-connect/using-slack-connect-api-methods).

* * *

## Response {#response}

#### Typical success response

```
{  "ok": true,  "organizations": [    {      "team_id": "T123ABC456",      "team_name": "Sandra Inc.",      "team_domain": "sandra",      "public_channel_count": 1,      "private_channel_count": 1,      "im_channel_count": 1,      "mpim_channel_count": 1,      "connected_workspaces": {        "workspace_id": "Jesse Inc",        "workspace_name": "E123ABC456"      },      "slack_connect_prefs": {},      "connection_status": "CONNECTED",      "last_active_timestamp": 1718656058,      "is_sponsored": false,      "canvas": {        "total_count": 1,        "ownership_details": [          {            "team_id": "T123ABC456"          },          {            "count": 1          }        ]      },      "lists": {        "total_count": 1,        "ownership_details": [          {            "team_id": "T123ABC456"          },          {            "count": 1          }        ]      }    }  ],  "total_count": 1,  "response_metadata": {    "next_cursor": "T123ABC999"  }}
```

#### Typical error response

```
{  "ok": false,  "error": "invalid_auth"}
```

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

There was an internal error processing this request.

`internal_error`

The server could not complete your operation(s) without encountering an error, likely due to a transient issue on our end. It's possible some aspect of the operation succeeded before the error was raised.

`invalid_arg_name`

The method was passed an argument whose name falls outside the bounds of accepted or expected values. This includes very long names and names with non-alphanumeric characters other than `_`. If you get this error, it is typically an indication that you have made a _very_ malformed API call.

`invalid_arguments`

One or more of the API arguments are invalid.

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

`invalid_workspace_filter`

The specified workspace is not valid.

`method_deprecated`

The method has been deprecated.

`missing_post_type`

The method was called via a `POST` request and included a data payload, but the request did not include a `Content-Type` header.

`missing_scope`

The token used is not granted the specific scope permissions required to complete this request.

`no_permission`

The workspace token used in this request does not have the permissions necessary to complete the request. Make sure your app is a member of the conversation it's attempting to post a message to.

`not_allowed`

The user is not allowed to perform the action.

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

The user does not have permission to perform the action.

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

`user_cannot_manage_workspace`

The calling user cannot manage the workspace passed in the workspace filter.
