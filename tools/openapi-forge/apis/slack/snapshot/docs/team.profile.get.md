Source: https://docs.slack.dev/reference/methods/team.profile.get

# team.profile.get method

DocsCall generator

## Facts {#facts}

**Description**Retrieve a team's profile.

**Method Access**

*   HTTP
*   Slack CLI
*   JavaScript
*   Python
*   Java

```
GET https://slack.com/api/team.profile.get
```

[![slack-cli](/img/logos/slack-cli.png)](/tools/slack-cli)

```
slack api team.profile.get
```

[![bolt-js](/img/logos/bolt-js-logo.svg)](/tools/bolt-js)

```
app.client.team.profile.get
```

[![bolt-py](/img/logos/bolt-py-logo.svg)](/tools/bolt-python)

```
app.client.team_profile_get
```

[![bolt-java](/img/logos/bolt-java-logo.svg)](/tools/java-slack-sdk/guides/getting-started-with-bolt)

```
app.client().teamProfileGet
```

**Scopes**

Bot token:

[`users.profile:read`](/reference/scopes/users.profile.read)

User token:

[`users.profile:read`](/reference/scopes/users.profile.read)

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

**`visibility`**`string`Optional

Filter by visibility.

_Acceptable values:_ `all` `visible` `hidden`

_Example:_ `all`

## Usage info {#usage-info}

This method is used to get the profile field definitions for this team.

The `team_id` is only relevant when using an org-level token. This field will be ignored if the API call is sent using a workspace-level token. The optional `visibility` argument allows the client to filter the profile fields based on visibility. The following values are supported:

*   `all` is the default, and returns all fields.
*   `visible` returns only fields for which the `is_hidden` option is missing or false.
*   `hidden` returns only fields for which the `is_hidden` option is true.

* * *

## Response {#response}

#### Typical success response

```
{  "ok": true,  "profile": {    "fields": [      {        "id": "111111ABC",        "ordering": 0,        "label": "Phone extension",        "hint": "Enter the extension to reach your desk",        "type": "text",        "possible_values": null,        "options": {          "is_scim": true,          "is_protected": true        },        "is_hidden": false,        "section_id": "123ABC"      },      {        "id": "222222ABC",        "ordering": 1,        "label": "Date of birth",        "hint": "When you were born",        "type": "date",        "possible_values": null,        "options": {          "is_scim": true,          "is_protected": true        },        "is_hidden": true,        "section_id": "123ABC"      },      {        "id": "333333ABC",        "ordering": 2,        "label": "House",        "hint": "Put on the sorting hat",        "type": "options_list",        "possible_values": [          "Gryffindor",          "Hufflepuff",          "Ravenclaw",          "Slytherin"        ],        "options": {          "is_scim": false,          "is_protected": false        },        "is_hidden": false,        "section_id": "456DEF"      }    ],    "sections": [      {        "id": "123ABC",        "team_id": "T123456",        "section_type": "contact",        "label": "Contact Information",        "order": 1,        "is_hidden": true      },      {        "id": "456DEF",        "team_id": "T123456",        "section_type": "custom",        "label": "About Me",        "order": 2,        "is_hidden": true      }    ]  }}
```

#### Typical error response

```
{  "ok": false,  "error": "invalid_auth"}
```

The response contains a `profile` item with an array of key value pairs. Right now only the `fields` key is supported, which contains a list of field definitions for this team.

There are two ways to update a field. The value of `is_scim` determines which method to use:

*   If `is_scim` is `True`, update the field via a SCIM API call
*   If `is_scim` is `False`, update the field via [`users.profile.set`](/reference/methods/users.profile.set). You'll need its `id` to do so. Field | Type | Description --------------|--------------|------------ `id` | String | The ID of either the section or field `is_protected` | Boolean | `is_scim` | Boolean | If true, can be updated via SCIM APIs `field_name`| String | The name of the field `hint` | String | Any additional context the user may need to understand the field `label` | String | The text that will appear under the field or section `possible_values` | String | The values that allowed to be chosen by the user `options` | String| An object containing the `is_protected` and `is_scim` fields `ordering` | Integer | The placement of the field or section on the profile `section_id`| String | The `id` of the section the field is in `section_type` | String | The type of content in the section. Users can only create `custom` section types `type` | String | The format the field supports. Can be `date`, `link`, `long_text`, `options_list`, `tags`, `text`, or `user`

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
