Source: https://docs.slack.dev/reference/methods/users.profile.get

# users.profile.get method

DocsCall generator

## Facts {#facts}

**Description**Retrieve a user's profile information, including their custom status.

**Method Access**

*   HTTP
*   Slack CLI
*   JavaScript
*   Python
*   Java

```
GET https://slack.com/api/users.profile.get
```

[![slack-cli](/img/logos/slack-cli.png)](/tools/slack-cli)

```
slack api users.profile.get
```

[![bolt-js](/img/logos/bolt-js-logo.svg)](/tools/bolt-js)

```
app.client.users.profile.get
```

[![bolt-py](/img/logos/bolt-py-logo.svg)](/tools/bolt-python)

```
app.client.users_profile_get
```

[![bolt-java](/img/logos/bolt-java-logo.svg)](/tools/java-slack-sdk/guides/getting-started-with-bolt)

```
app.client().usersProfileGet
```

**Scopes**

Bot token:

[`users.profile:read`](/reference/scopes/users.profile.read)

User token:

[`users.profile:read`](/reference/scopes/users.profile.read)

**Content types**

`application/x-www-form-urlencoded`

`application/json`

**Rate Limits**[Tier 4: 100+ per minute](/apis/web-api/rate-limits)

## Arguments {#arguments}

### Required arguments

**`token`**`string`Required

Authentication token bearing required scopes. Tokens should be passed as an HTTP Authorization header or alternatively, as a POST parameter.

_Example:_ `xxxx-xxxxxxxxx-xxxx`

### Optional arguments

**`include_labels`**`boolean`Optional

Include labels for each ID in custom profile fields. Using this parameter will heavily rate-limit your requests and is not recommended.

_Default:_ `false`

_Example:_ `true`

**`user`**`string`Optional

User to retrieve profile info for

## Usage info {#usage-info}

Use this method to retrieve a user's profile information.

If you're frequently calling `users.profile.get` on behalf of a team or user, we recommend caching labels retrieved from [`team.profile.get`](/reference/methods/team.profile.get). Using the `include_labels` parameter will severely rate-limit requests to this method.

* * *

## Response {#response}

#### Typical success response

```
{  "ok": true,  "profile": {    "title": "Head of Coffee Production",    "phone": "",    "skype": "",    "real_name": "John Smith",    "real_name_normalized": "John Smith",    "display_name": "john",    "display_name_normalized": "john",    "fields": {      "Xf0111111": {        "value": "Barista",        "alt": ""      },      "Xf0222222": {        "value": "2022-04-11",        "alt": ""      },      "Xf0333333": {        "value": "https://example.com",        "alt": ""      }    },    "status_text": "Watching cold brew steep",    "status_emoji": ":coffee:",    "status_emoji_display_info": [],    "status_expiration": 0,    "avatar_hash": "123xyz",    "start_date": "2022-03-21",    "email": "johnsmith@example.com",    "pronouns": "they/them/theirs",    "huddle_state": "default_unset",    "huddle_state_expiration_ts": 0,    "first_name": "john",    "last_name": "smith",    "image_24": "https://.../...-24.png",    "image_32": "https://.../...-32.png",    "image_48": "https://.../...-48.png",    "image_72": "https://.../...-72.png",    "image_192": "https://.../....-192png",    "image_512": "https://.../...-512.png"  }}
```

#### Typical error response

```
{  "ok": false,  "error": "user_not_found"}
```

The following fields are the default fields of a user's workspace profile. A user may have additional custom fields, though! Use [`team.profile.get`](/reference/methods/team.profile.get) to view all the workspace's custom profile fields.

Field

Type

Description

`avatar_hash`

String

`display_name`

String

The display name the user has chosen to identify themselves by in their workspace profile. Do not use this field as a unique identifier for a user, as it may change at any time. Instead, use `id` and `team_id` in concert.

`display_name_normalized`

String

The `display_name` field, but with any non-Latin characters filtered out.

`email`

String

A valid email address. It cannot have spaces, and it must have an `@` and a domain. It cannot be in use by another member of the same team. Changing a user's email address will send an email to both the old and new addresses, and also post a slackbot message to the user informing them of the change. This field can only be changed _by admins_ for users on **paid** teams. When using an OAuth Access Token (that starts with `xoxp-`) to retrieve one's own profile details, the `email` field will not be returned in the response if the token does not have the `users:read.email` scope.

`fields`

Object

All the [custom profile fields](/reference/methods/users.profile.set#custom_profile) for the user.

`first_name`

String

The user's first name. The name `slackbot` cannot be used. Updating `first_name` will update the first name within `real_name`.

`image_*`

String

These various fields will contain `https` URLs that point to square ratio, web-viewable images (GIFs, JPEGs, or PNGs) that represent different sizes of a user's profile picture.

`last_name`

String

The user's last name. The name `slackbot` cannot be used. Updating `last_name` will update the second name within `real_name`.

`phone`

String

The user's phone number, in any format.

`pronouns`

String

The pronouns the user prefers to be addressed by.

`real_name`

String

The user's first and last name. Updating this field will update `first_name` and `last_name`. If only one name is provided, the value of `last_name` will be cleared.

`real_name_normalized`

String

The `real_name` field, but with any non-Latin characters filtered out.

`skype`

String

A shadow from a bygone era. It will always be an empty string and cannot be set otherwise.

`start_date`

String

The date the person joined the organization. Only available if [Slack Atlas](https://slack.com/atlas) is enabled.

`status_emoji`

String

The displayed emoji that is enabled for the Slack team, such as `:train:`.

`status_expiration`

Integer

the Unix timestamp of when the status will expire. Providing `0` or omitting this field results in a custom status that will not expire.

`status_text`

String

The displayed text of up to 100 characters. We strongly encourage brevity. See [custom status](/apis/web-api/user-presence-and-status#custom_status) for more info.

`team`

String

The ID of the workspace the user is in.

`title`

String

The user's title.

Bot users may contain an `always_active` profile field, indicating whether the bot user is active in a way that [overrides traditional presence rules](/apis/web-api/user-presence-and-status#bot_presence).

### Email addresses {#email-addresses}

Accessing Email Addresses

The [`users:read.email`](/reference/scopes/users.read.email) OAuth scope is now required to access the `email` field in user objects returned by the [`users.list`](/reference/methods/users.list) and [`users.info`](/reference/methods/users.info) web API methods. [`users:read`](/reference/scopes/users.read) is no longer a sufficient scope for this data field. [Learn more](/changelog/2017-04-narrowing-email-access).

If the _Email Display_ setting is unchecked in your Slack Admin settings, `users.profile.get` does not return an `email` value.

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

You are attempting to call this method too frequently.

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

The token being used is not valid.

`token_revoked`

Authentication token is for a deleted user or workspace or the app has been removed when using a `user` token.

`two_factor_setup_required`

Two factor setup is required.

`user_not_found`

Value passed for `user` was invalid.
