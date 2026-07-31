Source: https://docs.slack.dev/reference/scopes/files.read

# files:read scope

View files shared in channels and conversations that your Slack app has been added to

## Facts

**Supported token types**

[`Bot`](/authentication/tokens#bot)

[`User`](/authentication/tokens#user)

[`Legacy Bot`](/authentication/tokens#legacy-bot)

**Compatible API methods**

[`files.info`](/reference/methods/files.info)

[`files.list`](/reference/methods/files.list)

**Compatible events**

[`file_change`](/reference/events/file_change)

[`file_comment_added`](/reference/events/file_comment_added)

[`file_comment_deleted`](/reference/events/file_comment_deleted)

[`file_comment_edited`](/reference/events/file_comment_edited)

[`file_created`](/reference/events/file_created)

[`file_deleted`](/reference/events/file_deleted)

[`file_public`](/reference/events/file_public)

[`file_shared`](/reference/events/file_shared)

[`file_unshared`](/reference/events/file_unshared)

## Usage info {#usage-info}

This scope allows an app to retrieve both information about files uploaded to workspaces as well as _download_ those files.
