// The Slack app Coral asks you to create for Slack's MCP server, and the link
// that opens Slack's app creation page with it filled in. Only this instance
// knows its callback URL, so the link adds it as the one redirect URL.
// https://docs.slack.dev/app-manifests/configuring-apps-with-app-manifests#sharing
// Fields: https://docs.slack.dev/reference/app-manifest

import { stringify } from 'yaml'

const SLACK_NEW_APP_URL = 'https://api.slack.com/apps?new_app=1'

const SLACK_APP_MANIFEST = {
  display_information: {
    name: 'Coral',
    description: 'Lets Coral read and act in Slack for you.',
  },
  settings: { is_mcp_enabled: true },
  oauth_config: {
    scopes: {
      // Slack's MCP server acts with the signed-in person's user token, so the
      // app has no bot user and no bot scopes. The scopes copy Slack's per-tool
      // table: https://docs.slack.dev/ai/slack-mcp-server#oauth-scopes
      user: [
        // Search messages and channels
        'search:read.public',
        'search:read.private',
        'search:read.mpim',
        'search:read.im',
        // Search files
        'search:read.files',
        // Search users
        'search:read.users',
        // Read and upload files
        'files:read',
        'files:write',
        // Search emoji
        'emoji:read',
        // Send and schedule messages
        'chat:write',
        // Read a channel or thread
        'channels:history',
        'groups:history',
        'mpim:history',
        'im:history',
        // Create conversations and channels
        'channels:write',
        'groups:write',
        'im:write',
        'mpim:write',
        // List channel members and a person's channels
        'channels:read',
        'groups:read',
        'im:read',
        'mpim:read',
        // Reactions
        'reactions:read',
        'reactions:write',
        // Canvases
        'canvases:read',
        'canvases:write',
        // User profiles and email addresses
        'users:read',
        'users:read.email',
        // Lists
        'lists:read',
        'lists:write',
      ],
    },
  },
}

/** The app creation link, with `callbackUrl` as the app's only redirect URL. */
export function slackAppCreateLink(callbackUrl: string): string {
  const manifest = {
    ...SLACK_APP_MANIFEST,
    oauth_config: { ...SLACK_APP_MANIFEST.oauth_config, redirect_urls: [callbackUrl] },
  }
  const url = new URL(SLACK_NEW_APP_URL)
  url.searchParams.set('manifest_yaml', stringify(manifest))
  return url.toString()
}
