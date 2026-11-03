// The helpers a recipe's client_config_hint_generator names. Each writes the
// Markdown hint below the client ID field from facts only this instance knows.

import type { ClientConfigHintGenerator } from './recipe-format'
import { slackAppCreateLink } from './slack-app-manifest'

export interface ClientConfigHintContext {
  /** Where the OAuth server sends the person back after sign-in. */
  callbackUrl: string
  /** The authorization endpoint Coral discovered; empty before discovery. */
  signInUrl?: string
}

/**
 * Datadog rejects a callback URL that its organization has not allowed, with
 * "Mismatching redirect URI". The MCP settings page sits on the host the person
 * signs in at, so the link follows their site.
 */
function datadogMcpSettingsLink(signInUrl: string | undefined) {
  try {
    return `[MCP settings](${new URL('/organization-settings/mcp', signInUrl).href})`
  } catch {
    return '[Datadog MCP setup guide](https://docs.datadoghq.com/mcp_server/setup/)'
  }
}

export const CLIENT_CONFIG_HINT_GENERATORS: Record<
  ClientConfigHintGenerator,
  (context: ClientConfigHintContext) => string
> = {
  slack_app_create_link: ({ callbackUrl }) =>
    `[Create a Slack app](${slackAppCreateLink(callbackUrl)}), then copy its client ID and client secret here. For help, read the [Slack setup guide](https://withcoral.com/docs/guides/providers/slack).`,
  datadog_mcp_redirect_allowlist: ({ signInUrl }) =>
    `Before you sign in, ask a Datadog admin to add the callback URL in ${datadogMcpSettingsLink(signInUrl)}.`,
}
