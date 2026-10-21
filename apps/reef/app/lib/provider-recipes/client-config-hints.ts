// The helpers a recipe's client_config_hint_generator names. Each writes the
// Markdown hint below the client ID field from facts only this instance knows.

import type { ClientConfigHintGenerator } from './recipe-format'
import { slackAppCreateLink } from './slack-app-manifest'

export interface ClientConfigHintContext {
  /** Where the OAuth server sends the person back after sign-in. */
  callbackUrl: string
}

export const CLIENT_CONFIG_HINT_GENERATORS: Record<
  ClientConfigHintGenerator,
  (context: ClientConfigHintContext) => string
> = {
  slack_app_create_link: ({ callbackUrl }) =>
    `[Create a Slack app](${slackAppCreateLink(callbackUrl)}), then copy its client ID and client secret here.`,
}
