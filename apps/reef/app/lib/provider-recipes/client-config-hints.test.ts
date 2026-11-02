import { describe, expect, it } from 'vitest'
import { parse } from 'yaml'

import { CLIENT_CONFIG_HINT_GENERATORS } from './client-config-hints'

describe('datadog_mcp_redirect_allowlist', () => {
  const callbackUrl = 'https://coral.example.com/provider-setup/oauth/callback'
  // An unallowed callback fails sign-in with "Mismatching redirect URI", so the
  // link must open the settings of the site the person signs in at.
  it.each([
    [
      'https://app.datadoghq.eu/oauth2/v1/authorize',
      '[MCP settings](https://app.datadoghq.eu/organization-settings/mcp)',
    ],
    [
      'https://us3.datadoghq.com/oauth2/v1/authorize',
      '[MCP settings](https://us3.datadoghq.com/organization-settings/mcp)',
    ],
    // Without a site, the link opens the guide and says so.
    [undefined, '[Datadog MCP setup guide](https://docs.datadoghq.com/mcp_server/setup/)'],
    ['', '[Datadog MCP setup guide](https://docs.datadoghq.com/mcp_server/setup/)'],
  ])('links sign-in at %s to %s', (signInUrl, link) => {
    const hint = CLIENT_CONFIG_HINT_GENERATORS.datadog_mcp_redirect_allowlist({
      callbackUrl,
      signInUrl,
    })
    expect(hint).toContain(link)
  })
})

describe('slack_app_create_link', () => {
  it('links to a Slack app with exactly this callback URL', () => {
    const callbackUrl = 'https://coral.example.com/provider-setup/oauth/callback'
    const hint = CLIENT_CONFIG_HINT_GENERATORS.slack_app_create_link({ callbackUrl })
    const [, url] = /^\[Create a Slack app\]\((\S+)\), then copy/.exec(hint) ?? []
    expect(url).toMatch(/^https:\/\/api\.slack\.com\/apps\?new_app=1&manifest_yaml=/)
    const sent = parse(new URL(url).searchParams.get('manifest_yaml') ?? '')
    expect(sent.oauth_config.redirect_urls).toEqual([callbackUrl])
    expect(sent.settings.is_mcp_enabled).toBe(true)
  })
})
