import { describe, expect, it } from 'vitest'
import { parse } from 'yaml'

import { CLIENT_CONFIG_HINT_GENERATORS } from './client-config-hints'

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
