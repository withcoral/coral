import { describe, expect, it } from 'vitest'

import { redactSourceUrl } from './source-url'

const shownUrl = (value: string) => redactSourceUrl(value)?.url

describe('redactSourceUrl', () => {
  // Catches: settings showing the password or token a source URL embeds.
  it('removes embedded credentials', () => {
    expect(shownUrl('https://reader:s3cret@api.example.com/openapi.json')).toBe(
      'https://api.example.com/openapi.json',
    )
  })

  // Catches: an API key passed as a query parameter reaching the page, or
  // ordinary parameters hidden for no reason.
  it('hides only the values of parameters that look secret', () => {
    const shown = shownUrl(
      'https://api.example.com/spec?format=json&api_key=abc123&version=2&X-Amz-Signature=f00#frag',
    )
    expect(shown).toBe(
      'https://api.example.com/spec?format=json&api_key=[redacted]&version=2&X-Amz-Signature=[redacted]',
    )
    expect(shown).not.toContain('abc123')
  })

  it('treats every credential-like parameter name as secret', () => {
    for (const name of [
      'token',
      'access_token',
      'private_token',
      'apikey',
      'key',
      'secret',
      'client_secret',
      'password',
      'auth',
      'authorization',
      'sig',
      'signature',
      'session',
      'code',
      'credential',
      'jwt',
    ])
      expect(shownUrl(`https://a.example/?${name}=hidden`), name).toBe(
        `https://a.example/?${name}=[redacted]`,
      )
  })

  it('shows an ordinary URL unchanged', () => {
    expect(shownUrl('https://mcp.example.com/mcp')).toBe('https://mcp.example.com/mcp')
  })

  // Catches: the page parsing the URL again, or showing the credentials in its host.
  it('gives the host alongside the URL', () => {
    expect(redactSourceUrl('https://reader:secret@api.example.com:8443/spec')).toEqual({
      url: 'https://api.example.com:8443/spec',
      host: 'api.example.com:8443',
    })
  })

  // Catches: an unparseable value shown verbatim, which could hold anything.
  it('shows nothing for a value that is not a URL', () => {
    expect(redactSourceUrl('not a url token=abc')).toBeNull()
  })
})
