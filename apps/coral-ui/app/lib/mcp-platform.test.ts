import { describe, expect, it } from 'vitest'

import { isWindowsRequest } from './mcp-platform'

function requestWithHeaders(headers: HeadersInit): Request {
  // Browsers strip protected headers from constructed Request instances; the
  // loader receives them from the actual HTTP request, so model that boundary.
  return { headers: new Headers(headers) } as Request
}

describe('isWindowsRequest', () => {
  it('prefers the browser platform client hint', () => {
    expect(
      isWindowsRequest(
        requestWithHeaders({
          'sec-ch-ua-platform': '"Windows"',
          'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0)',
        }),
      ),
    ).toBe(true)
    expect(
      isWindowsRequest(
        requestWithHeaders({
          'sec-ch-ua-platform': '"macOS"',
          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        }),
      ),
    ).toBe(false)
  })

  it('falls back to the user agent when client hints are unavailable', () => {
    expect(
      isWindowsRequest(
        requestWithHeaders({ 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }),
      ),
    ).toBe(true)
  })

  it('does not select PowerShell for other platforms', () => {
    expect(isWindowsRequest(requestWithHeaders({ 'sec-ch-ua-platform': '"macOS"' }))).toBe(false)
  })
})
