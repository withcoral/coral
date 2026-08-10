import { describe, expect, it } from 'vitest'

import { loader } from './mcp-install'

describe('MCP installer resource route', () => {
  it('serves a shell script for an allowlisted client', async () => {
    const response = await loader({
      params: { clientId: 'codex' },
      request: new Request('http://reef.test/mcp/install/codex'),
    } as Parameters<typeof loader>[0])

    expect(response.status).toBe(200)
    expect(response.headers.get('content-type')).toContain('text/x-shellscript')
    expect(response.headers.get('content-disposition')).toContain('coral-mcp-codex.sh')
    await expect(response.text()).resolves.toContain('client="codex"')
  })

  it('does not generate a script for unknown clients', async () => {
    const response = await loader({
      params: { clientId: 'not-a-client' },
      request: new Request('http://reef.test/mcp/install/not-a-client'),
    } as Parameters<typeof loader>[0])

    expect(response.status).toBe(404)
  })
})
