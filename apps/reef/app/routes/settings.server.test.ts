import { describe, expect, it } from 'vitest'

import { loader } from './settings-loader'

describe('settings loader', () => {
  it('loads install commands from the request origin', () => {
    expect(
      loader({ request: new Request('https://reef.example/settings') } as Parameters<
        typeof loader
      >[0]),
    ).toEqual({
      runtime: 'web',
      mcpClients: expect.arrayContaining([
        expect.objectContaining({
          id: 'codex',
          installCommand: 'curl -fsSL https://reef.example/mcp/install/codex | sh',
        }),
      ]),
    })
  })
})
