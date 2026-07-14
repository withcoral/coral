import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getMcpLaunchConfig } from './mcp-config'
import { externalCoralPath } from './sidecar'

vi.mock('./sidecar', () => ({
  externalCoralPath: vi.fn(),
}))

beforeEach(() => {
  vi.resetAllMocks()
})

describe('getMcpLaunchConfig', () => {
  it('uses the executable bundled with Coral Desktop', async () => {
    vi.mocked(externalCoralPath).mockResolvedValue(
      '/Applications/Coral.app/Contents/Resources/coral/coral',
    )

    await expect(getMcpLaunchConfig()).resolves.toEqual({
      args: ['mcp-stdio'],
      command: '/Applications/Coral.app/Contents/Resources/coral/coral',
    })
  })
})
