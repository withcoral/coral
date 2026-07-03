import { describe, expect, it } from 'vitest'

import { loader } from './root'

describe('root loader', () => {
  it('returns sidebar state', async () => {
    await expect(runLoader('http://localhost:5173/')).resolves.toEqual({
      sidebarIsMinimized: false,
    })
  })

  it('reads the collapsed sidebar cookie', async () => {
    await expect(
      runLoader('http://localhost:5173/', 'reef_sidebar_collapsed=true'),
    ).resolves.toEqual({
      sidebarIsMinimized: true,
    })
  })
})

async function runLoader(url: string, cookie?: string) {
  return loader({
    params: {},
    request: new Request(url, cookie ? { headers: { cookie } } : undefined),
  } as never)
}
