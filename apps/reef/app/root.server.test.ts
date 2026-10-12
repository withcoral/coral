import { RouterContextProvider } from 'react-router'
import { describe, expect, it, vi } from 'vitest'

import { bootstrapRuntimeConfig, runtimeConfigContext } from '@/lib/runtime-config.server'

import { middleware } from './root'

describe('root route', () => {
  // Public routes sit outside the protected boundary and still need this configuration.
  it('gives every route the bootstrap configuration', async () => {
    const url = new URL('http://localhost:5173/login')
    const context = new RouterContextProvider()
    const next = vi.fn(async () => new Response())

    for (const run of middleware) {
      await run(
        { context, params: {}, pattern: url.pathname, request: new Request(url), url },
        next,
      )
    }

    expect(context.get(runtimeConfigContext)).toBe(bootstrapRuntimeConfig())
    expect(next).toHaveBeenCalled()
  })
})
