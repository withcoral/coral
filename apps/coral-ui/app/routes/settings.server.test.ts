import { describe, expect, it } from 'vitest'

import { loader } from './settings-loader'

describe('settings loader', () => {
  it('keeps the web route outside the Desktop-only view', () => {
    expect(loader({} as Parameters<typeof loader>[0])).toEqual({ runtime: 'web' })
  })
})
