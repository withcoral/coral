import { describe, expect, it } from 'vitest'

import { decodeSelectedTools } from './selection'

const a = { provider: 'prov-a', tool: 'providers/prov-a/tools/one' }
const b = { provider: 'prov-b', tool: 'providers/prov-b/tools/two' }

describe('decodeSelectedTools', () => {
  it('reads every selected tool in order', () => {
    const data = new FormData()
    data.append('selectedTool', JSON.stringify(a))
    data.append('selectedTool', JSON.stringify(b))
    expect(decodeSelectedTools(data)).toEqual([a, b])
  })

  // Catches: a malformed entry silently dropped, so the action updates fewer
  // tools than the reader selected without saying so.
  it('rejects the whole submission when one entry is not a provider and tool', () => {
    for (const bad of [
      '{not json',
      JSON.stringify({ provider: 'x' }),
      JSON.stringify(['x', 'y']),
    ]) {
      const data = new FormData()
      data.append('selectedTool', JSON.stringify(a))
      data.append('selectedTool', bad)
      expect(decodeSelectedTools(data)).toBeNull()
    }
  })
})
