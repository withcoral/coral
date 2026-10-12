import { describe, expect, it } from 'vitest'

import { decodeSelectedOperations } from './selection'

const a = { provider: 'prov-a', operation: 'providers/prov-a/operations/one' }
const b = { provider: 'prov-b', operation: 'providers/prov-b/operations/two' }

describe('decodeSelectedOperations', () => {
  it('reads every selected operation in order', () => {
    const data = new FormData()
    data.append('selectedOperation', JSON.stringify(a))
    data.append('selectedOperation', JSON.stringify(b))
    expect(decodeSelectedOperations(data)).toEqual([a, b])
  })

  // Catches: a malformed entry silently dropped, so the action updates fewer
  // operations than the reader selected without saying so.
  it('rejects the whole submission when one entry is not a provider and operation', () => {
    for (const bad of [
      '{not json',
      JSON.stringify({ provider: 'x' }),
      JSON.stringify(['x', 'y']),
    ]) {
      const data = new FormData()
      data.append('selectedOperation', JSON.stringify(a))
      data.append('selectedOperation', bad)
      expect(decodeSelectedOperations(data)).toBeNull()
    }
  })
})
