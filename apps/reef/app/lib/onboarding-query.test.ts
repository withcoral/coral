import { tableFromArrays, tableToIPC } from 'apache-arrow'
import { describe, expect, it } from 'vitest'

import { decodeOnboardingSampleQueryRows } from './onboarding-query'

describe('decodeOnboardingSampleQueryRows', () => {
  it('decodes the source counts returned by the onboarding query', () => {
    const arrowIpcStream = tableToIPC(
      tableFromArrays({
        source: ['github', 'slack'],
        tables: BigInt64Array.from([12n, 2n]),
      }),
    )

    expect(decodeOnboardingSampleQueryRows(arrowIpcStream)).toEqual([
      { source: 'github', tables: 12n },
      { source: 'slack', tables: 2n },
    ])
  })

  it('rejects an unexpected result shape', () => {
    const arrowIpcStream = tableToIPC(tableFromArrays({ schema_name: ['github'] }))

    expect(() => decodeOnboardingSampleQueryRows(arrowIpcStream)).toThrow(
      'The sample query returned an unexpected result shape.',
    )
  })
})
