import { tableFromArrays, tableToIPC } from 'apache-arrow'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const { executeSql, queryClientForRequest } = vi.hoisted(() => {
  const executeSqlMock = vi.fn()
  return {
    executeSql: executeSqlMock,
    queryClientForRequest: vi.fn(() => ({ executeSql: executeSqlMock })),
  }
})

vi.mock('@/lib/coral-request.server', () => ({
  queryClientForRequest,
}))

import { ONBOARDING_SAMPLE_QUERY } from './onboarding-query'
import {
  decodeOnboardingSampleQueryRows,
  loadOnboardingSampleQuery,
} from './onboarding-query.server'

beforeEach(() => {
  executeSql.mockReset()
  queryClientForRequest.mockClear()
})

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

describe('loadOnboardingSampleQuery', () => {
  it('executes the fixed sample query through the request-scoped server client', async () => {
    executeSql.mockResolvedValue({
      arrowIpcStream: tableToIPC(
        tableFromArrays({ source: ['github'], tables: BigInt64Array.from([3n]) }),
      ),
    })

    await expect(
      loadOnboardingSampleQuery(
        new Request('http://reef.test/onboarding?step=query'),
        'coral-access-token',
        'analytics',
      ),
    ).resolves.toEqual({
      rows: [{ source: 'github', tables: '3' }],
      status: 'success',
    })
    expect(executeSql).toHaveBeenCalledOnce()
    expect(queryClientForRequest).toHaveBeenCalledWith(expect.any(Request), 'coral-access-token')
    expect(executeSql.mock.calls[0]?.[0]).toMatchObject({
      sql: ONBOARDING_SAMPLE_QUERY,
      workspace: { name: 'analytics' },
    })
  })

  it('returns query failures as renderable loader data', async () => {
    executeSql.mockResolvedValue({
      arrowIpcStream: tableToIPC(tableFromArrays({ unexpected: ['value'] })),
    })

    await expect(
      loadOnboardingSampleQuery(
        new Request('http://reef.test/onboarding?step=query'),
        'coral-access-token',
        'analytics',
      ),
    ).resolves.toEqual({
      message: 'The sample query returned an unexpected result shape.',
      status: 'error',
    })
  })
})
