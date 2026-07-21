import { createMemoryRouter, RouterProvider } from 'react-router'
import { TraceOperationKind, TraceStatus } from '@/generated/coral/v1/traces_pb'
import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-react'

import type { TraceSummaryData } from './trace-utils'
import { TraceList } from './trace-list'

function summary(traceId: string): TraceSummaryData {
  return {
    durationNanos: '1000000',
    endTimeUnixNanos: '2000000',
    name: 'coral.query',
    operationKind: TraceOperationKind.UNSPECIFIED,
    operationName: '',
    query: `select '${traceId}'`,
    rootSpanId: `root-${traceId}`,
    rowCount: '1',
    rowCountRecorded: true,
    spanCount: 1,
    startTimeUnixNanos: '1000000',
    status: TraceStatus.OK,
    traceId,
  }
}

describe('TraceList', () => {
  it('encodes trace IDs exactly once and preserves the current search string', async () => {
    const router = createMemoryRouter(
      [
        {
          element: (
            <TraceList
              referenceTimeMs={2_000_000}
              traces={[summary('trace/with?reserved')]}
              workspaceId="analytics"
            />
          ),
          path: '/workspaces/:workspaceId/traces',
        },
      ],
      { initialEntries: ['/workspaces/analytics/traces?pro'] },
    )

    const screen = await render(<RouterProvider router={router} />)

    await expect
      .element(screen.getByRole('link', { name: /select 'trace\/with\?reserved'/i }))
      .toHaveAttribute('href', '/workspaces/analytics/traces/trace%2Fwith%3Freserved?pro')
  })

  it('marks the URL-selected row current independently from keyboard highlighting', async () => {
    const router = createMemoryRouter(
      [
        {
          children: [{ element: null, path: ':traceId' }],
          element: (
            <TraceList
              activeTraceId="keyboard-active"
              referenceTimeMs={2_000_000}
              traces={[summary('selected'), summary('keyboard-active')]}
              workspaceId="analytics"
            />
          ),
          path: '/workspaces/:workspaceId/traces',
        },
      ],
      { initialEntries: ['/workspaces/analytics/traces/selected'] },
    )

    const screen = await render(<RouterProvider router={router} />)

    await expect
      .element(screen.getByRole('link', { name: /select 'selected'/i }))
      .toHaveAttribute('aria-current', 'page')
    await expect
      .element(screen.getByRole('link', { name: /select 'keyboard-active'/i }))
      .toHaveAttribute('data-active', 'true')
  })
})
