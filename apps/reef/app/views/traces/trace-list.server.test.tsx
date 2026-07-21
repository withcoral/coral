import { renderToString } from 'react-dom/server'
import { MemoryRouter } from 'react-router'
import { TraceOperationKind, TraceStatus } from '@/generated/coral/v1/traces_pb'
import { describe, expect, it, vi } from 'vitest'

import { TraceList } from './trace-list'
import type { TraceSummaryData } from './trace-utils'

const trace: TraceSummaryData = {
  durationNanos: '1000000',
  endTimeUnixNanos: '1700000001000000000',
  name: 'coral.query',
  operationKind: TraceOperationKind.UNSPECIFIED,
  operationName: '',
  query: 'select 1',
  rootSpanId: 'root',
  rowCount: '1',
  rowCountRecorded: true,
  spanCount: 1,
  startTimeUnixNanos: '1700000000000000000',
  status: TraceStatus.OK,
  traceId: 'trace-a',
}

function renderList() {
  return renderToString(
    <MemoryRouter initialEntries={['/workspaces/analytics/traces']}>
      <TraceList referenceTimeMs={1_700_000_030_000} traces={[trace]} workspaceId="analytics" />
    </MemoryRouter>,
  )
}

describe('TraceList server rendering', () => {
  it('renders stable relative time from loader data regardless of ambient time', () => {
    vi.spyOn(Date, 'now').mockReturnValueOnce(1_700_000_031_000)
    const serverHtml = renderList()
    vi.spyOn(Date, 'now').mockReturnValueOnce(1_700_000_090_000)
    const hydrationHtml = renderList()

    expect(serverHtml).toContain('30s ago')
    expect(hydrationHtml).toBe(serverHtml)
  })
})
