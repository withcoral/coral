import { renderToString } from 'react-dom/server'
import { MemoryRouter } from 'react-router'
import {
  TraceInvocationKind,
  TraceOperationKind,
  TraceStatus,
} from '@/generated/coral/v1/traces_pb'
import { describe, expect, it, vi } from 'vitest'

import { TraceList } from './trace-list'
import type { TraceSummaryData } from './trace-utils'

const trace: TraceSummaryData = {
  durationNanos: '1000000',
  endTimeUnixNanos: '1700000001000000000',
  name: 'coral.query',
  invocationKind: TraceInvocationKind.DIRECT,
  operationKind: TraceOperationKind.QUERY,
  operationName: 'sql',
  query: 'select 1',
  rootSpanId: 'root',
  rowCount: '1',
  rowCountRecorded: true,
  spanCount: 1,
  startTimeUnixNanos: '1700000000000000000',
  status: TraceStatus.OK,
  traceId: 'trace-a',
}

const searchTrace: TraceSummaryData = {
  ...trace,
  name: 'coral.mcp.call_tool',
  invocationKind: TraceInvocationKind.MCP,
  operationKind: TraceOperationKind.SEARCH,
  operationName: 'search',
  query: 'SELECT customers FROM Zurich',
  rowCount: '0',
  rowCountRecorded: false,
  traceId: 'search-trace',
}

function renderList(traces = [trace]) {
  return renderToString(
    <MemoryRouter initialEntries={['/workspaces/analytics/traces']}>
      <TraceList referenceTimeMs={1_700_000_030_000} traces={traces} workspaceId="analytics" />
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

  it('renders locally recorded text for MCP Search traces', () => {
    const html = renderList([searchTrace])
    const renderedText = html.replace(/<[^>]+>/g, '')

    expect(renderedText).toContain('Search · SELECT customers FROM Zurich')
    expect(html).not.toContain('coral.mcp.call_tool')
    expect(html).not.toContain('token keyword')
  })

  it('renders locally recorded text for direct Search traces', () => {
    const html = renderList([
      {
        ...searchTrace,
        name: 'coral.search',
        invocationKind: TraceInvocationKind.DIRECT,
        operationKind: TraceOperationKind.SEARCH,
        operationName: 'search',
        query: 'DIRECT SEARCH TEXT',
      },
    ])

    expect(html.replace(/<[^>]+>/g, '')).toContain('Search · DIRECT SEARCH TEXT')
  })

  it('keeps a Search-only fallback for typed traces captured before text retention', () => {
    const html = renderList([{ ...searchTrace, query: '' }])
    const renderedText = html.replace(/<[^>]+>/g, '')

    expect(renderedText).toContain('Search')
    expect(renderedText).not.toContain('Search ·')
  })

  it('keeps a Search-only fallback for pre-operation-metadata traces', () => {
    const html = renderList([
      {
        ...searchTrace,
        name: 'coral.search',
        invocationKind: TraceInvocationKind.UNSPECIFIED,
        operationKind: TraceOperationKind.UNSPECIFIED,
        operationName: '',
        query: '',
      },
    ])
    const renderedText = html.replace(/<[^>]+>/g, '')

    expect(renderedText).toContain('Search')
    expect(renderedText).not.toContain('coral.search')
  })

  it('renders SQL for Query operations named search', () => {
    const html = renderList([
      {
        ...trace,
        operationKind: TraceOperationKind.QUERY,
        operationName: 'search',
        query: 'SELECT * FROM searchable_items',
      },
    ])

    expect(html).toContain('SELECT')
    expect(html).toContain('searchable_items')
    expect(html).toContain('token keyword')
  })

  it('renders future tool names generically alongside nested SQL', () => {
    const html = renderList([
      {
        ...trace,
        name: 'coral.mcp.call_tool',
        invocationKind: TraceInvocationKind.MCP,
        operationKind: TraceOperationKind.TOOL,
        operationName: 'future_lookup',
        query: 'SELECT * FROM catalog.items',
      },
    ])

    const renderedText = html.replace(/<[^>]+>/g, '')
    expect(renderedText).toContain('Future lookup · SELECT * FROM catalog.items')
    expect(html).not.toContain('coral.mcp.call_tool')
  })
})
