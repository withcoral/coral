import { create } from '@bufbuild/protobuf'

import { WorkspaceSchema } from '@/generated/coral/v1/resources_pb'
import {
  TraceInvocationKind,
  TraceOperationKind,
  TraceStatus,
  TraceView,
} from '@/generated/coral/v1/traces_pb'
import type { TraceSummaryData } from '@/views/traces/trace-utils'
import { describe, expect, it, vi } from 'vitest'

import { listQueryTraces, loadTracesRouteData, traceEndpointLabel } from './traces-loader'

function summary(
  traceId: string,
  query = `select '${traceId}'`,
  name = query ? 'coral.query' : 'http.request',
  operationKind = TraceOperationKind.QUERY,
  operationName = 'sql',
  invocationKind = TraceInvocationKind.DIRECT,
): TraceSummaryData {
  return {
    durationNanos: '1000000',
    endTimeUnixNanos: '2000000',
    name,
    invocationKind,
    operationKind,
    operationName,
    query,
    rootSpanId: `root-${traceId}`,
    rowCount: '1',
    rowCountRecorded: true,
    spanCount: 1,
    startTimeUnixNanos: '1000000',
    status: TraceStatus.OK,
    traceId,
  }
}

function legacySummary(
  traceId: string,
  query = `select '${traceId}'`,
  name = query ? 'coral.query' : 'http.request',
): TraceSummaryData {
  return summary(
    traceId,
    query,
    name,
    TraceOperationKind.UNSPECIFIED,
    '',
    TraceInvocationKind.UNSPECIFIED,
  )
}

describe('traces list loader', () => {
  const request = new Request('http://reef.test/workspaces/analytics/traces')
  const workspace = create(WorkspaceSchema, { name: 'analytics' })

  it('requests the typed Query Stream view and keeps future operation kinds', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(123_456)
    const search = summary(
      'search',
      '',
      'coral.mcp.call_tool',
      TraceOperationKind.SEARCH,
      'search',
      TraceInvocationKind.MCP,
    )
    const futureTool = summary(
      'future',
      '',
      'coral.mcp.call_tool',
      TraceOperationKind.OTHER,
      'future_lookup',
      TraceInvocationKind.MCP,
    )
    const listPage = vi.fn().mockResolvedValue({
      nextPageToken: '',
      traces: [
        summary('query'),
        search,
        futureTool,
        legacySummary('discovery', '', 'coral.mcp.list_tools'),
      ],
    })

    await expect(loadTracesRouteData(request, workspace, listPage)).resolves.toEqual({
      endpointLabel: 'reef.test',
      loadError: null,
      referenceTimeMs: 123_456,
      traces: [summary('query'), search, futureTool],
    })
    expect(listPage).toHaveBeenCalledWith(request, workspace, 100, '', TraceView.QUERY_STREAM)
  })

  it('uses bounded client filtering only for legacy servers', async () => {
    const firstPage = Array.from({ length: 60 }, (_, index) => legacySummary(`trace-${index}`))
    const secondPage = Array.from({ length: 60 }, (_, index) =>
      legacySummary(`trace-${index + 60}`),
    )
    const listPage = vi
      .fn()
      .mockResolvedValueOnce({ nextPageToken: 'page-2', traces: firstPage })
      .mockResolvedValueOnce({ nextPageToken: 'page-3', traces: secondPage })

    const traces = await listQueryTraces(request, workspace, listPage)

    expect(listPage).toHaveBeenCalledTimes(2)
    expect(listPage).toHaveBeenNthCalledWith(
      2,
      request,
      workspace,
      100,
      'page-2',
      TraceView.QUERY_STREAM,
    )
    expect(traces).toHaveLength(80)
    expect(traces.map(({ traceId }) => traceId)).toEqual(
      Array.from({ length: 80 }, (_, index) => `trace-${index}`),
    )
  })

  it('does not scan general trace pages after receiving typed operations', async () => {
    const listPage = vi.fn().mockResolvedValue({
      nextPageToken: 'unused-page',
      traces: [summary('typed')],
    })

    await expect(listQueryTraces(request, workspace, listPage)).resolves.toEqual([summary('typed')])
    expect(listPage).toHaveBeenCalledOnce()
  })

  it('maps unavailable and generic failures into route data', async () => {
    const unavailable = vi.fn().mockRejectedValue(new Error('rpc unimplemented'))
    await expect(loadTracesRouteData(request, workspace, unavailable)).resolves.toMatchObject({
      loadError:
        'Trace storage is not enabled for this Coral server. Enable [local_traces].enabled = true, restart the Coral server, then run an operation.',
      traces: [],
    })

    const generic = vi.fn().mockRejectedValue(new Error('sidecar unavailable'))
    await expect(loadTracesRouteData(request, workspace, generic)).resolves.toMatchObject({
      loadError: 'sidecar unavailable',
      traces: [],
    })
  })

  it('falls back to the service name for an invalid request URL', () => {
    expect(traceEndpointLabel({ url: 'not a url' } as Request)).toBe('TraceService')
  })
})
