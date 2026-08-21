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

import { listQueryTraces, loadTracesRouteData } from './traces-loader'

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

describe('traces list loader', () => {
  const request = new Request('http://coral-ui.test/workspaces/analytics/traces')
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
      traces: [summary('query'), search, futureTool],
    })

    await expect(
      loadTracesRouteData(request, workspace, 'coral-access-token', listPage),
    ).resolves.toEqual({
      endpointLabel: 'coral-ui.test',
      loadError: null,
      referenceTimeMs: 123_456,
      traces: [summary('query'), search, futureTool],
    })
    expect(listPage).toHaveBeenCalledWith(
      request,
      workspace,
      100,
      '',
      TraceView.QUERY_STREAM,
      'coral-access-token',
    )
  })

  it('uses one bounded Query Stream page', async () => {
    const traces = Array.from({ length: 100 }, (_, index) => summary(`trace-${index}`))
    const listPage = vi.fn().mockResolvedValue({
      nextPageToken: 'unused-page',
      traces,
    })

    await expect(
      listQueryTraces(request, workspace, 'coral-access-token', listPage),
    ).resolves.toEqual(traces.slice(0, 80))
    expect(listPage).toHaveBeenCalledOnce()
  })

})
