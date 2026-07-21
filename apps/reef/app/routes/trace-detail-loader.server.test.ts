import { create } from '@bufbuild/protobuf'

import { WorkspaceSchema } from '@/generated/coral/v1/resources_pb'
import { TraceStatus } from '@/generated/coral/v1/traces_pb'
import type { TraceDetailData } from '@/views/traces/trace-utils'
import { describe, expect, it, vi } from 'vitest'

import { loadTraceDetailRouteData } from './trace-detail-loader'

function detail(traceId: string): TraceDetailData {
  return {
    spans: [],
    summary: {
      durationNanos: '1000000',
      endTimeUnixNanos: '2000000',
      name: 'coral.query',
      query: 'select 1',
      rootSpanId: 'root',
      rowCount: '1',
      rowCountRecorded: true,
      spanCount: 1,
      startTimeUnixNanos: '1000000',
      status: TraceStatus.OK,
      traceId,
    },
  }
}

describe('trace detail loader', () => {
  const request = new Request('http://reef.test/workspaces/analytics/traces/trace-07')
  const workspace = create(WorkspaceSchema, { name: 'analytics' })

  it('loads the URL-selected trace without decoding it again', async () => {
    const getTrace = vi.fn().mockResolvedValue(detail('trace/with?reserved'))

    await expect(
      loadTraceDetailRouteData(
        request,
        'trace/with?reserved',
        workspace,
        'coral-access-token',
        getTrace,
      ),
    ).resolves.toEqual({ detail: detail('trace/with?reserved'), loadError: null })
    expect(getTrace).toHaveBeenCalledWith(
      request,
      'trace/with?reserved',
      workspace,
      'coral-access-token',
    )
  })

  it('returns an inline error for a missing trace ID', async () => {
    const getTrace = vi.fn()
    await expect(
      loadTraceDetailRouteData(request, undefined, workspace, 'coral-access-token', getTrace),
    ).resolves.toEqual({
      detail: null,
      loadError: 'Missing trace ID',
    })
    expect(getTrace).not.toHaveBeenCalled()
  })

  it('maps unavailable and generic failures into route data', async () => {
    await expect(
      loadTraceDetailRouteData(
        request,
        'trace-07',
        workspace,
        'coral-access-token',
        vi.fn().mockRejectedValue(new Error('HTTP 404 from TraceService')),
      ),
    ).resolves.toMatchObject({
      detail: null,
      loadError:
        'Trace storage is not enabled for this Coral server. Enable [local_traces].enabled = true, restart the Coral server, then run a query.',
    })

    await expect(
      loadTraceDetailRouteData(
        request,
        'trace-07',
        workspace,
        'coral-access-token',
        vi.fn().mockRejectedValue(new Error('trace lookup failed')),
      ),
    ).resolves.toEqual({ detail: null, loadError: 'trace lookup failed' })
  })

  it('never retains stale detail after a later trace lookup fails', async () => {
    const getTrace = vi
      .fn()
      .mockResolvedValueOnce(detail('trace-a'))
      .mockRejectedValueOnce(new Error('trace-b failed'))

    await expect(
      loadTraceDetailRouteData(request, 'trace-a', workspace, 'coral-access-token', getTrace),
    ).resolves.toEqual({
      detail: detail('trace-a'),
      loadError: null,
    })
    await expect(
      loadTraceDetailRouteData(request, 'trace-b', workspace, 'coral-access-token', getTrace),
    ).resolves.toEqual({
      detail: null,
      loadError: 'trace-b failed',
    })
    expect(typeof window).toBe('undefined')
  })
})
