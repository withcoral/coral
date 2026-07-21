import { createMemoryRouter, Outlet, RouterProvider, useParams } from 'react-router'
import { TraceOperationKind, TraceStatus } from '@/generated/coral/v1/traces_pb'
import { describe, expect, it } from 'vitest'
import { userEvent } from 'vitest/browser'
import { render } from 'vitest-browser-react'

import type { TraceDetailData, TraceSpanData } from './trace-utils'
import { TraceDetail } from './trace-detail'

const WORKSPACE_ID = 'analytics'
const TRACES_PATH = `/workspaces/${WORKSPACE_ID}/traces`

function detail(): TraceDetailData {
  return {
    spans: [],
    summary: {
      durationNanos: '12000000',
      endTimeUnixNanos: '13000000',
      name: 'coral.query',
      operationKind: TraceOperationKind.UNSPECIFIED,
      operationName: '',
      query: 'select * from github.pull_requests',
      rootSpanId: 'root',
      rowCount: '7',
      rowCountRecorded: true,
      spanCount: 1,
      startTimeUnixNanos: '1000000',
      status: TraceStatus.OK,
      traceId: 'trace-07',
    },
  }
}

function httpSpan(
  spanId: string,
  table: string,
  extra: Record<string, unknown> = {},
): TraceSpanData {
  return {
    attributesJson: JSON.stringify({
      'coral.source': 'github',
      'coral.table': table,
      'http.request.method': 'GET',
      'http.response.status_code': 200,
      'url.full': `https://api.github.com/${table}`,
      ...extra,
    }),
    durationNanos: '5000000',
    endTimeUnixNanos: '7000000',
    eventsJson: '[]',
    kind: 'client',
    linksJson: '[]',
    name: 'http.get',
    parentSpanId: '',
    parentSpanIsRemote: false,
    resourceJson: '{}',
    scopeName: 'github',
    scopeAttributesJson: '{}',
    scopeSchemaUrl: '',
    scopeVersion: '',
    spanId,
    startTimeUnixNanos: '2000000',
    status: TraceStatus.OK,
    statusMessage: '',
    isRemote: false,
    traceFlags: 1,
    traceId: 'trace-07',
    traceState: '',
  }
}

function populatedDetail(): TraceDetailData {
  return {
    ...detail(),
    spans: [
      httpSpan('json', 'pull_requests', {
        'coral.http.response.body': JSON.stringify({ title: 'Rendered JSON body' }),
      }),
      httpSpan('malformed', 'issue_previews', {
        'coral.http.response.body': '{"oops":',
      }),
      httpSpan('missing', 'request_preview', {
        'http.request.body.present': true,
        'http.request.body.size': '2048',
      }),
      httpSpan('truncated', 'archive', {
        'coral.http.response.body.truncated': true,
        'http.response.body.size': '4096',
      }),
    ],
  }
}

function DetailRoute({
  loadError,
  loadedDetail,
}: {
  loadError: string | null
  loadedDetail: TraceDetailData | null
}) {
  const { traceId = '' } = useParams()
  return <TraceDetail detail={loadedDetail} loadError={loadError} traceId={traceId} />
}

function renderDetail(
  loadError: string | null,
  loadedDetail: TraceDetailData | null,
  traces = loadedDetail?.summary ? [loadedDetail.summary] : [],
  initialEntries?: string[],
) {
  const rootSpanId = loadedDetail?.summary?.rootSpanId
  const entries = initialEntries ?? [
    `${TRACES_PATH}/trace-07?pro${rootSpanId ? `&rootSpanId=${encodeURIComponent(rootSpanId)}` : ''}`,
  ]
  const router = createMemoryRouter(
    [
      {
        children: [
          {
            element: <DetailRoute loadError={loadError} loadedDetail={loadedDetail} />,
            path: ':traceId',
          },
        ],
        element: <Outlet context={{ traces, workspaceId: WORKSPACE_ID }} />,
        path: TRACES_PATH,
      },
    ],
    { initialEntries: entries, initialIndex: entries.length - 1 },
  )
  return { router, screen: render(<RouterProvider router={router} />) }
}

function dispatchModArrow(key: 'ArrowDown' | 'ArrowUp') {
  const mac = /Mac|iPhone|iPad|iPod/.test(navigator.platform)
  window.dispatchEvent(
    new KeyboardEvent('keydown', {
      bubbles: true,
      code: key,
      ctrlKey: !mac,
      key,
      metaKey: mac,
    }),
  )
}

describe('TraceDetail', () => {
  it('renders route-provided trace data', async () => {
    const { screen } = renderDetail(null, detail())

    await expect.element((await screen).getByText('Query details')).toBeVisible()
    await expect
      .element((await screen).getByText('select * from github.pull_requests'))
      .toBeVisible()
    await expect.element((await screen).getByText('7')).toBeVisible()
  })

  it('canonicalizes a selector-less legacy URL to the matching typed detail operation', async () => {
    const first = { ...detail().summary!, query: 'select 1', rootSpanId: 'root-a' }
    const second = { ...detail().summary!, query: 'select 2', rootSpanId: 'root-b' }
    const selectedDetail = detail()
    selectedDetail.summary = {
      ...second,
      operationKind: TraceOperationKind.QUERY,
      operationName: 'sql',
    }
    const { router, screen } = renderDetail(
      null,
      selectedDetail,
      [first, second],
      [TRACES_PATH, `${TRACES_PATH}/trace-07?pro`],
    )

    await expect
      .poll(() => new URLSearchParams(router.state.location.search).get('rootSpanId'))
      .toBe('root-b')
    await expect.element((await screen).getByText('select 2')).toBeVisible()

    await router.navigate(-1)
    expect(router.state.location.pathname).toBe(TRACES_PATH)
  })

  it('falls back to the first visible operation when legacy detail has no typed match', async () => {
    const first = { ...detail().summary!, query: 'select 1', rootSpanId: 'root-a' }
    const second = { ...detail().summary!, query: 'select 2', rootSpanId: 'root-b' }
    const { router, screen } = renderDetail(
      null,
      null,
      [first, second],
      [`${TRACES_PATH}/trace-07?pro`],
    )

    await expect
      .poll(() => new URLSearchParams(router.state.location.search).get('rootSpanId'))
      .toBe('root-a')
    await expect.element((await screen).getByText('select 1')).toBeVisible()
  })

  it('keeps route loader failures closable', async () => {
    const { router, screen } = renderDetail('trace lookup failed', null)
    const rendered = await screen

    await expect.element(rendered.getByText('Tracing unavailable')).toBeVisible()
    await rendered.getByRole('button', { name: 'Back to query stream' }).click()
    expect(router.state.location.pathname).toBe(TRACES_PATH)
    expect(router.state.location.search).toBe('?pro')
  })

  it('renders and selects populated HTTP spans with JSON and fallback body states', async () => {
    const { screen } = renderDetail(null, populatedDetail())
    const rendered = await screen
    expect(await rendered.getByRole('treeitem').elements()).toHaveLength(4)

    await rendered.getByRole('button', { name: /^GET github\.pull_requests\b/ }).click()
    await rendered.getByRole('tab', { name: 'Response body' }).click()
    await expect.element(rendered.getByText('"title": "Rendered JSON body"')).toBeVisible()

    await rendered.getByRole('button', { name: /^GET github\.issue_previews\b/ }).click()
    await rendered.getByRole('tab', { name: 'Response body' }).click()
    await expect.element(rendered.getByText('{"oops":')).toBeVisible()

    await rendered.getByRole('button', { name: /^GET github\.request_preview\b/ }).click()
    await rendered.getByRole('tab', { name: 'Request body' }).click()
    await expect
      .element(
        rendered.getByText('Request body was present (2.0 KB), but content was not captured.'),
      )
      .toBeVisible()

    await rendered.getByRole('button', { name: /^GET github\.archive\b/ }).click()
    await rendered.getByRole('tab', { name: 'Response body (truncated)' }).click()
    await expect
      .element(
        rendered.getByText('Response body was truncated (4.0 KB), but no preview was recorded.'),
      )
      .toBeVisible()
  })

  it('closes with Escape and selects adjacent traces from keyboard shortcuts', async () => {
    const currentSummary = detail().summary!
    const traces = ['newer', 'trace-07', 'older'].map((traceId) => ({
      ...currentSummary,
      traceId,
    }))
    const { router } = renderDetail(null, detail(), traces)
    await router.navigate(`${TRACES_PATH}/trace-07?search=playwright&pro&rootSpanId=root`)

    dispatchModArrow('ArrowDown')
    await expect.poll(() => router.state.location.pathname).toBe(`${TRACES_PATH}/older`)
    expect(router.state.location.search).toBe('?search=playwright&pro&rootSpanId=root')

    await router.navigate(`${TRACES_PATH}/trace-07?search=playwright&pro&rootSpanId=root`)
    dispatchModArrow('ArrowUp')
    await expect.poll(() => router.state.location.pathname).toBe(`${TRACES_PATH}/newer`)

    await userEvent.keyboard('{Escape}')
    await expect.poll(() => router.state.location.pathname).toBe(TRACES_PATH)
    expect(router.state.location.search).toBe('?search=playwright&pro')
  })

  it('navigates adjacent operations that share a trace ID by root span', async () => {
    const current = detail().summary!
    const first = { ...current, query: 'select 1', rootSpanId: 'root-a' }
    const second = { ...current, query: 'select 2', rootSpanId: 'root-b' }
    const selectedDetail = detail()
    selectedDetail.summary = first
    const { router } = renderDetail(null, selectedDetail, [first, second])
    await router.navigate(`${TRACES_PATH}/trace-07?pro&rootSpanId=root-a`)

    dispatchModArrow('ArrowDown')
    await expect
      .poll(() => new URLSearchParams(router.state.location.search).get('rootSpanId'))
      .toBe('root-b')
  })

  it('renders the no-summary state and keeps it closable', async () => {
    const { router, screen } = renderDetail(null, { spans: [] })
    const rendered = await screen
    await expect.element(rendered.getByText('No spans for this trace')).toBeVisible()
    await rendered.getByRole('button', { name: 'Back to query stream' }).click()
    expect(router.state.location.pathname).toBe(TRACES_PATH)
  })
})
