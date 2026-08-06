import {
  createMemoryRouter,
  RouterProvider,
  useLoaderData,
  type LoaderFunction,
} from 'react-router'
import {
  TraceInvocationKind,
  TraceOperationKind,
  TraceStatus,
} from '@/generated/coral/v1/traces_pb'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { userEvent } from 'vitest/browser'
import { render } from 'vitest-browser-react'

import { shouldRevalidate } from '@/routes/traces'
import { routePath } from '@/routing/routemap'

import { TracesIndex } from './traces-index'
import type { TraceSummaryData } from './trace-utils'

interface Data {
  endpointLabel: string
  loadError: string | null
  referenceTimeMs: number
  traces: TraceSummaryData[]
}

const WORKSPACE_ID = 'analytics'
const TRACES_PATH = routePath('workspaceTraces', { workspaceId: WORKSPACE_ID })

function summary(traceId: string): TraceSummaryData {
  return {
    durationNanos: '1000000',
    endTimeUnixNanos: '2000000',
    name: 'coral.query',
    invocationKind: TraceInvocationKind.UNSPECIFIED,
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

function IndexRoute() {
  return <TracesIndex {...(useLoaderData() as Data)} workspaceId={WORKSPACE_ID} />
}

function renderIndex(loader: LoaderFunction, initialEntry = `${TRACES_PATH}?pro`) {
  const router = createMemoryRouter(
    [
      {
        children: [{ element: <div>Trace detail</div>, path: ':traceId' }],
        element: <IndexRoute />,
        loader,
        path: TRACES_PATH,
        shouldRevalidate,
      },
    ],
    { initialEntries: [initialEntry] },
  )
  return { router, screen: render(<RouterProvider router={router} />) }
}

function dispatchModKey(key: string, code: string) {
  const mac = /Mac|iPhone|iPad|iPod/.test(navigator.platform)
  window.dispatchEvent(
    new KeyboardEvent('keydown', {
      bubbles: true,
      code,
      ctrlKey: !mac,
      key,
      metaKey: mac,
    }),
  )
}

afterEach(() => vi.useRealTimers())

describe('TracesIndex route behavior', () => {
  it('polls only exact /traces every 30 seconds and guards overlapping refreshes', async () => {
    vi.useFakeTimers()
    let releaseRefresh: (() => void) | undefined
    const loader = vi
      .fn()
      .mockResolvedValueOnce({
        endpointLabel: 'reef.test',
        referenceTimeMs: 120_000,
        loadError: null,
        traces: [summary('a')],
      })
      .mockImplementationOnce(
        () =>
          new Promise<Data>((resolve) => {
            releaseRefresh = () =>
              resolve({
                endpointLabel: 'reef.test',
                referenceTimeMs: 120_000,
                loadError: null,
                traces: [summary('a')],
              })
          }),
      )
      .mockResolvedValue({
        endpointLabel: 'reef.test',
        referenceTimeMs: 120_000,
        loadError: null,
        traces: [summary('a')],
      })
    const { router, screen } = renderIndex(loader)
    await expect.element((await screen).getByText("select 'a'", { exact: false })).toBeVisible()

    await vi.advanceTimersByTimeAsync(29_999)
    expect(loader).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1)
    expect(loader).toHaveBeenCalledTimes(2)
    await vi.advanceTimersByTimeAsync(30_000)
    expect(loader).toHaveBeenCalledTimes(2)
    releaseRefresh?.()
    await vi.runAllTicks()

    await router.navigate(`${TRACES_PATH}/a?pro`)
    await vi.advanceTimersByTimeAsync(60_000)
    expect(loader).toHaveBeenCalledTimes(2)
  })

  it('retains the last successful list on refresh error, then accepts a later empty success', async () => {
    let data: Data = {
      endpointLabel: 'reef.test',
      referenceTimeMs: 120_000,
      loadError: null,
      traces: [summary('kept')],
    }
    const { router, screen } = renderIndex(() => data)
    const rendered = await screen
    await expect.element(rendered.getByText("select 'kept'", { exact: false })).toBeVisible()

    data = {
      endpointLabel: 'reef.test',
      referenceTimeMs: 120_000,
      loadError: 'sidecar unavailable',
      traces: [],
    }
    await router.revalidate()
    await expect.element(rendered.getByText('sidecar unavailable')).toBeVisible()
    await expect.element(rendered.getByText("select 'kept'", { exact: false })).toBeVisible()
    await expect.element(rendered.getByText('Disconnected')).toBeVisible()

    data = { endpointLabel: 'reef.test', referenceTimeMs: 120_000, loadError: null, traces: [] }
    await router.revalidate()
    await expect.element(rendered.getByText('No queries yet')).toBeVisible()
    await expect.element(rendered.getByText('Connected')).toBeVisible()
    expect(rendered.getByText("select 'kept'", { exact: false }).query()).toBeNull()
  })

  it('searches with mod+F, clears with Escape, and navigates the active row with Arrow/Enter', async () => {
    const scrollIntoView = vi.fn()
    const original = HTMLElement.prototype.scrollIntoView
    HTMLElement.prototype.scrollIntoView = scrollIntoView
    const { router, screen } = renderIndex(() => ({
      endpointLabel: 'reef.test',
      referenceTimeMs: 120_000,
      loadError: null,
      traces: [summary('alpha'), summary('playwright')],
    }))
    const rendered = await screen

    dispatchModKey('f', 'KeyF')
    const search = rendered.getByPlaceholder('Search queries...')
    await expect.element(search).toHaveFocus()
    await search.fill('playwright')
    await expect.element(rendered.getByText('1 of 2 queries')).toBeVisible()
    expect(rendered.getByText("select 'alpha'", { exact: false }).query()).toBeNull()
    await userEvent.keyboard('{Escape}')
    await expect.element(rendered.getByText('2 queries')).toBeVisible()

    await userEvent.keyboard('{ArrowDown}')
    await expect
      .element(rendered.getByRole('link', { name: /select 'alpha'/i }))
      .toHaveAttribute('data-active', 'true')
    expect(scrollIntoView).toHaveBeenCalled()
    await userEvent.keyboard('{Enter}')
    expect(router.state.location.pathname).toBe(`${TRACES_PATH}/alpha`)
    expect(router.state.location.search).toBe('?pro')
    HTMLElement.prototype.scrollIntoView = original
  })

  it('renders successful empty and unavailable list states', async () => {
    const empty = renderIndex(() => ({
      endpointLabel: 'reef.test',
      referenceTimeMs: 120_000,
      loadError: null,
      traces: [],
    }))
    await expect.element((await empty.screen).getByText('No queries yet')).toBeVisible()
    await expect.element((await empty.screen).getByText('0 queries')).toBeVisible()

    const unavailable = renderIndex(() => ({
      endpointLabel: 'reef.test',
      referenceTimeMs: 120_000,
      loadError:
        'Trace storage is not enabled for this Coral server. Enable [local_traces].enabled = true',
      traces: [],
    }))
    await expect.element((await unavailable.screen).getByText('Tracing unavailable')).toBeVisible()
    await expect.element((await unavailable.screen).getByText('Disconnected')).toBeVisible()
  })
})
