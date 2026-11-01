/** @vitest-environment happy-dom */
import { act, renderHook, waitFor } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'
import { useActionSignal } from './use-action-count'

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

/** Serves `/activity/count` as a stream the test writes events to. */
function countStream() {
  let controller!: ReadableStreamDefaultController<Uint8Array>
  const fetch = vi.fn(
    async (_path: string, init: RequestInit) =>
      new Response(
        new ReadableStream<Uint8Array>({
          start(value) {
            controller = value
            init.signal!.addEventListener('abort', () => value.error(new Error('aborted')), {
              once: true,
            })
          },
        }),
        { headers: { 'Content-Type': 'text/event-stream' } },
      ),
  )
  vi.stubGlobal('fetch', fetch)
  return {
    fetch,
    emit: (event: unknown) =>
      controller.enqueue(new TextEncoder().encode(`data: ${JSON.stringify(event)}\n\n`)),
  }
}

const w = '/workspaces/w'
const x = '/workspaces/x'
/** A count with every request in one kind. */
const approvals = (total: string) => ({
  total,
  approvals: total,
  connections: '0',
  revisions: { approvals: `revision-${total}`, connections: '' },
})
const split = {
  total: '3',
  approvals: '2',
  connections: '1',
  revisions: { approvals: 'a', connections: 'c' },
}
// Each event either changes another Workspace's count or ends the connection, so the test
// waits until the hook has read it. Without that, an unchanged count passes at once.
it.each([
  [
    'a streamed count',
    { kind: 'count', counts: { [w]: split, [x]: approvals('4') } },
    { [w]: split, [x]: approvals('4') },
  ],
  [
    'a streamed zero',
    { kind: 'count', counts: { [w]: approvals('0'), [x]: approvals('4') } },
    { [w]: approvals('0'), [x]: approvals('4') },
  ],
  [
    'no count for an unavailable workspace count',
    { kind: 'count', counts: { [w]: null, [x]: approvals('4') } },
    { [w]: null, [x]: approvals('4') },
  ],
  [
    'the last counts for a kind that is not a digit string',
    { kind: 'count', counts: { [w]: { ...split, approvals: 3 } } },
    { [w]: approvals('1') },
  ],
  [
    'the last counts for a count without its revisions',
    { kind: 'count', counts: { [w]: { total: '3', approvals: '3', connections: '0' } } },
    { [w]: approvals('1') },
  ],
  [
    'the last counts for a count without its kinds',
    { kind: 'count', counts: { [w]: '3' } },
    { [w]: approvals('1') },
  ],
  [
    'the last counts for counts that are not a map',
    { kind: 'count', counts: ['3'] },
    { [w]: approvals('1') },
  ],
  [
    'the last counts while the stream is unavailable',
    { kind: 'unavailable' },
    { [w]: approvals('1') },
  ],
  ['nothing when the session expires', { kind: 'sessionExpired' }, null],
])('shows %s', async (_, event, expected) => {
  const { fetch, emit } = countStream()
  const { result, unmount } = renderHook(() => useActionSignal({ [w]: approvals('1') }))
  expect(result.current.counts).toEqual({ [w]: approvals('1') })
  await waitFor(() => expect(fetch).toHaveBeenCalledWith('/activity/count', expect.anything()))
  const signal = fetch.mock.calls[0][1].signal!
  act(() => emit(event))
  // A connection that ends aborts before it reports, so the report has run once it aborts.
  await waitFor(() => expect(signal.aborted || x in (result.current.counts ?? {})).toBe(true))
  expect(result.current.counts).toEqual(expected)
  unmount()
  expect(signal.aborted).toBe(true)
})

it.each([
  ['clears the last counts when the stream stays down', [], null],
  [
    'keeps a fresh snapshot that arrives in time',
    [{ kind: 'count', counts: { [w]: approvals('2') } }],
    { [w]: approvals('2') },
  ],
])('%s 30 s after it drops', async (_, later, expected) => {
  vi.useFakeTimers({ shouldAdvanceTime: true })
  const { fetch, emit } = countStream()
  const { result, unmount } = renderHook(() => useActionSignal({ [w]: approvals('1') }))
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1))
  act(() => emit({ kind: 'unavailable' }))
  await waitFor(() => expect(fetch.mock.calls[0][1].signal!.aborted).toBe(true))
  // The retry opens a new connection; each later event arrives on it.
  await act(() => vi.advanceTimersByTimeAsync(2000))
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2))
  for (const event of later) act(() => emit(event))
  await act(() => vi.advanceTimersByTimeAsync(27_000))
  expect(result.current.counts).toEqual(later.length ? expected : { [w]: approvals('1') })
  await act(() => vi.advanceTimersByTimeAsync(1000))
  expect(result.current.counts).toEqual(expected)
  unmount()
})

it('reports a drop at once, while the counts wait out their grace period', async () => {
  vi.useFakeTimers({ shouldAdvanceTime: true })
  const { fetch, emit } = countStream()
  const { result, unmount } = renderHook(() => useActionSignal({ [w]: approvals('1') }))
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1))
  expect(result.current.connected).toBe(true)
  act(() => emit({ kind: 'unavailable' }))
  await waitFor(() => expect(result.current.connected).toBe(false))
  expect(result.current.counts).toEqual({ [w]: approvals('1') })
  await act(() => vi.advanceTimersByTimeAsync(2000))
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2))
  act(() => emit({ kind: 'count', counts: { [w]: approvals('2') } }))
  await waitFor(() => expect(result.current.connected).toBe(true))
  expect(result.current.counts).toEqual({ [w]: approvals('2') })
  unmount()
})

it('stays disconnected once the session expires', async () => {
  const { fetch, emit } = countStream()
  const { result, unmount } = renderHook(() => useActionSignal({ [w]: approvals('1') }))
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1))
  act(() => emit({ kind: 'sessionExpired' }))
  await waitFor(() => expect(result.current).toEqual({ counts: null, connected: false }))
  unmount()
})
