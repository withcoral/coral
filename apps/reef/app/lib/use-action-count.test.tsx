/** @vitest-environment happy-dom */
import { act, renderHook, waitFor } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'
import { useActionCounts } from './use-action-count'

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
// Each event either changes another Workspace's count or ends the connection, so the test
// waits until the hook has read it. Without that, an unchanged count passes at once.
it.each([
  ['a streamed count', { kind: 'count', counts: { [w]: '3', [x]: '4' } }, { [w]: '3', [x]: '4' }],
  ['a streamed zero', { kind: 'count', counts: { [w]: '0', [x]: '4' } }, { [w]: '0', [x]: '4' }],
  [
    'no count for an unavailable workspace count',
    { kind: 'count', counts: { [w]: null, [x]: '4' } },
    { [w]: null, [x]: '4' },
  ],
  [
    'the last counts for a count that is not a digit string',
    { kind: 'count', counts: { [w]: 3 } },
    { [w]: '1' },
  ],
  ['the last counts for counts that are not a map', { kind: 'count', counts: ['3'] }, { [w]: '1' }],
  ['the last counts while the stream is unavailable', { kind: 'unavailable' }, { [w]: '1' }],
  ['nothing when the session expires', { kind: 'sessionExpired' }, null],
])('shows %s', async (_, event, expected) => {
  const { fetch, emit } = countStream()
  const { result, unmount } = renderHook(() => useActionCounts({ [w]: '1' }))
  expect(result.current).toEqual({ [w]: '1' })
  await waitFor(() => expect(fetch).toHaveBeenCalledWith('/activity/count', expect.anything()))
  const signal = fetch.mock.calls[0][1].signal!
  act(() => emit(event))
  // A connection that ends aborts before it reports, so the report has run once it aborts.
  await waitFor(() => expect(signal.aborted || x in (result.current ?? {})).toBe(true))
  expect(result.current).toEqual(expected)
  unmount()
  expect(signal.aborted).toBe(true)
})

it.each([
  ['clears the last counts when the stream stays down', [], null],
  [
    'keeps a fresh snapshot that arrives in time',
    [{ kind: 'count', counts: { [w]: '2' } }],
    { [w]: '2' },
  ],
])('%s 30 s after it drops', async (_, later, expected) => {
  vi.useFakeTimers({ shouldAdvanceTime: true })
  const { fetch, emit } = countStream()
  const { result, unmount } = renderHook(() => useActionCounts({ [w]: '1' }))
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1))
  act(() => emit({ kind: 'unavailable' }))
  await waitFor(() => expect(fetch.mock.calls[0][1].signal!.aborted).toBe(true))
  // The retry opens a new connection; each later event arrives on it.
  await act(() => vi.advanceTimersByTimeAsync(2000))
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2))
  for (const event of later) act(() => emit(event))
  await act(() => vi.advanceTimersByTimeAsync(27_000))
  expect(result.current).toEqual(later.length ? expected : { [w]: '1' })
  await act(() => vi.advanceTimersByTimeAsync(1000))
  expect(result.current).toEqual(expected)
  unmount()
})
