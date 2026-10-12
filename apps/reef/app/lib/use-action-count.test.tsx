/** @vitest-environment happy-dom */
import { act, renderHook, waitFor } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'
import { useActionCounts } from './use-action-count'

afterEach(() => vi.unstubAllGlobals())

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
it.each([
  ['a streamed count', { kind: 'count', counts: { [w]: '3' } }, { [w]: '3' }],
  ['a streamed zero', { kind: 'count', counts: { [w]: '0' } }, { [w]: '0' }],
  ['an unavailable workspace count', { kind: 'count', counts: { [w]: null } }, { [w]: null }],
  ['nothing for a count that is not a digit string', { kind: 'count', counts: { [w]: 3 } }, null],
  ['nothing for counts that are not a map', { kind: 'count', counts: ['3'] }, null],
  ['nothing when the count is unavailable', { kind: 'unavailable' }, null],
  ['nothing when the session expires', { kind: 'sessionExpired' }, null],
])('shows %s', async (_, event, expected) => {
  const { fetch, emit } = countStream()
  const { result, unmount } = renderHook(() => useActionCounts({ [w]: '1' }))
  expect(result.current).toEqual({ [w]: '1' })
  await waitFor(() => expect(fetch).toHaveBeenCalledWith('/activity/count', expect.anything()))
  act(() => emit(event))
  await waitFor(() => expect(result.current).toEqual(expected))
  unmount()
  expect(fetch.mock.calls[0][1].signal!.aborted).toBe(true)
})
