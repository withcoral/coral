import { describe, expect, it, vi } from 'vitest'

import { createRequestCancellationBridge } from './request-cancellation'

describe('createRequestCancellationBridge', () => {
  it('aborts the request and upstream body when the response is cancelled', async () => {
    const cancelUpstream = vi.fn()
    const upstreamBody = new ReadableStream<Uint8Array>({ cancel: cancelUpstream })
    const cancellation = createRequestCancellationBridge(new Request('https://app.test/oauth'))
    const aborted = new Promise<void>((resolve) => {
      cancellation.request.signal.addEventListener('abort', () => resolve(), { once: true })
    })
    const response = cancellation.wrapResponse(new Response(upstreamBody))
    const reason = new Error('renderer cancelled')

    await response.body!.cancel(reason)
    await aborted

    expect(cancellation.request.signal.reason).toBe(reason)
    expect(cancelUpstream).toHaveBeenCalledWith(reason)
  })
})
