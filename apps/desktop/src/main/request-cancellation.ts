// Electron cancels the response body when the renderer aborts a custom-protocol
// fetch, but not the handler request. Restore that link for React Router.
export function createRequestCancellationBridge(request: Request) {
  const abortController = new AbortController()

  return {
    request: new Request(request, {
      signal: AbortSignal.any([request.signal, abortController.signal]),
    }),
    wrapResponse(response: Response): Response {
      if (!response.body) return response

      const bridge = new TransformStream<Uint8Array, Uint8Array>()
      void response.body.pipeTo(bridge.writable).catch((reason) => abortController.abort(reason))

      return new Response(bridge.readable, {
        headers: response.headers,
        status: response.status,
        statusText: response.statusText,
      })
    },
  }
}
