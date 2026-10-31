// Traces `./dev reef` requests the way server.js traces production ones.
import type { IncomingMessage, ServerResponse } from 'node:http'
import { finished } from 'node:stream'
import type { Plugin } from 'vite'

import { createRequestTracing, type RequestTracing } from './server-tracing.js'

const PROCESS_TRACING = Symbol.for('reef.dev-request-tracing')

/** Add this before `reactRouter()`, whose handler must run in the span. */
export function devRequestTracing(): Plugin {
  return {
    name: 'reef:dev-request-tracing',
    apply: 'serve',
    configureServer(server) {
      // Storybook and Vite's helper servers run in middleware mode, and React
      // Router serves no requests there.
      if (server.config.server.middlewareMode) return
      const middleware = traceDevRequests(processTracing().traceRequest)
      // A returned hook runs after Vite's asset middleware, so modules and
      // styles do not each open a span.
      return () => {
        server.middlewares.use(middleware)
      }
    },
    // Vite awaits this when it restarts or stops on SIGTERM. Coral has already
    // exported the children of these spans, so they must not be lost.
    async closeBundle() {
      await tracingHolder()[PROCESS_TRACING]?.flush()
    },
  }
}

function tracingHolder() {
  return globalThis as { [PROCESS_TRACING]?: RequestTracing }
}

// A Vite restart reloads this file, but OTel's globals last as long as the process.
function processTracing() {
  const holder = tracingHolder()
  if (holder[PROCESS_TRACING]) return holder[PROCESS_TRACING]
  const tracing = createRequestTracing()
  holder[PROCESS_TRACING] = tracing
  // Vite leaves Ctrl-C to Node, which would exit without closing the server.
  process.once('SIGINT', () => void tracing.flush().finally(() => process.exit(130)))
  return tracing
}

/** Runs the rest of the middleware chain in a span that ends with the response. */
export function traceDevRequests(traceRequest: RequestTracing['traceRequest']) {
  return (
    request: IncomingMessage,
    response: ServerResponse,
    next: (error?: unknown) => void,
  ): void => {
    const controller = new AbortController()
    response.once('close', () => {
      if (!response.writableFinished) controller.abort()
    })
    // The span reads no body and no header but `traceparent`.
    const { traceparent } = request.headers
    const traced = new Request(new URL(request.url ?? '/', 'http://localhost'), {
      headers: typeof traceparent === 'string' ? { traceparent } : {},
      method: request.method,
      signal: controller.signal,
    })
    void traceRequest(
      traced,
      () =>
        new Promise<void>((resolve, reject) => {
          finished(response, (error) => (error ? reject(error) : resolve()))
          next()
        }),
      // Vite has already answered or logged the failure, and the span has it.
    ).catch(() => undefined)
  }
}
