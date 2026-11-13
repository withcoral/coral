import assert from 'node:assert/strict'
import { once } from 'node:events'
import { createServer, request as httpRequest } from 'node:http'
import { test } from 'node:test'

import { context, propagation, SpanKind, SpanStatusCode, trace } from '@opentelemetry/api'

import { traceDevRequests } from './dev-tracing.ts'
import { createRequestTracing } from './server-tracing.js'

const INBOUND = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'

function collectingExporter() {
  const spans = []
  return {
    spans,
    export(batch, done) {
      spans.push(...batch)
      done({ code: 0 })
    },
    shutdown: async () => undefined,
  }
}

function outboundTraceparent() {
  const carrier = {}
  propagation.inject(context.active(), carrier)
  return carrier.traceparent
}

/** Serves `next` behind the middleware and returns the spans it exported. */
async function served(next, drive) {
  const spanExporter = collectingExporter()
  const tracing = createRequestTracing({ env: {}, spanExporter })
  const middleware = traceDevRequests(tracing.traceRequest)
  const server = createServer((request, response) =>
    middleware(request, response, () => next(response)),
  )
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  try {
    await drive(`http://127.0.0.1:${server.address().port}`)
  } finally {
    server.close()
    await once(server, 'close')
    await tracing.shutdown()
  }
  return spanExporter.spans
}

test('runs the next handler in a span that ends with the response', async () => {
  let outbound
  let openWhenAnswered
  let finishResponse
  const responded = new Promise((resolve) => (finishResponse = resolve))
  const spans = await served(
    (response) => {
      outbound = outboundTraceparent()
      const active = trace.getActiveSpan()
      // The answer comes later, outside the middleware's own turn.
      responded.then(() => {
        openWhenAnswered = active.isRecording()
        response.end('ok')
      })
    },
    async (origin) => {
      const answer = fetch(`${origin}/workspaces/42?marker=secret`, {
        headers: { traceparent: INBOUND },
      })
      finishResponse()
      await (await answer).text()
    },
  )

  assert.equal(spans.length, 1)
  const [span] = spans
  assert.equal(span.kind, SpanKind.SERVER)
  assert.equal(span.spanContext().traceId, INBOUND.split('-')[1])
  assert.equal(outbound, `00-${INBOUND.split('-')[1]}-${span.spanContext().spanId}-01`)
  assert.equal(span.status.code, SpanStatusCode.UNSET)
  assert.equal(openWhenAnswered, true)
})

// A span that never ends fails here instead of holding the run.
test('a caller that leaves does not fail the span', { timeout: 10_000 }, async () => {
  let handling
  const handled = new Promise((resolve) => (handling = resolve))
  const spans = await served(
    () => handling(trace.getActiveSpan()),
    async (origin) => {
      const call = httpRequest(`${origin}/workspaces/42`)
      call.on('error', () => undefined)
      call.end()
      // Leave once the server handles the request, and wait until the server
      // sees it and ends the span. Fixed delays raced both on a busy machine:
      // a caller that left too soon opened no span, and tracing could stop
      // before the span ended.
      const span = await handled
      call.destroy()
      while (span.isRecording()) await new Promise((resolve) => setImmediate(resolve))
    },
  )

  assert.equal(spans.length, 1)
  assert.equal(spans[0].status.code, SpanStatusCode.UNSET)
})
