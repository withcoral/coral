import assert from 'node:assert/strict'
import { test } from 'node:test'

import { context, propagation, SpanKind, SpanStatusCode, trace } from '@opentelemetry/api'

import { createRequestTracing } from './server-tracing.js'

const INBOUND_TRACE = '0af7651916cd43dd8448eb211c80319c'
const INBOUND_SPAN = 'b7ad6b7169203331'
const INBOUND = `00-${INBOUND_TRACE}-${INBOUND_SPAN}-01`

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

function request(path = '/workspaces/42', init) {
  return new Request(`https://reef.example.test${path}`, init)
}

/** The headers a Coral gRPC call would carry from the active context. */
function outboundHeaders() {
  const headers = new Headers()
  propagation.inject(context.active(), headers, {
    set: (carrier, key, value) => carrier.set(key, value),
  })
  return headers
}

function outboundTraceparent() {
  return outboundHeaders().get('traceparent')
}

/** Finishes React Router's handler for the active request. */
async function finishHandler(tracing, result, method = 'GET') {
  let handle
  tracing.instrumentation.handler({ instrument: (hooks) => (handle = hooks.request) })
  await handle(async () => result, { request: { method } })
}

/** Runs `respond` through one traced request and returns the exported spans. */
async function traced(incoming, respond, env = {}) {
  const spanExporter = collectingExporter()
  const tracing = createRequestTracing({ env, spanExporter })
  try {
    await tracing.traceRequest(incoming, () => respond?.(tracing))
  } finally {
    await tracing.shutdown()
  }
  return spanExporter.spans
}

test('opens one root server span for a request without trace context', async () => {
  let outbound
  const spans = await traced(request(), () => {
    outbound = outboundTraceparent()
  })

  assert.equal(spans.length, 1)
  const [span] = spans
  assert.equal(span.kind, SpanKind.SERVER)
  assert.equal(span.parentSpanContext, undefined)
  const { traceId, spanId } = span.spanContext()
  assert.equal(outbound, `00-${traceId}-${spanId}-01`)
  assert.equal(span.status.code, SpanStatusCode.UNSET)
})

test('continues a valid inbound traceparent', async () => {
  let outbound
  const spans = await traced(
    request('/', { headers: { traceparent: INBOUND } }),
    () => (outbound = outboundTraceparent()),
  )

  const [span] = spans
  assert.equal(span.spanContext().traceId, INBOUND_TRACE)
  assert.equal(span.parentSpanContext?.spanId, INBOUND_SPAN)
  assert.equal(outbound, `00-${INBOUND_TRACE}-${span.spanContext().spanId}-01`)
})

test('starts a new trace for an invalid inbound traceparent', async () => {
  for (const traceparent of [
    'not-a-traceparent',
    `ff-${INBOUND_TRACE}-${INBOUND_SPAN}-01`,
    `00-${'0'.repeat(32)}-${INBOUND_SPAN}-01`,
    `00-${INBOUND_TRACE}-${'0'.repeat(16)}-01`,
    `00-${INBOUND_TRACE.toUpperCase()}-${INBOUND_SPAN}-01`,
    `${INBOUND}, ${INBOUND}`,
  ]) {
    const [span] = await traced(request('/', { headers: { traceparent } }))
    assert.equal(span.parentSpanContext, undefined, traceparent)
    assert.notEqual(span.spanContext().traceId, INBOUND_TRACE, traceparent)
  }
})

test('neither reads nor forwards baggage or tracestate', async () => {
  let outbound
  let baggage
  const spans = await traced(
    request('/', {
      headers: { traceparent: INBOUND, tracestate: 'vendor=secret', baggage: 'user=secret' },
    }),
    () => {
      baggage = propagation.getBaggage(context.active())
      outbound = outboundHeaders()
    },
  )

  assert.equal(outbound.get('baggage'), null)
  assert.equal(outbound.get('tracestate'), null)
  assert.equal(baggage, undefined)
  assert.doesNotMatch(JSON.stringify(spans.map(exportedFields)), /secret/)
})

/** Everything OTLP sends about a span except the IDs and timings. */
function exportedFields(span) {
  return {
    attributes: span.attributes,
    events: span.events,
    links: span.links,
    name: span.name,
    status: span.status,
    traceState: span.spanContext().traceState?.serialize(),
  }
}

test('names the span by route template and records the outcome with safe fields', async () => {
  const cases = [
    {
      name: 'a matched route',
      method: 'GET',
      path: '/workspaces/42/activity?secret=1',
      result: { status: 'success', statusCode: 200, meta: { pattern: 'workspaces/:workspace' } },
      span: {
        name: 'GET /workspaces/:workspace',
        attributes: {
          'http.request.method': 'GET',
          'http.response.status_code': 200,
          'http.route': '/workspaces/:workspace',
        },
        status: SpanStatusCode.UNSET,
      },
    },
    {
      name: 'a request no route matched',
      method: 'GET',
      path: '/secret-path',
      result: { status: 'success', statusCode: 404, meta: { pattern: '' } },
      span: {
        name: 'GET',
        attributes: { 'http.request.method': 'GET', 'http.response.status_code': 404 },
        status: SpanStatusCode.UNSET,
      },
    },
    {
      name: 'a server error',
      method: 'POST',
      path: '/workspaces',
      result: { status: 'success', statusCode: 503, meta: { pattern: 'workspaces' } },
      span: {
        name: 'POST /workspaces',
        attributes: {
          'error.type': '503',
          'http.request.method': 'POST',
          'http.response.status_code': 503,
          'http.route': '/workspaces',
        },
        status: SpanStatusCode.ERROR,
      },
    },
    {
      name: 'a method outside the standard set',
      method: 'PROPFIND',
      path: '/secret-path',
      result: { status: 'success', statusCode: 405, meta: undefined },
      span: {
        name: 'HTTP',
        attributes: { 'http.request.method': '_OTHER', 'http.response.status_code': 405 },
        status: SpanStatusCode.UNSET,
      },
    },
  ]

  for (const { name, method, path, result, span: expected } of cases) {
    const [span] = await traced(request(path, { method }), (tracing) =>
      finishHandler(tracing, result, method),
    )

    assert.equal(span.name, expected.name, name)
    assert.deepEqual({ ...span.attributes }, expected.attributes, name)
    assert.equal(span.status.code, expected.status, name)
    assert.equal(span.status.message, undefined, name)
    assert.doesNotMatch(JSON.stringify(exportedFields(span)), /secret/, name)
  }
})

const secretFailure = () => Promise.reject(new Error('secret failure text'))

test('fails the span when the response breaks off, not when the caller left', async () => {
  const spanExporter = collectingExporter()
  const tracing = createRequestTracing({ env: {}, spanExporter })
  const caller = new AbortController()

  await assert.rejects(tracing.traceRequest(request('/handler'), secretFailure))
  await assert.rejects(
    tracing.traceRequest(request('/body'), async () => {
      await finishHandler(tracing, { statusCode: 200 })
      await secretFailure()
    }),
  )
  await assert.rejects(
    tracing.traceRequest(request('/left', { signal: caller.signal }), () => {
      caller.abort()
      throw new Error('premature close')
    }),
  )
  await tracing.shutdown()

  const [handler, body, left] = spanExporter.spans
  assert.equal(handler.status.code, SpanStatusCode.ERROR)
  assert.equal(handler.attributes['error.type'], '_OTHER')
  assert.equal(body.status.code, SpanStatusCode.ERROR)
  assert.equal(body.attributes['error.type'], 'response_body')
  assert.doesNotMatch(JSON.stringify(spanExporter.spans.map(exportedFields)), /secret/)
  assert.equal(left.status.code, SpanStatusCode.UNSET)
  assert.equal(left.attributes['error.type'], undefined)
})

test('keeps the span open until the response ends', async () => {
  let open
  const spanExporter = collectingExporter()
  const tracing = createRequestTracing({ env: {}, spanExporter })
  await tracing.traceRequest(request(), async () => {
    await new Promise((resolve) => setImmediate(resolve))
    open = trace.getActiveSpan()?.isRecording()
  })
  await tracing.shutdown()
  assert.equal(open, true)
  assert.equal(spanExporter.spans.length, 1)
})

test('a collector that refuses the last batch does not fail shutdown', async () => {
  const spanExporter = {
    ...collectingExporter(),
    export: (_batch, done) => done({ code: 1, error: new Error('collector unavailable') }),
  }
  const tracing = createRequestTracing({ env: {}, spanExporter })
  await tracing.traceRequest(request(), () => undefined)

  await assert.doesNotReject(tracing.shutdown())
})

test('leaves health probes untraced', async () => {
  for (const path of ['/healthz', '/readyz']) {
    let outbound
    const spans = await traced(request(path, { headers: { traceparent: INBOUND } }), () => {
      outbound = outboundTraceparent()
    })
    assert.deepEqual(spans, [], path)
    assert.equal(outbound, null, path)
  }
})

test('without an exporter, forwards only a valid inbound traceparent', async () => {
  for (const [headers, expected] of [
    [{}, null],
    [{ traceparent: 'invalid' }, null],
    [{ traceparent: INBOUND, baggage: 'user=secret', tracestate: 'vendor=secret' }, INBOUND],
    [{ traceparent: `00-${INBOUND_TRACE}-${INBOUND_SPAN}-ff` }, INBOUND],
  ]) {
    const tracing = createRequestTracing({ env: {} })
    let outbound
    await tracing.traceRequest(request('/', { headers }), () => {
      outbound = outboundHeaders()
    })
    await tracing.shutdown()

    assert.equal(outbound.get('traceparent'), expected)
    assert.equal(outbound.get('baggage'), null)
    assert.equal(outbound.get('tracestate'), null)
  }
})

test('exports only when an operator configures it', async () => {
  for (const [env, exports] of [
    [{}, false],
    [{ OTEL_EXPORTER_OTLP_ENDPOINT: '  ' }, false],
    [{ OTEL_EXPORTER_OTLP_ENDPOINT: 'http://127.0.0.1:4318' }, true],
    [{ OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: 'http://127.0.0.1:4318/v1/traces' }, true],
    [{ OTEL_EXPORTER_OTLP_ENDPOINT: 'http://127.0.0.1:4318', OTEL_SDK_DISABLED: 'TRUE' }, false],
    [{ OTEL_EXPORTER_OTLP_ENDPOINT: 'http://127.0.0.1:4318', OTEL_TRACES_EXPORTER: 'none' }, false],
    [
      {
        OTEL_EXPORTER_OTLP_ENDPOINT: 'http://127.0.0.1:4318',
        OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: '  ',
      },
      true,
    ],
  ]) {
    // An unsampled span still names its trace, and leaves no span to send.
    const tracing = createRequestTracing({ env: { ...env, OTEL_TRACES_SAMPLER: 'always_off' } })
    let opened
    await tracing.traceRequest(request(), () => {
      opened = outboundTraceparent() !== null
    })
    await tracing.shutdown()
    assert.equal(opened, exports, JSON.stringify(env))
  }
})

test('samples as OTEL_TRACES_SAMPLER says, and honors a sampled caller by default', async () => {
  const unsampled = `00-${INBOUND_TRACE}-${INBOUND_SPAN}-00`
  for (const [env, headers, exported, outboundFlags] of [
    [{}, {}, 1, '01'],
    [{}, { traceparent: unsampled }, 0, '00'],
    [{ OTEL_TRACES_SAMPLER: 'always_off' }, {}, 0, '00'],
    [{ OTEL_TRACES_SAMPLER: 'always_on' }, { traceparent: unsampled }, 1, '01'],
    [{ OTEL_TRACES_SAMPLER: 'traceidratio', OTEL_TRACES_SAMPLER_ARG: '0' }, {}, 0, '00'],
    [{ OTEL_TRACES_SAMPLER: 'traceidratio', OTEL_TRACES_SAMPLER_ARG: '1' }, {}, 1, '01'],
    [{ OTEL_TRACES_SAMPLER: 'parentbased_always_off' }, {}, 0, '00'],
    [{ OTEL_TRACES_SAMPLER: 'parentbased_always_off' }, { traceparent: INBOUND }, 1, '01'],
    [
      { OTEL_TRACES_SAMPLER: 'parentbased_traceidratio', OTEL_TRACES_SAMPLER_ARG: '0' },
      {},
      0,
      '00',
    ],
  ]) {
    let outbound
    const spans = await traced(
      request('/', { headers }),
      () => (outbound = outboundTraceparent()),
      env,
    )
    assert.equal(spans.length, exported, JSON.stringify(env))
    assert.ok(outbound.endsWith(`-${outboundFlags}`), JSON.stringify(env))
  }
})

test('rejects export settings it cannot honor', () => {
  const endpoint = { OTEL_EXPORTER_OTLP_ENDPOINT: 'http://127.0.0.1:4318' }
  for (const [env, message] of [
    [{ OTEL_EXPORTER_OTLP_ENDPOINT: 'not a url' }, /OTEL_EXPORTER_OTLP_ENDPOINT must/],
    [{ OTEL_EXPORTER_OTLP_ENDPOINT: 'collector:4318' }, /OTEL_EXPORTER_OTLP_ENDPOINT must/],
    [
      { ...endpoint, OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: 'not a url' },
      /OTEL_EXPORTER_OTLP_TRACES_ENDPOINT must/,
    ],
    [{ ...endpoint, OTEL_EXPORTER_OTLP_PROTOCOL: 'grpc' }, /OTEL_EXPORTER_OTLP_PROTOCOL must/],
    [
      {
        ...endpoint,
        OTEL_EXPORTER_OTLP_PROTOCOL: 'http/protobuf',
        OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: 'grpc',
      },
      /OTEL_EXPORTER_OTLP_TRACES_PROTOCOL must/,
    ],
    [{ ...endpoint, OTEL_TRACES_SAMPLER: 'jaeger_remote' }, /OTEL_TRACES_SAMPLER must/],
    [
      { ...endpoint, OTEL_TRACES_SAMPLER: 'traceidratio', OTEL_TRACES_SAMPLER_ARG: '2' },
      /OTEL_TRACES_SAMPLER_ARG must/,
    ],
    [
      { ...endpoint, OTEL_TRACES_SAMPLER: 'traceidratio', OTEL_TRACES_SAMPLER_ARG: 'x' },
      /OTEL_TRACES_SAMPLER_ARG must/,
    ],
  ]) {
    assert.throws(() => createRequestTracing({ env }), message, JSON.stringify(env))
  }
})
