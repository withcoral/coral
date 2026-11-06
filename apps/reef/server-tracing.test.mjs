import assert from 'node:assert/strict'
import { test } from 'node:test'

import { Code, ConnectError } from '@connectrpc/connect'
import { context, propagation, SpanKind, SpanStatusCode, trace } from '@opentelemetry/api'

import { createRequestHandler, RouterContextProvider } from 'react-router'

import {
  createRequestTracing,
  requestInstrumentation,
  traceCoralCall,
  traceLoaderStep,
} from './server-tracing.js'

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
async function finishHandler(result, method = 'GET', url = 'https://reef.example.test/x') {
  let handle
  requestInstrumentation.handler({ instrument: (hooks) => (handle = hooks.request) })
  await handle(async () => result, { request: { method, url } })
}

/** Runs `respond` through one traced request and returns the exported spans. */
async function traced(incoming, respond, env = {}) {
  const spanExporter = collectingExporter()
  const tracing = createRequestTracing({ env, spanExporter })
  try {
    await tracing.traceRequest(incoming, () => respond?.())
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
function eventNames(span) {
  return span.events.map((event) => event.name)
}

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
    const [span] = await traced(request(path, { method }), () => finishHandler(result, method))

    assert.equal(span.name, expected.name, name)
    assert.deepEqual({ ...span.attributes }, expected.attributes, name)
    assert.equal(span.status.code, expected.status, name)
    assert.equal(span.status.message, undefined, name)
    assert.doesNotMatch(JSON.stringify(exportedFields(span)), /secret/, name)
  }
})

const secretFailure = () => Promise.reject(new Error('secret failure text'))

/** Runs one route function through the instrumentation, as React Router does. */
async function runRouteFunction(kind, id, result, during) {
  let hooks
  requestInstrumentation.route({ id, instrument: (given) => (hooks = given) })
  await hooks[kind](async () => {
    await during?.()
    return result
  })
}

/** Makes one Coral call through the interceptor and returns the call it sent. */
async function coralCall(next = async () => ({ stream: false })) {
  const call = {
    header: new Headers(),
    method: { name: 'ListWorkspaces' },
    service: { typeName: 'coral.management.v1.WorkspaceService' },
  }
  const response = await traceCoralCall(next)(call)
  return { call, response }
}

const CORAL_METHOD = 'coral.management.v1.WorkspaceService/ListWorkspaces'

function spanNamed(spans, name) {
  return spans.find((span) => span.name === name)
}

test('names a loader or action span by route and nests its Coral calls', async () => {
  for (const [kind, name] of [
    ['loader', 'loader routes/workspace'],
    ['action', 'action routes/workspace'],
  ]) {
    const spans = await traced(request(), () =>
      runRouteFunction(kind, 'routes/workspace', { status: 'success' }, coralCall),
    )

    const [routeSpan, client, root] = [
      spanNamed(spans, name),
      spanNamed(spans, CORAL_METHOD),
      spans.at(-1),
    ]
    assert.equal(routeSpan.kind, SpanKind.INTERNAL, name)
    assert.equal(routeSpan.parentSpanContext.spanId, root.spanContext().spanId, name)
    assert.equal(client.parentSpanContext.spanId, routeSpan.spanContext().spanId, name)
    assert.equal(routeSpan.status.code, SpanStatusCode.UNSET, name)
  }
})

test('fails a route span only for a server failure, and keeps its text out', async () => {
  for (const [result, errorType, status] of [
    [{ status: 'error', error: new Response(null, { status: 302 }) }, undefined, 'UNSET'],
    [{ status: 'error', error: new Response(null, { status: 404 }) }, undefined, 'UNSET'],
    [{ status: 'error', error: new Response(null, { status: 503 }) }, '503', 'ERROR'],
    [{ status: 'error', error: new Error('secret failure text') }, '_OTHER', 'ERROR'],
  ]) {
    const spans = await traced(request(), () => runRouteFunction('loader', 'routes/a', result))

    const span = spanNamed(spans, 'loader routes/a')
    assert.equal(span.attributes['error.type'], errorType)
    assert.equal(span.status.code, SpanStatusCode[status])
    assert.doesNotMatch(JSON.stringify(exportedFields(span)), /secret/)
  }
})

test('makes a client span the parent of each Coral call', async () => {
  let sent
  const spans = await traced(request(), async () => {
    sent = (await coralCall()).call
  })

  const client = spanNamed(spans, CORAL_METHOD)
  assert.equal(client.kind, SpanKind.CLIENT)
  assert.deepEqual(
    { ...client.attributes },
    {
      'rpc.method': CORAL_METHOD,
      'rpc.response.status_code': 'OK',
      'rpc.system.name': 'grpc',
    },
  )
  const { traceId, spanId } = client.spanContext()
  assert.equal(sent.header.get('traceparent'), `00-${traceId}-${spanId}-01`)
})

test('records a Coral failure as Coral does, and keeps its text out', async () => {
  for (const [code, statusName, errorType, status] of [
    [Code.NotFound, 'NOT_FOUND', undefined, 'UNSET'],
    [Code.Canceled, 'CANCELLED', undefined, 'UNSET'],
    [Code.Unavailable, 'UNAVAILABLE', 'UNAVAILABLE', 'ERROR'],
    [Code.Internal, 'INTERNAL', 'INTERNAL', 'ERROR'],
  ]) {
    const spans = await traced(request(), () =>
      assert.rejects(
        coralCall(async () => {
          throw new ConnectError('secret failure text', code)
        }),
        ConnectError,
      ),
    )

    const client = spanNamed(spans, CORAL_METHOD)
    assert.equal(client.attributes['rpc.response.status_code'], statusName)
    assert.equal(client.attributes['error.type'], errorType)
    assert.equal(client.status.code, SpanStatusCode[status])
    assert.doesNotMatch(JSON.stringify(exportedFields(client)), /secret/)
  }
})

test('fails a client span for an error that is not a gRPC status', async () => {
  const spans = await traced(request(), () => assert.rejects(coralCall(secretFailure)))

  const client = spanNamed(spans, CORAL_METHOD)
  assert.equal(client.attributes['error.type'], '_OTHER')
  assert.equal(client.attributes['rpc.response.status_code'], undefined)
  assert.equal(client.status.code, SpanStatusCode.ERROR)
})

test('keeps a streaming client span open until its stream ends', async () => {
  for (const [messages, statusName] of [
    [
      async function* () {
        yield 'first'
      },
      'OK',
    ],
    [
      // oxlint-disable-next-line require-yield -- The stream fails before its first message.
      async function* () {
        throw new ConnectError('secret failure text', Code.Unavailable)
      },
      'UNAVAILABLE',
    ],
  ]) {
    const spanExporter = collectingExporter()
    const tracing = createRequestTracing({ env: {}, spanExporter })
    await tracing.traceRequest(request(), async () => {
      const { response } = await coralCall(async () => ({ message: messages(), stream: true }))
      await tracing.flush()
      assert.equal(spanNamed(spanExporter.spans, CORAL_METHOD), undefined, statusName)
      await assert.doesNotReject(async () => {
        try {
          for await (const message of response.message) assert.equal(message, 'first')
        } catch (error) {
          assert.ok(error instanceof ConnectError)
        }
      })
    })
    await tracing.shutdown()

    const client = spanNamed(spanExporter.spans, CORAL_METHOD)
    assert.equal(client.attributes['rpc.response.status_code'], statusName)
  }
})

test('records a stream its consumer stops early as cancelled', async () => {
  const spans = await traced(request(), async () => {
    const { response } = await coralCall(async () => ({
      message: (async function* () {
        yield 'first'
        yield 'second'
      })(),
      stream: true,
    }))
    for await (const message of response.message) {
      assert.equal(message, 'first')
      break
    }
  })

  const client = spanNamed(spans, CORAL_METHOD)
  assert.equal(client.attributes['rpc.response.status_code'], 'CANCELLED')
  assert.equal(client.status.code, SpanStatusCode.UNSET)
})

test('opens no route or client span outside a traced request', async () => {
  const spanExporter = collectingExporter()
  const tracing = createRequestTracing({ env: {}, spanExporter })
  let sent
  await tracing.traceRequest(request('/readyz'), () =>
    runRouteFunction('loader', 'routes/readyz', { status: 'success' }, async () => {
      sent = (await coralCall()).call
    }),
  )
  await tracing.shutdown()

  assert.deepEqual(spanExporter.spans, [])
  assert.equal(sent.header.get('traceparent'), null)
})

test('fails the span when the response breaks off, not when the caller left', async () => {
  const spanExporter = collectingExporter()
  const tracing = createRequestTracing({ env: {}, spanExporter })
  const caller = new AbortController()

  await assert.rejects(tracing.traceRequest(request('/handler'), secretFailure))
  await assert.rejects(
    tracing.traceRequest(request('/body'), async () => {
      await finishHandler({ statusCode: 200 })
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
  assert.deepEqual(eventNames(body), ['response ready'])
  assert.deepEqual(eventNames(left), ['caller left'])
  assert.equal(handler.status.code, SpanStatusCode.ERROR)
  assert.equal(handler.attributes['error.type'], '_OTHER')
  assert.equal(handler.attributes['http.response.status_code'], 500)
  assert.equal(body.status.code, SpanStatusCode.ERROR)
  assert.equal(body.attributes['error.type'], 'response_body')
  assert.equal(body.attributes['http.response.status_code'], 200)
  assert.doesNotMatch(JSON.stringify(spanExporter.spans.map(exportedFields)), /secret/)
  assert.equal(left.status.code, SpanStatusCode.UNSET)
  assert.equal(left.attributes['error.type'], undefined)
  assert.equal(left.attributes['http.response.status_code'], undefined)
})

test('names a data request by its route template alone', async () => {
  const meta = { pattern: 'workspaces/:workspace' }
  for (const [path, expected] of [
    ['/workspaces/42.data?_routes=secret', '/workspaces/:workspace.data'],
    ['/workspaces/42?days=7', undefined],
  ]) {
    const [span] = await traced(request(path), () =>
      finishHandler({ meta, statusCode: 200 }, 'GET', request(path).url),
    )
    assert.equal(span.attributes['http.route'], '/workspaces/:workspace', path)
    assert.equal(span.attributes['url.template'], expected, path)
    assert.doesNotMatch(JSON.stringify(exportedFields(span)), /secret|42/, path)
  }
})

const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const millis = ([seconds, nanos]) => seconds * 1000 + nanos / 1e6
const fails = (error) => async () => {
  throw error
}
const serverRoute = (id, path, parentId, loader) => ({
  id,
  path,
  parentId,
  module: { default: () => null, loader },
})

// Catches: a later React Router that answers before its slowest loader, which would
// end the claim that a slow shell loader holds back a data response.
test('a data response is ready only after the slowest loader of a real request', async () => {
  const spanExporter = collectingExporter()
  const tracing = createRequestTracing({ env: {}, spanExporter })
  const handler = createRequestHandler(
    {
      routes: {
        root: serverRoute('root', '', undefined, () => null),
        'routes/shell': serverRoute('routes/shell', 'page', 'root', () =>
          traceLoaderStep('shell step', () => pause(40)),
        ),
        'routes/page': serverRoute('routes/page', 'child', 'routes/shell', () => 'page'),
      },
      entry: { module: { default: () => null, instrumentations: [requestInstrumentation] } },
      assets: { routes: {}, entry: { module: '', imports: [] }, url: '', version: '' },
      future: {},
      publicPath: '/',
      assetsBuildDirectory: 'build',
      ssr: true,
      isSpaMode: false,
      prerender: [],
      routeDiscovery: { mode: 'lazy', manifestPath: '/__manifest' },
      allowedActionOrigins: false,
    },
    'production',
  )
  const incoming = new Request('https://reef.example.test/page/child.data')
  let status
  await tracing.traceRequest(incoming, async () => {
    const response = await handler(incoming, new RouterContextProvider())
    status = response.status
    await response.text()
  })
  await tracing.shutdown()

  assert.equal(status, 200)
  const root = spanExporter.spans.at(-1)
  const ready = root.events.find((event) => event.name === 'response ready')
  const step = spanNamed(spanExporter.spans, 'shell step')
  const shell = spanNamed(spanExporter.spans, 'loader routes/shell')
  const page = spanNamed(spanExporter.spans, 'loader routes/page')
  assert.equal(root.attributes['url.template'], '/page/child.data')
  assert.equal(step.parentSpanContext.spanId, shell.spanContext().spanId)
  assert.ok(millis(page.endTime) < millis(step.endTime), 'the page loader ends first')
  assert.ok(millis(ready.time) >= millis(step.endTime), 'the response waits for the slow step')
  assert.ok(millis(root.endTime) >= millis(ready.time), 'the body ends after the response is ready')
})

test('a loader step nests its Coral calls, and fails only for what is not a Coral status', async () => {
  const spanExporter = collectingExporter()
  const tracing = createRequestTracing({ env: {}, spanExporter })
  let answer
  await tracing.traceRequest(request(), async () => {
    answer = await traceLoaderStep('fine', async () => {
      await coralCall()
      return 'answer'
    })
    await assert.rejects(
      traceLoaderStep('rejected', fails(new ConnectError('x', Code.Unauthenticated))),
    )
    await assert.rejects(traceLoaderStep('broken', fails(new Error('secret'))))
  })
  await tracing.shutdown()

  assert.equal(answer, 'answer')
  const { spans } = spanExporter
  const fine = spanNamed(spans, 'fine')
  assert.equal(spanNamed(spans, CORAL_METHOD).parentSpanContext.spanId, fine.spanContext().spanId)
  assert.equal(fine.parentSpanContext.spanId, spans.at(-1).spanContext().spanId)
  assert.equal(fine.status.code, SpanStatusCode.UNSET)
  assert.equal(spanNamed(spans, 'rejected').status.code, SpanStatusCode.UNSET)
  const broken = spanNamed(spans, 'broken')
  assert.equal(broken.attributes['error.type'], '_OTHER')
  assert.doesNotMatch(JSON.stringify(exportedFields(broken)), /secret/)
})

test('a loader step outside a traced request opens no span and still answers', async () => {
  assert.equal(await traceLoaderStep('untraced', async () => 'answer'), 'answer')
  assert.equal(trace.getActiveSpan(), undefined)
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

test('flush sends finished spans and tracing keeps working', async () => {
  const spanExporter = collectingExporter()
  const tracing = createRequestTracing({ env: {}, spanExporter })

  await tracing.traceRequest(request('/first'), () => undefined)
  await tracing.flush()
  assert.equal(spanExporter.spans.length, 1)

  await tracing.traceRequest(request('/second'), () => undefined)
  await tracing.shutdown()
  assert.equal(spanExporter.spans.length, 2)
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
    await tracing.traceRequest(request('/', { headers }), async () => {
      outbound = (await coralCall()).call.header
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
