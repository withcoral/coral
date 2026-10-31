// Spans for Reef's incoming requests: one server span for each request, one
// span for each loader and action it runs, and one client span for each Coral
// call. Coral's server span for a call is the child of its client span, so a
// gap between the two is time spent between the processes.
//
// This module sets the fields Coral's server spans carry (see
// crates/coral-telemetry/src/field.rs): the method, the route template and the
// status. A span never carries headers, bodies, query strings, cookies, full
// URLs or error text, and `baggage` and `tracestate` are never read.
import { ConnectError } from '@connectrpc/connect'
import {
  context,
  diag,
  DiagConsoleLogger,
  DiagLogLevel,
  propagation,
  ROOT_CONTEXT,
  SpanKind,
  SpanStatusCode,
  trace,
  TraceFlags,
} from '@opentelemetry/api'
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks'
import { parseTraceParent, W3CTraceContextPropagator } from '@opentelemetry/core'
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-proto'
import { detectResources, envDetector, resourceFromAttributes } from '@opentelemetry/resources'
import {
  AlwaysOffSampler,
  AlwaysOnSampler,
  BatchSpanProcessor,
  ParentBasedSampler,
  TraceIdRatioBasedSampler,
  TracerProvider,
} from '@opentelemetry/sdk-trace'

const STANDARD_METHODS = new Set([
  'CONNECT',
  'DELETE',
  'GET',
  'HEAD',
  'OPTIONS',
  'PATCH',
  'POST',
  'PUT',
  'TRACE',
])
// Probes poll every few seconds, so tracing them would bury real requests.
// Coral's own health routes open no span either.
const UNTRACED_PATHS = new Set(['/healthz', '/readyz'])
const TRACER_NAME = 'ui'
// These match Coral's server spans (crates/coral-telemetry/src/request.rs).
const GRPC_STATUS_NAMES = [
  'OK',
  'CANCELLED',
  'UNKNOWN',
  'INVALID_ARGUMENT',
  'DEADLINE_EXCEEDED',
  'NOT_FOUND',
  'ALREADY_EXISTS',
  'PERMISSION_DENIED',
  'RESOURCE_EXHAUSTED',
  'FAILED_PRECONDITION',
  'ABORTED',
  'OUT_OF_RANGE',
  'UNIMPLEMENTED',
  'INTERNAL',
  'UNAVAILABLE',
  'DATA_LOSS',
  'UNAUTHENTICATED',
]
const GRPC_SERVER_ERRORS = new Set([2, 4, 12, 13, 14, 15])

/**
 * Builds the process's request tracing at bootstrap and registers the context
 * the Coral gRPC transport reads. Export starts only when the operator sets an
 * OTLP endpoint. Without one, Reef opens no spans and a valid inbound
 * `traceparent` passes through unchanged, so Coral never names a parent that
 * no backend receives.
 *
 * Returns `traceRequest`, which runs `respond` in one request's span and ends
 * the span when `respond` settles; `flush`, which sends finished spans; and
 * `shutdown`, which flushes and stops tracing. `requestInstrumentation` names
 * the span.
 */
export function createRequestTracing({
  env = process.env,
  spanExporter = configuredSpanExporter(env),
} = {}) {
  const provider = spanExporter && spanProvider(env, spanExporter)
  // Baggage stays unread because this is the only registered propagator.
  propagation.setGlobalPropagator(new W3CTraceContextPropagator())
  context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable())
  if (provider) {
    diag.setLogger(new DiagConsoleLogger(), DiagLogLevel.WARN)
    trace.setGlobalTracerProvider(provider)
  }

  return {
    async traceRequest(request, respond) {
      if (UNTRACED_PATHS.has(new URL(request.url).pathname)) return respond()
      const parent = remoteParent(request)
      // Without a provider this tracer's spans record nothing and keep their parent.
      const span = trace.getTracer(TRACER_NAME).startSpan(
        spanName(request.method),
        {
          attributes: { 'http.request.method': safeMethod(request.method) },
          kind: SpanKind.SERVER,
        },
        parent,
      )
      try {
        return await context.with(trace.setSpan(parent, span), respond)
      } catch (error) {
        // An aborted request's caller left, so the failure is theirs.
        if (!request.signal.aborted) {
          // A failure after the handler answers is a body that broke off.
          const answered = span.attributes?.['http.response.status_code'] !== undefined
          failSpan(span, answered ? 'response_body' : '_OTHER')
        }
        throw error
      } finally {
        span.end()
      }
    },
    async flush() {
      await provider?.forceFlush().catch(warnFlushFailed)
    },
    async shutdown() {
      await provider?.shutdown().catch(warnFlushFailed)
      diag.disable()
      trace.disable()
      context.disable()
      propagation.disable()
    },
  }
}

// A collector that refuses the last batch must not fail a clean exit.
function warnFlushFailed(error) {
  diag.warn(`Reef could not flush its traces: ${error.message}`)
}

function configuredSpanExporter(env) {
  if (env.OTEL_SDK_DISABLED?.trim().toLowerCase() === 'true') return undefined
  if (env.OTEL_TRACES_EXPORTER?.trim().toLowerCase() === 'none') return undefined
  const endpoint = firstSet(env, [
    'OTEL_EXPORTER_OTLP_TRACES_ENDPOINT',
    'OTEL_EXPORTER_OTLP_ENDPOINT',
  ])
  if (!endpoint) return undefined
  // The exporter would replace an endpoint it cannot parse with localhost. These
  // errors name the variable alone, because an endpoint can hold a credential.
  if (!isHttpUrl(env[endpoint])) throw new Error(`${endpoint} must be an http or https URL`)
  const protocol = firstSet(env, [
    'OTEL_EXPORTER_OTLP_TRACES_PROTOCOL',
    'OTEL_EXPORTER_OTLP_PROTOCOL',
  ])
  if (protocol && env[protocol].trim() !== 'http/protobuf')
    throw new Error(`${protocol} must be http/protobuf`)
  // The exporter reads its endpoint, headers and timeout from the environment.
  return new OTLPTraceExporter()
}

function firstSet(env, names) {
  return names.find((name) => env[name]?.trim())
}

function isHttpUrl(text) {
  const protocol = URL.parse(text)?.protocol
  return protocol === 'http:' || protocol === 'https:'
}

function spanProvider(env, exporter) {
  return new TracerProvider({
    resource: resourceFromAttributes({ 'service.name': 'ui' }).merge(
      detectResources({ detectors: [envDetector] }),
    ),
    sampler: configuredSampler(env),
    spanProcessors: [new BatchSpanProcessor({ exporter })],
  })
}

// The SDK leaves the sampling variables to its caller. A malformed value fails
// startup: a silent default could export every trace, or none.
function configuredSampler(env) {
  const name = env.OTEL_TRACES_SAMPLER?.trim() || 'parentbased_always_on'
  const samplers = {
    always_off: () => new AlwaysOffSampler(),
    always_on: () => new AlwaysOnSampler(),
    parentbased_always_off: () => new ParentBasedSampler({ root: new AlwaysOffSampler() }),
    parentbased_always_on: () => new ParentBasedSampler({ root: new AlwaysOnSampler() }),
    parentbased_traceidratio: () =>
      new ParentBasedSampler({ root: new TraceIdRatioBasedSampler(sampleRatio(env)) }),
    traceidratio: () => new TraceIdRatioBasedSampler(sampleRatio(env)),
  }
  if (!Object.hasOwn(samplers, name))
    throw new Error(`OTEL_TRACES_SAMPLER must be one of ${Object.keys(samplers).join(', ')}`)
  return samplers[name]()
}

function sampleRatio(env) {
  const text = env.OTEL_TRACES_SAMPLER_ARG?.trim() || '1'
  const ratio = Number(text)
  if (ratio >= 0 && ratio <= 1) return ratio
  throw new Error('OTEL_TRACES_SAMPLER_ARG must be a number from 0 to 1')
}

// Only `traceparent` crosses into the trace. `tracestate` is the caller's text,
// and an SDK span would export it. The sampled bit is the only flag kept, because
// the propagator cannot write the higher bits back as two hex digits.
function remoteParent(request) {
  const header = request.headers.get('traceparent')
  const parent = header ? parseTraceParent(header) : null
  if (!parent) return ROOT_CONTEXT
  return trace.setSpanContext(ROOT_CONTEXT, {
    ...parent,
    isRemote: true,
    traceFlags: parent.traceFlags & TraceFlags.SAMPLED,
  })
}

/**
 * The React Router server instrumentation. It names the active request's span
 * and opens a span for each loader and action. It holds no state, so it works
 * from the app build's own copy of this module.
 */
export const requestInstrumentation = {
  handler(handler) {
    handler.instrument({ request: recordResponse })
  },
  route(route) {
    route.instrument({
      action: (run) => traceRouteFunction(`action ${route.id}`, run),
      loader: (run) => traceRouteFunction(`loader ${route.id}`, run),
    })
  },
}

/**
 * The Connect interceptor that opens a client span for a Coral call and sends
 * it as Coral's parent. A streaming call's span ends with its stream.
 */
export const traceCoralCall = (next) => async (call) => {
  const method = `${call.service.typeName}/${call.method.name}`
  const span = startChildSpan(method, {
    attributes: { 'rpc.method': method, 'rpc.system.name': 'grpc' },
    kind: SpanKind.CLIENT,
  })
  if (!span) return next(call)
  propagation.inject(trace.setSpan(context.active(), span), call.header, headerSetter)
  try {
    const response = await next(call)
    if (response.stream) return { ...response, message: endingWith(response.message, span) }
    endCoralCall(span)
    return response
  } catch (error) {
    endCoralCall(span, error)
    throw error
  }
}

const headerSetter = { set: (headers, name, value) => headers.set(name, value) }

// A health probe has no request span, and it must not open a trace of its own.
function startChildSpan(name, options) {
  return trace.getActiveSpan() && trace.getTracer(TRACER_NAME).startSpan(name, options)
}

async function* endingWith(messages, span) {
  let failure
  try {
    yield* messages
  } catch (error) {
    failure = error
    throw error
  } finally {
    endCoralCall(span, failure)
  }
}

function endCoralCall(span, error) {
  if (error && !(error instanceof ConnectError)) failSpan(span, '_OTHER')
  else {
    const code = error?.code ?? 0
    const status = GRPC_STATUS_NAMES[code] ?? 'UNKNOWN'
    span.setAttribute('rpc.response.status_code', status)
    if (GRPC_SERVER_ERRORS.has(code)) failSpan(span, status)
  }
  span.end()
}

async function traceRouteFunction(name, run) {
  const span = startChildSpan(name)
  if (!span) {
    await run()
    return
  }
  try {
    const { error, status } = await context.with(trace.setSpan(context.active(), span), run)
    if (status === 'error') recordRouteFailure(span, error)
  } finally {
    span.end()
  }
}

// A thrown redirect or 404 is a route's normal answer.
function recordRouteFailure(span, error) {
  if (!(error instanceof Response)) failSpan(span, '_OTHER')
  else if (error.status >= 500) failSpan(span, String(error.status))
}

// React Router has matched the request by the time its handler returns.
async function recordResponse(handleRequest, { request }) {
  const { meta, statusCode } = await handleRequest()
  const span = trace.getActiveSpan()
  if (!span) return
  span.setAttribute('http.response.status_code', statusCode)
  if (meta?.pattern) {
    // React Router leaves the leading slash off a route's pattern.
    const route = `/${meta.pattern.replace(/^\//, '')}`
    span.setAttribute('http.route', route)
    span.updateName(`${spanName(request.method)} ${route}`)
  }
  if (statusCode >= 500) failSpan(span, String(statusCode))
}

function failSpan(span, errorType) {
  span.setAttribute('error.type', errorType)
  span.setStatus({ code: SpanStatusCode.ERROR })
}

function safeMethod(method) {
  return STANDARD_METHODS.has(method) ? method : '_OTHER'
}

function spanName(method) {
  return STANDARD_METHODS.has(method) ? method : 'HTTP'
}
