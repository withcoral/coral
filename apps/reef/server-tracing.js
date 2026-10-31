// Server spans for Reef's incoming requests. The Coral gRPC transport injects
// the active context into each call, so Coral's spans become their children.
//
// This module sets the fields Coral's server spans carry (see
// crates/coral-telemetry/src/field.rs): the method, the route template and the
// status. A span never carries headers, bodies, query strings, cookies, full
// URLs or error text, and `baggage` and `tracestate` are never read.
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
// A failure after the handler answers is a body that broke off.
const spansWithResponse = new WeakSet()

/**
 * Builds the process's request tracing at bootstrap and registers the context
 * the Coral gRPC transport reads. Export starts only when the operator sets an
 * OTLP endpoint. Without one, Reef opens no spans and a valid inbound
 * `traceparent` passes through unchanged, so Coral never names a parent that
 * no backend receives.
 *
 * Returns `traceRequest`, which runs `respond` in one request's span and ends
 * the span when `respond` settles; `instrumentation`, the React Router server
 * instrumentation that names the span; and `shutdown`, which flushes finished
 * spans.
 */
export function createRequestTracing({
  env = process.env,
  spanExporter = configuredSpanExporter(env),
} = {}) {
  const provider = spanExporter && spanProvider(env, spanExporter)
  // Baggage stays unread because this is the only registered propagator.
  propagation.setGlobalPropagator(new W3CTraceContextPropagator())
  context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable())
  if (provider) diag.setLogger(new DiagConsoleLogger(), DiagLogLevel.WARN)
  // Without a provider this tracer's spans record nothing and keep their parent.
  const tracer = provider ? provider.getTracer('reef') : trace.getTracer('reef')

  return {
    instrumentation: {
      handler(handler) {
        handler.instrument({ request: recordResponse })
      },
    },
    async traceRequest(request, respond) {
      if (UNTRACED_PATHS.has(new URL(request.url).pathname)) return respond()
      const parent = remoteParent(request)
      const span = tracer.startSpan(
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
          failSpan(span, spansWithResponse.has(span) ? 'response_body' : '_OTHER')
        }
        throw error
      } finally {
        span.end()
      }
    },
    async shutdown() {
      // A collector that refuses the last batch must not fail a clean exit.
      await provider?.shutdown().catch((error) => {
        diag.warn(`Reef could not flush its traces: ${error.message}`)
      })
      diag.disable()
      context.disable()
      propagation.disable()
    },
  }
}

function configuredSpanExporter(env) {
  if (env.OTEL_SDK_DISABLED?.trim().toLowerCase() === 'true') return undefined
  const endpoint = env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT || env.OTEL_EXPORTER_OTLP_ENDPOINT
  // The exporter reads its endpoint, headers and timeout from the environment.
  return endpoint?.trim() ? new OTLPTraceExporter() : undefined
}

function spanProvider(env, exporter) {
  return new TracerProvider({
    resource: resourceFromAttributes({ 'service.name': 'reef' }).merge(
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

// React Router has matched the request by the time its handler returns.
async function recordResponse(handleRequest, { request }) {
  const { meta, statusCode } = await handleRequest()
  const span = trace.getActiveSpan()
  if (!span) return
  spansWithResponse.add(span)
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
