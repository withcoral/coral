import type { Interceptor } from '@connectrpc/connect'
import type { SpanExporter } from '@opentelemetry/sdk-trace'
import type { ServerInstrumentation } from 'react-router'

export interface RequestTracing {
  traceRequest(request: Request, respond: () => unknown): Promise<unknown>
  flush(): Promise<void>
  shutdown(): Promise<void>
}

export function createRequestTracing(options?: {
  env?: Record<string, string | undefined>
  spanExporter?: SpanExporter
}): RequestTracing

export const requestInstrumentation: ServerInstrumentation

export const traceCoralCall: Interceptor

export function traceLoaderStep<T>(name: string, work: () => Promise<T>): Promise<T>
