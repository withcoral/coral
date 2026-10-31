import type { Interceptor } from '@connectrpc/connect'
import type { ServerInstrumentation } from 'react-router'

export interface RequestTracing {
  traceRequest(request: Request, respond: () => unknown): Promise<unknown>
  flush(): Promise<void>
  shutdown(): Promise<void>
}

export function createRequestTracing(): RequestTracing

export const requestInstrumentation: ServerInstrumentation

export const traceCoralCall: Interceptor
