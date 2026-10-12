import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import type { RouterContextProvider } from 'react-router'

import {
  HealthCheckRequestSchema,
  HealthCheckResponse_ServingStatus,
} from '@/generated/grpc/health/v1/health_pb'

import { healthClient } from './coral-request.server'
import { runtimeConfigContext } from './runtime-config.server'

const CORAL_READINESS_SERVICE = 'coral.readiness'

export async function assertCoralReady(
  request: Request,
  context: Readonly<RouterContextProvider>,
): Promise<void> {
  // Probe the Coral endpoint that management requests use.
  const health = healthClient(context.get(runtimeConfigContext).endpoint)
  const response = await health.check(
    create(HealthCheckRequestSchema, { service: CORAL_READINESS_SERVICE }),
    { signal: request.signal, timeoutMs: 2000 },
  )

  if (response.status !== HealthCheckResponse_ServingStatus.SERVING) {
    throw new ConnectError('Coral reported that its engine is not ready', Code.Unavailable)
  }
}
