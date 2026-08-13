import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'

import {
  HealthCheckRequestSchema,
  HealthCheckResponse_ServingStatus,
} from '@/generated/grpc/health/v1/health_pb'

import { healthClientForRequest } from './coral-request.server'

const CORAL_READINESS_SERVICE = 'coral.readiness'

export async function assertCoralReady(request: Request): Promise<void> {
  const health = healthClientForRequest(request)
  const response = await health.check(
    create(HealthCheckRequestSchema, { service: CORAL_READINESS_SERVICE }),
    { signal: request.signal },
  )

  if (response.status !== HealthCheckResponse_ServingStatus.SERVING) {
    throw new ConnectError('Coral reported that its engine is not ready', Code.Unavailable)
  }
}
