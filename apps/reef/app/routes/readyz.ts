import { assertCoralReady } from '@/lib/readiness.server'

import type { Route } from './+types/readyz'

/** Readiness proves Reef can complete a real native gRPC request to Coral. */
export async function loader({ request }: Route.LoaderArgs): Promise<Response> {
  await assertCoralReady(request)
  return Response.json({ coral: 'reachable', status: 'ok' })
}
