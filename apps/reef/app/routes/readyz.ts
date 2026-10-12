import { assertCoralReady } from '@/lib/readiness.server'

import type { Route } from './+types/readyz'

/** Reef readiness follows its private Coral dependency, never process liveness. */
export async function loader({ context, request }: Route.LoaderArgs): Promise<Response> {
  try {
    await assertCoralReady(request, context)
    return Response.json({ coral: 'reachable', status: 'ok' })
  } catch {
    return Response.json({ status: 'unavailable' }, { status: 503 })
  }
}
