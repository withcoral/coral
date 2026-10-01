import { assertCoralReady } from '@/lib/readiness.server'

import type { Route } from './+types/readyz'

/** Readiness reports Coral's unauthenticated engine-health signal. */
export async function loader({ request }: Route.LoaderArgs): Promise<Response> {
  await assertCoralReady(request)
  return Response.json({ coral: 'reachable', status: 'ok' })
}
