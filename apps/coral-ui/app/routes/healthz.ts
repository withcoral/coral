import type { Route } from './+types/healthz'

import { assertCoralUIRuntimeConfig } from '@/lib/runtime-config.server'

export function loader({ request: _request }: Route.LoaderArgs): Response {
  assertCoralUIRuntimeConfig()
  return Response.json({ status: 'ok' })
}
