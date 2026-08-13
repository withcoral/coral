import type { Route } from './+types/healthz'

import { assertReefRuntimeConfig } from '@/lib/runtime-config.server'

export function loader({ request: _request }: Route.LoaderArgs): Response {
  assertReefRuntimeConfig()
  return Response.json({ status: 'ok' })
}
