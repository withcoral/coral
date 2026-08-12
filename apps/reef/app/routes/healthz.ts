import type { Route } from './+types/healthz'

export function loader({ request: _request }: Route.LoaderArgs): Response {
  return Response.json({ status: 'ok' })
}
