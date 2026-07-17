import type { Route } from './+types/index'

import { redirectToFirstWorkspaceSources } from '@/lib/workspace-redirect.server'

export async function loader({ request }: Route.LoaderArgs) {
  return redirectToFirstWorkspaceSources(request)
}

export default function AppIndex() {
  return null
}
