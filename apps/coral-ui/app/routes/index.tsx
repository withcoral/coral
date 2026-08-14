import type { Route } from './+types/index'

import { requestAuthContext } from '@/auth/server-context'
import { redirectToFirstWorkspaceTraces } from '@/lib/workspace-redirect.server'

export async function loader({ context, request }: Route.LoaderArgs) {
  return redirectToFirstWorkspaceTraces(request, context.get(requestAuthContext).accessToken)
}

export default function AppIndex() {
  return null
}
