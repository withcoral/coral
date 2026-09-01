import type { Route } from './+types/index'

import { replace } from 'react-router'

import { requestAuthContext } from '@/auth/server-context'
import { redirectToWorkspaceTraces } from '@/lib/workspace-redirect.server'
import { listWorkspacesForRequest, pickImpliedWorkspace } from '@/lib/workspaces.server'
import { routePath } from '@/routing/routemap'

export async function loader({ context, request }: Route.LoaderArgs) {
  const accessToken = context.get(requestAuthContext).accessToken
  const workspace = pickImpliedWorkspace(await listWorkspacesForRequest(request, accessToken))
  // Onboarding no longer gates the app, but it still holds the only
  // workspace-creation form, so a caller who belongs to no workspace goes there
  // rather than into the 404 an implied workspace would raise.
  if (!workspace) return replace(routePath('onboarding'))

  return redirectToWorkspaceTraces(request, workspace)
}

export default function AppIndex() {
  return null
}
