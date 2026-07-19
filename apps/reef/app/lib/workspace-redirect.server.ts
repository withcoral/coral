import { redirect } from 'react-router'

import { firstWorkspaceForRequest } from '@/lib/workspaces.server'
import { routePath } from '@/routing/routemap'

export async function redirectToFirstWorkspaceSources(request: Request): Promise<Response> {
  const workspace = await firstWorkspaceForRequest(request)
  const search = new URL(request.url).search
  return redirect(`${routePath('workspaceSources', { workspaceId: workspace.name })}${search}`)
}
