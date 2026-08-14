import { redirect } from 'react-router'

import { firstWorkspaceForRequest } from '@/lib/workspaces.server'
import { routePath } from '@/routing/routemap'

export async function redirectToFirstWorkspaceSources(
  request: Request,
  accessToken: string | null,
): Promise<Response> {
  const workspace = await firstWorkspaceForRequest(request, accessToken)
  const search = new URL(request.url).search
  return redirect(`${routePath('workspaceSources', { workspaceId: workspace.name })}${search}`)
}
