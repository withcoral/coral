import { redirect } from 'react-router'

import { impliedWorkspaceForRequest } from '@/lib/workspaces.server'
import { routePath } from '@/routing/routemap'

export async function redirectToImpliedWorkspaceTraces(
  request: Request,
  accessToken: string | null,
): Promise<Response> {
  const workspace = await impliedWorkspaceForRequest(request, accessToken)
  const search = new URL(request.url).search
  return redirect(`${routePath('workspaceTraces', { workspaceId: workspace.name })}${search}`)
}
