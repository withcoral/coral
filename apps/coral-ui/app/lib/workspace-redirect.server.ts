import { redirect } from 'react-router'

import type { Workspace } from '@/generated/coral/v1/resources_pb'
import { routePath } from '@/routing/routemap'

export function redirectToWorkspaceTraces(request: Request, workspace: Workspace): Response {
  const search = new URL(request.url).search
  return redirect(`${routePath('workspaceTraces', { workspaceId: workspace.name })}${search}`)
}
