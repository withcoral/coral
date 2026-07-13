import type { Workspace } from '@/generated/coral/v1/resources_pb'
import { workspaceClientForRequest } from '@/lib/coral-request.server'

export async function listWorkspacesForRequest(request: Request): Promise<Workspace[]> {
  const response = await workspaceClientForRequest(request).listWorkspaces({})
  return response.workspaces
}

export async function firstWorkspaceForRequest(request: Request): Promise<Workspace> {
  const [workspace] = await listWorkspacesForRequest(request)
  if (workspace) return workspace

  throw new Response('No Coral workspace is configured.', {
    status: 404,
    statusText: 'Workspace Not Found',
  })
}
