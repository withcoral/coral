import { create } from '@bufbuild/protobuf'

import { WorkspaceSchema } from '@/generated/coral/v1/resources_pb'
import type { Workspace } from '@/generated/coral/v1/resources_pb'
import { CreateWorkspaceRequestSchema } from '@/generated/coral/v1/workspaces_pb'
import type { WorkspaceMembership } from '@/generated/coral/v1/workspaces_pb'
import { workspaceClientForRequest } from '@/lib/coral-request.server'

export async function listWorkspacesForRequest(request: Request): Promise<WorkspaceMembership[]> {
  const response = await workspaceClientForRequest(request).listWorkspaces({})
  return response.memberships
}

export async function firstWorkspaceForRequest(request: Request): Promise<Workspace> {
  const [membership] = await listWorkspacesForRequest(request)
  if (membership?.workspace) return membership.workspace

  throw new Response('No Coral workspace is configured.', {
    status: 404,
    statusText: 'Workspace Not Found',
  })
}

export async function createWorkspaceForRequest(
  request: Request,
  name: string,
): Promise<Workspace> {
  const response = await workspaceClientForRequest(request).createWorkspace(
    create(CreateWorkspaceRequestSchema, {
      workspace: create(WorkspaceSchema, { name }),
    }),
  )
  if (response.workspace) return response.workspace

  throw new Error('Coral did not return the created workspace')
}
