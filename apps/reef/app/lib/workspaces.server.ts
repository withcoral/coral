import { create } from '@bufbuild/protobuf'

import { WorkspaceSchema } from '@/generated/coral/v1/resources_pb'
import type { Workspace } from '@/generated/coral/v1/resources_pb'
import { CreateWorkspaceRequestSchema } from '@/generated/coral/v1/workspaces_pb'
import type { WorkspaceMembership } from '@/generated/coral/v1/workspaces_pb'
import { workspaceClientForRequest } from '@/lib/coral-request.server'

/**
 * Memberships the current user holds, each carrying the workspace and the caller's role in it.
 * Callers that only navigate should derive `membership.workspace` where they need it, so the role
 * stays available to policy-aware surfaces.
 */
export async function listWorkspacesForRequest(request: Request): Promise<WorkspaceMembership[]> {
  const response = await workspaceClientForRequest(request).listWorkspaces({})
  return response.memberships
}

export async function firstWorkspaceForRequest(request: Request): Promise<Workspace> {
  // A membership without a workspace resource has nothing to navigate to. Skip it rather than
  // inventing a workspace the server never sent, and keep looking for one it did send.
  for (const { workspace } of await listWorkspacesForRequest(request)) {
    if (workspace) return workspace
  }

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
