import { create } from '@bufbuild/protobuf'

import { WorkspaceSchema } from '@/generated/coral/v1/resources_pb'
import type { Workspace } from '@/generated/coral/v1/resources_pb'
import { CreateWorkspaceRequestSchema } from '@/generated/coral/v1/workspaces_pb'
import { workspaceClientForRequest } from '@/lib/coral-request.server'

export async function listWorkspacesForRequest(
  request: Request,
  accessToken: string | null,
): Promise<Workspace[]> {
  const response = await workspaceClientForRequest(request, accessToken).listWorkspaces({})
  // Coral now answers with the caller's memberships. Reef reads only the
  // workspace each one points at; surfacing the role it carries is a separate
  // change to Reef's own contract.
  //
  // A membership without a workspace is a Coral that broke its own contract, so
  // it is reported rather than skipped. Dropping it would shorten the list —
  // and an emptied list reads downstream as "you belong to no workspace", which
  // sends the caller to a 404 about configuration for what is really a
  // malformed response.
  return response.memberships.map((membership) => {
    if (!membership.workspace) {
      throw new Error('Coral returned a workspace membership without a workspace')
    }
    return membership.workspace
  })
}

export const DEFAULT_WORKSPACE_ID = 'default'

/**
 * `ListWorkspaces` makes no promise about order: the server sorts by name, so a
 * workspace named before `default` would otherwise take its place.
 */
export function pickImpliedWorkspace(workspaces: readonly Workspace[]): Workspace | undefined {
  return workspaces.find(({ name }) => name === DEFAULT_WORKSPACE_ID) ?? workspaces[0]
}

export async function impliedWorkspaceForRequest(
  request: Request,
  accessToken: string | null,
): Promise<Workspace> {
  const workspace = pickImpliedWorkspace(await listWorkspacesForRequest(request, accessToken))
  if (workspace) return workspace

  throw new Response('No Coral workspace is configured.', {
    status: 404,
    statusText: 'Workspace Not Found',
  })
}

export async function createWorkspaceForRequest(
  request: Request,
  accessToken: string | null,
  name: string,
): Promise<Workspace> {
  const response = await workspaceClientForRequest(request, accessToken).createWorkspace(
    create(CreateWorkspaceRequestSchema, {
      workspace: create(WorkspaceSchema, { name }),
    }),
  )
  if (response.workspace) return response.workspace

  throw new Error('Coral did not return the created workspace')
}
