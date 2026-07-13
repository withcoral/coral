import { create } from '@bufbuild/protobuf'

import { WorkspaceSchema, type Workspace } from '@/generated/coral/v1/resources_pb'

export function workspaceFromParams(params: { workspaceId?: string }): Workspace {
  const workspaceId = params.workspaceId
  if (!workspaceId) {
    throw new Response('Missing workspace route parameter.', {
      status: 400,
      statusText: 'Invalid Workspace',
    })
  }
  return create(WorkspaceSchema, { name: workspaceId })
}
