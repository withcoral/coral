import { create } from '@bufbuild/protobuf'
import { expect, it } from 'vitest'
import {
  WorkspaceRole,
  WorkspaceSchema,
  WorkspaceState,
} from '@/generated/coral/management/v1/workspace_pb'
import { sidebarWorkspaceView, workspaceView } from './management-views.server'

const workspaceUrl = 'https://mcp.example.com/workspaces/00000000-0000-4000-8000-000000000001/mcp'

it.each([
  ['a deployment with sign-in', 'https://mcp.example.com/mcp', 'https://mcp.example.com/mcp'],
  ['local mode, which has no sign-in at /mcp', '', workspaceUrl],
])(
  'connects a client at the URL %s offers, while the sidebar keeps the Workspace URL',
  (_case, organizationMcpEndpoint, connectEndpoint) => {
    const workspace = create(WorkspaceSchema, {
      name: 'workspaces/00000000-0000-4000-8000-000000000001',
      state: WorkspaceState.ACTIVE,
      effectiveRole: WorkspaceRole.MEMBER,
      mcpEndpoint: workspaceUrl,
      organizationMcpEndpoint,
    })
    expect(workspaceView(workspace).connectEndpoint).toBe(connectEndpoint)
    expect(sidebarWorkspaceView(workspace).mcpEndpoint).toBe(workspaceUrl)
  },
)
