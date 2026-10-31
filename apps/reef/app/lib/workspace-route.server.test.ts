import { expect, it, vi } from 'vitest'

import { authTestContext } from '@/auth/server-context.test-helper'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'

import { fakeManagementTransport } from './management.test-support'
import { authorizedWorkspace } from './workspace-route.server'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('./coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))

const id = '29a180e5-397c-4844-92de-8bbea6066235'

function serveRole(effectiveRole: WorkspaceRole) {
  coral.transport = fakeManagementTransport({
    workspaces: {
      getWorkspace: ({ name }) => ({
        name,
        displayName: 'Finance',
        effectiveRole,
        state: WorkspaceState.ACTIVE,
      }),
    },
  })
}

const load = (options?: { ownerOnly?: boolean }) =>
  authorizedWorkspace(authTestContext(null), id, options)

it('refuses an owner-only page to a member with 403', async () => {
  serveRole(WorkspaceRole.MEMBER)
  const refused = await load({ ownerOnly: true }).catch((error: unknown) => error)
  expect(refused).toBeInstanceOf(Response)
  expect((refused as Response).status).toBe(403)
  expect((refused as Response).headers.get('Cache-Control')).toBe('private, no-store')
})

it.each([
  ['a member on a shared page', WorkspaceRole.MEMBER, {}],
  ['an owner on an owner-only page', WorkspaceRole.OWNER, { ownerOnly: true }],
])('opens the workspace for %s', async (_, role, options) => {
  serveRole(role)
  const { workspace } = await load(options)
  expect(workspace).toMatchObject({ name: `workspaces/${id}`, displayName: 'Finance' })
})
