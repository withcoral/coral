import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { fakeManagementTransport } from '@/lib/management.test-support'
import * as organizationMembers from './organization-members'
import * as workspaceMembers from './workspace-members'

const coral = vi.hoisted(() => ({ transport: undefined as unknown, calls: 0 }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))

const workspace = '00000000-0000-4000-8000-000000000001'
const origin = 'http://127.0.0.1:3000'

beforeEach(() => {
  coral.calls = 0
  const called = () => {
    coral.calls += 1
    return {}
  }
  coral.transport = fakeManagementTransport({
    workspaces: { getWorkspace: called },
    memberships: {
      listWorkspaceMemberships: called,
      listWorkspaceOwners: called,
      getWorkspaceMembership: called,
      createWorkspaceMembership: called,
      deleteWorkspaceMembership: called,
    },
    organizationMemberships: {
      listOrganizationMemberships: called,
      getCurrentOrganizationMembership: called,
      deleteOrganizationMembership: called,
    },
  })
})

const post = (path: string, intent: string) => {
  const form = new FormData()
  form.set('intent', intent)
  return new Request(`${origin}${path}`, { method: 'POST', headers: { origin }, body: form })
}
const local = (request: Request) => authRouteTestArgs(request, { workspace }, null)

// Catches: a local deployment, where nobody else can sign in, serving or
// changing people it can never have.
it.each([
  [
    'organization members',
    () => organizationMembers.loader(local(new Request(`${origin}/organization/members`))),
  ],
  [
    'removing an organization member',
    () =>
      organizationMembers.action(local(post('/organization/members', 'removeOrganizationMember'))),
  ],
  [
    'workspace members',
    () => workspaceMembers.loader(local(new Request(`${origin}/workspaces/${workspace}/members`))),
  ],
  [
    'adding a workspace member',
    () => workspaceMembers.action(local(post(`/workspaces/${workspace}/members`, 'addMember'))),
  ],
])('a local deployment has no %s', async (_, run) => {
  const refused = await run().then(
    () => null,
    (error: unknown) => error,
  )
  expect(refused).toBeInstanceOf(Response)
  expect((refused as Response).status).toBe(404)
  expect(coral.calls).toBe(0)
})
