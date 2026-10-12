import { Code, ConnectError } from '@connectrpc/connect'
import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { OrganizationRole } from '@/generated/coral/management/v1/organization_membership_pb'
import { WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import { action, loader } from './organization-members'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))
const origin = 'http://127.0.0.1:3000'
const url = `${origin}/organization/members`
const member = (id: string, enabled = true) => ({
  name: `organizationMemberships/${id}`,
  etag: id,
  role: OrganizationRole.MEMBER,
  enabled,
  profile: { user: `users/${id}`, displayName: id },
})
let removed: { name: string; etag: string; transferTo: string }[]
let refuse: string | null

beforeEach(() => {
  removed = []
  refuse = null
  coral.transport = fakeManagementTransport({
    organizationMemberships: {
      listOrganizationMemberships: () => ({
        memberships: [member('ada'), member('ben'), member('cy', false)],
      }),
      getCurrentOrganizationMembership: () => member('ada'),
      deleteOrganizationMembership: ({ name, etag, transferTo }) => {
        if (refuse) throw new ConnectError(refuse, Code.FailedPrecondition)
        removed.push({ name, etag, transferTo })
        return {}
      },
    },
    organizationWorkspaces: {
      listOrganizationWorkspaces: () => ({
        workspaces: [
          {
            displayName: 'Research',
            state: WorkspaceState.ARCHIVED,
            owners: [{ user: 'users/ben', enabled: true }],
          },
          {
            displayName: 'Operations',
            state: WorkspaceState.ACTIVE,
            owners: [
              { user: 'users/ada', enabled: true },
              { user: 'users/ben', enabled: true },
            ],
          },
        ],
      }),
    },
  })
})

const remove = (fields: Record<string, string>, person = 'ben') => {
  const form = new FormData()
  form.set('intent', 'removeOrganizationMember')
  form.set('name', `organizationMemberships/${person}`)
  form.set('etag', person)
  for (const [name, value] of Object.entries(fields)) form.set(name, value)
  return action(
    // A signed-in deployment: a local one has no members page.
    authRouteTestArgs(new Request(url, { method: 'POST', headers: { origin }, body: form }), {}),
  )
}

// Catches: the removal dialog missing its transfer step for a sole owner, or
// offering a disabled member as the new owner.
it('names the workspaces each member alone owns and who can take them over', async () => {
  const result = await loader(authRouteTestArgs(new Request(url), {}))
  expect(result.data.memberships.map((m) => [m.name, m.soleOwnerOf])).toEqual([
    ['organizationMemberships/ada', []],
    ['organizationMemberships/ben', ['Research']],
    ['organizationMemberships/cy', []],
  ])
  expect(result.data.candidates.map((p) => p.name)).toEqual([
    'organizationMemberships/ada',
    'organizationMemberships/ben',
  ])
})

// Catches: the chosen new owner dropped on the way to Coral, which then
// refuses the removal; or a transfer sent when none was chosen.
// Ben alone owns Research and needs a transfer; Cy owns nothing.
it.each([
  ['a sole owner with the chosen new owner', 'ben', { transferTo: 'organizationMemberships/ada' }],
  ['a member who owns nothing with no transfer', 'cy', {}],
])('removes %s', async (_, person, fields) => {
  const result = await remove(fields, person)
  expect(result).toMatchObject({ data: { error: null } })
  expect(removed).toEqual([
    {
      name: `organizationMemberships/${person}`,
      etag: person,
      transferTo: (fields as { transferTo?: string }).transferTo ?? '',
    },
  ])
})

// Catches: a workspace or owner that changed after the page loaded leaving
// the person with a generic refusal and no next step.
it.each([
  [
    'OWNER_TRANSFER_REQUIRED',
    'This person is the only owner of a workspace. Choose a person to take over their workspaces.',
  ],
  [
    'OWNER_TARGET_NOT_ENABLED',
    'This person does not have an enabled organization membership. Choose another person.',
  ],
])('explains the refusal %s in the form', async (reason, error) => {
  refuse = reason
  const result = await remove({ transferTo: 'organizationMemberships/ada' })
  expect(result).toMatchObject({ init: { status: 409 }, data: { error } })
})

// Catches: a forged form naming something other than an organization member
// as the new owner.
it('refuses a transfer target that is not an organization membership', async () => {
  const thrown = await remove({ transferTo: 'workspaces/x/memberships/ada' }).then(
    () => undefined,
    (error: unknown) => error,
  )
  expect((thrown as Response).status).toBe(400)
  expect(removed).toEqual([])
})
