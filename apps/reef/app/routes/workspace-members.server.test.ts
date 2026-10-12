import { ConnectError, Code } from '@connectrpc/connect'
import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import { action, loader } from './workspace-members'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))
const workspace = '00000000-0000-4000-8000-000000000001'
const requestId = '00000000-0000-4000-8000-0000000000aa'
const origin = 'http://127.0.0.1:3000'
const url = `${origin}/workspaces/${workspace}/members`
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
const person = (id: string, email: string, enabled = true) => ({
  enabled,
  profile: { user: `users/${id}`, displayName: id, email },
})
let added: { email: string; requestId: string }[]
let refuse: Set<string>
let listOrganization: () => { memberships: ReturnType<typeof person>[] }

beforeEach(() => {
  added = []
  refuse = new Set()
  listOrganization = () => ({
    memberships: [
      person('ada', 'ada@example.com'),
      person('ben', 'ben@example.com'),
      person('cy', 'cy@example.com', false),
      person('dee', ''),
      person('eli', 'eli@example.com'),
    ],
  })
  coral.transport = fakeManagementTransport({
    workspaces: {
      getWorkspace: ({ name }) => ({
        name,
        effectiveRole: WorkspaceRole.OWNER,
        state: WorkspaceState.ACTIVE,
      }),
    },
    memberships: {
      // Ada is on the first page of members and Ben on the second.
      listWorkspaceMemberships: ({ pageToken }) => {
        const id = pageToken ? 'ben' : 'ada'
        return {
          memberships: [
            {
              name: `workspaces/${workspace}/memberships/${id}`,
              user: `users/${id}`,
              role: WorkspaceRole.MEMBER,
            },
          ],
          nextPageToken: pageToken ? '' : 'page-2',
        }
      },
      listWorkspaceOwners: () => ({
        owners: [
          { user: 'users/ada', enabled: true },
          { user: 'users/cy', enabled: false },
        ],
      }),
      // Ada is the last owner.
      deleteWorkspaceMembership: () => {
        throw new ConnectError('LAST_WORKSPACE_OWNER', Code.FailedPrecondition)
      },
      updateWorkspaceMembership: () => {
        throw new ConnectError('LAST_WORKSPACE_OWNER', Code.FailedPrecondition)
      },
      createWorkspaceMembership: (request) => {
        const { email } = request
        if (refuse.has(email)) throw new ConnectError('exists', Code.AlreadyExists)
        added.push({ email, requestId: request.requestId })
        return { profile: { displayName: email.split('@')[0] } }
      },
    },
    organizationMemberships: {
      listOrganizationMemberships: () => listOrganization(),
    },
  })
})

// A signed-in deployment: a local one has no members page.
const load = () => loader(authRouteTestArgs(new Request(url), { workspace }))
const add = (...emails: string[]) => {
  const form = new FormData()
  form.set('intent', 'addMember')
  form.set('role', 'member')
  form.set('requestId', requestId)
  for (const email of emails) form.append('email', email)
  return action(
    authRouteTestArgs(new Request(url, { method: 'POST', headers: { origin }, body: form }), {
      workspace,
    }),
  )
}

const change = (fields: Record<string, string>) => {
  const form = new FormData()
  form.set('name', `workspaces/${workspace}/memberships/ada`)
  form.set('etag', 'etag')
  for (const [name, value] of Object.entries(fields)) form.set(name, value)
  return action(
    authRouteTestArgs(new Request(url, { method: 'POST', headers: { origin }, body: form }), {
      workspace,
    }),
  )
}

// Catches: a disabled owner reaching the page as enabled, so the members list
// and the owner directory cannot mark them.
it('tells the page which owners are disabled', async () => {
  const result = await load()
  expect(result.data.owners.map((o) => [o.user, o.enabled])).toEqual([
    ['users/ada', true],
    ['users/cy', false],
  ])
})

// Catches: the last owner told only that "the current settings do not allow
// this change", with no way to know they must assign another owner.
it.each([
  ['leaving', { intent: 'leave' }],
  ['removal', { intent: 'membership', operation: 'remove' }],
  ['demotion', { intent: 'membership', operation: 'update', role: 'member' }],
])('explains why Coral refuses the %s of the last owner', async (_, fields) => {
  const result = await change(fields)
  expect(result).toMatchObject({
    init: { status: 409 },
    data: {
      error: 'This workspace must keep one owner. Assign another owner first.',
    },
  })
})

// Catches: offering people who are already members on any page, disabled, or
// have no email to add them by.
it('offers enabled organization members with an email who are not members yet', async () => {
  const result = await load()
  expect(result.data.candidates?.map((p) => p.email)).toEqual(['eli@example.com'])
})

// Catches: a workspace owner who cannot list the organization losing the page.
it('falls back to typed emails when you cannot list the organization', async () => {
  listOrganization = () => {
    throw new ConnectError('denied', Code.PermissionDenied)
  }
  const result = await load()
  expect(result.data.candidates).toBeNull()
})

// Catches: one idempotency key shared across several adds, which the server
// would treat as one request, or keys that change when the form is resent.
it('gives each added person a stable request ID of their own', async () => {
  await add('ben@example.com', 'cy@example.com')
  const first = added.map((a) => a.requestId)
  added = []
  await add('ben@example.com', 'cy@example.com')
  expect(added.map((a) => a.requestId)).toEqual(first)
  expect(new Set(first).size).toBe(2)
  for (const id of first) expect(id).toMatch(UUID)

  added = []
  await add('ben@example.com')
  expect(added).toEqual([{ email: 'ben@example.com', requestId }])
})

it.each([
  {
    name: 'everyone added',
    emails: ['ben@example.com', 'cy@example.com'],
    refused: [],
    expected: { error: null, message: '2 people added to the workspace.' },
  },
  {
    name: 'one refused of two',
    emails: ['ben@example.com', 'cy@example.com'],
    refused: ['cy@example.com'],
    expected: {
      error: expect.stringMatching(
        /^Coral added ben\. cy@example\.com: Coral could not add this person\. This person is already/,
      ),
      refused: ['cy@example.com'],
    },
  },
  {
    name: 'the only person refused',
    emails: ['cy@example.com'],
    refused: ['cy@example.com'],
    expected: {
      error: expect.stringMatching(
        /^Coral could not add this person\. This person is already a member/,
      ),
      refused: ['cy@example.com'],
    },
  },
])('reports $name and which people to retry', async ({ emails, refused, expected }) => {
  refuse = new Set(refused)
  const result = await add(...emails)
  expect(result).toMatchObject({ data: expected })
  expect(added.map((a) => a.email)).toEqual(emails.filter((e) => !refuse.has(e)))
})
