import { Code, ConnectError } from '@connectrpc/connect'
import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import { action, loader } from './organization-workspaces'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))
const origin = 'http://127.0.0.1:3000'
const url = `${origin}/organization/workspaces`
const name = 'workspaces/00000000-0000-4000-8000-000000000001'
const requestId = '00000000-0000-4000-8000-0000000000aa'
const member = (id: string, enabled = true) => ({
  name: `organizationMemberships/${id}`,
  enabled,
  profile: { user: `users/${id}`, displayName: id },
})
let calls: unknown[]
let refuse: ConnectError | null
/** Refuses only an assignment of this owner. */
let refuseOwner: string | null

beforeEach(() => {
  calls = []
  refuse = null
  refuseOwner = null
  const mutation = (method: string) => (request: object) => {
    if (refuse) throw refuse
    // Only the fields the form set, without the message's own metadata.
    calls.push([method, Object.fromEntries(Object.entries(request).filter(([k]) => k[0] !== '$'))])
    return {}
  }
  coral.transport = fakeManagementTransport({
    organizationMemberships: {
      listOrganizationMemberships: () => ({ memberships: [member('ada'), member('cy', false)] }),
      getCurrentOrganizationMembership: () => member('ada'),
    },
    organizationWorkspaces: {
      listOrganizationWorkspaces: ({ pageToken }) => {
        if (refuse) throw refuse
        return {
          workspaces: [
            {
              name,
              displayName: 'Research',
              state: pageToken ? WorkspaceState.ACTIVE : WorkspaceState.ARCHIVED,
              etag: 'v1',
              owners: [
                {
                  user: 'users/cy',
                  enabled: false,
                  profile: { user: 'users/cy', displayName: 'Cy' },
                },
              ],
            },
          ],
          nextPageToken: pageToken ? '' : 'p2',
        }
      },
      assignWorkspaceOwner: (request) => {
        if (request.owner === refuseOwner)
          throw new ConnectError('OWNER_TARGET_NOT_ENABLED', Code.FailedPrecondition)
        return mutation('assign')(request)
      },
      archiveOrganizationWorkspace: mutation('archive'),
      restoreOrganizationWorkspace: mutation('restore'),
    },
  })
})

const load = (search = '') => loader(authRouteTestArgs(new Request(url + search), {}, null))
const submit = (fields: Record<string, string | string[]>) => {
  const form = new FormData()
  form.set('name', name)
  form.set('etag', 'v1')
  form.set('requestId', requestId)
  for (const [key, value] of Object.entries(fields)) {
    form.delete(key)
    for (const v of [value].flat()) form.append(key, v)
  }
  return action(
    authRouteTestArgs(
      new Request(url, { method: 'POST', headers: { origin }, body: form }),
      {},
      null,
    ),
  )
}
const thrown = (promise: Promise<unknown>) =>
  promise.then(
    () => undefined,
    (error: unknown) => error as Response,
  )

// Catches: an archived workspace shown as active, a disabled owner shown as
// enabled, or a disabled member offered as a new owner.
it('lists a workspace with its state, its owners and who can own it', async () => {
  const result = await load()
  expect(result.data).toMatchObject({
    workspaces: [
      {
        name,
        displayName: 'Research',
        state: 'archived',
        etag: 'v1',
        owners: [{ user: 'users/cy', enabled: false, profile: { name: 'Cy' } }],
      },
    ],
    candidates: [{ name: 'organizationMemberships/ada' }],
    currentUser: 'users/ada',
    nextPage: 'p2',
  })
  expect((await load('?page=p2')).data.workspaces[0].state).toBe('active')
})

// Catches: an organization member who opens the URL getting an outage page,
// or any data, instead of a permission error.
it('gives someone who is not an organization owner a permission error', async () => {
  refuse = new ConnectError('denied', Code.PermissionDenied)
  expect((await thrown(load()))?.status).toBe(403)
})

// Catches: an action sent to the wrong method, or without the owner, etag or
// request ID that makes it safe to repeat.
it.each([
  [
    'an owner assignment',
    { intent: 'assignOwner', owner: 'organizationMemberships/ada' },
    ['assign', { name, owner: 'organizationMemberships/ada', requestId }],
    'Owner assigned.',
  ],
  [
    'an archive',
    { intent: 'archiveWorkspace' },
    ['archive', { name, etag: 'v1' }],
    'Workspace archived.',
  ],
  [
    'a restore',
    { intent: 'restoreWorkspace' },
    ['restore', { name, etag: 'v1' }],
    'Workspace restored. Other access restrictions still apply.',
  ],
])('applies %s', async (_, fields, call, message) => {
  const result = await submit(fields)
  expect(result).toMatchObject({ data: { error: null, message } })
  expect(calls).toEqual([call])
})

// Catches: several owners sent under one request ID, so Coral applies only the
// first, or a resubmitted form that assigns with new IDs.
it('assigns several owners, each with its own stable request ID', async () => {
  const owners = ['organizationMemberships/ada', 'organizationMemberships/bo']
  const result = await submit({ intent: 'assignOwner', owner: owners })
  expect(result).toMatchObject({ data: { error: null, message: '2 owners assigned.' } })
  const ids = calls.map((c) => (c as [string, { requestId: string }])[1].requestId)
  expect(calls.map((c) => (c as [string, { owner: string }])[1].owner)).toEqual(owners)
  expect(new Set([...ids, requestId]).size).toBe(3)
  calls = []
  await submit({ intent: 'assignOwner', owner: owners })
  expect(calls.map((c) => (c as [string, { requestId: string }])[1].requestId)).toEqual(ids)
})

// Catches: a partial assignment that hides who Coral refused, so a retry
// cannot keep only them.
it('returns the refused owners when only some assignments apply', async () => {
  refuseOwner = 'organizationMemberships/bo'
  const result = await submit({
    intent: 'assignOwner',
    owner: ['organizationMemberships/ada', 'organizationMemberships/bo'],
  })
  expect(result).toMatchObject({
    init: { status: 409 },
    data: {
      error:
        'Coral could not assign 1 of 2 owners. This person does not have an enabled organization membership. Choose another person.',
      refused: ['organizationMemberships/bo'],
    },
  })
  expect(calls.map((c) => (c as [string, { owner: string }])[1].owner)).toEqual([
    'organizationMemberships/ada',
  ])
})

// Catches: a refusal that tells an organization owner to use workspace
// settings they cannot open, or that hides why the person cannot be an owner.
it.each([
  ['WORKSPACE_ARCHIVED', 'This workspace is archived. Restore it before you assign an owner.'],
  [
    'OWNER_TARGET_NOT_ENABLED',
    'This person does not have an enabled organization membership. Choose another person.',
  ],
])('explains the refusal %s in the form', async (reason, error) => {
  refuse = new ConnectError(reason, Code.FailedPrecondition)
  const result = await submit({ intent: 'assignOwner', owner: 'organizationMemberships/ada' })
  expect(result).toMatchObject({ init: { status: 409 }, data: { error } })
})

// Catches: a forged form reaching Coral with an unknown action or a target
// that is not a workspace or an organization member.
it.each([
  ['an unknown action', { intent: 'deleteWorkspace' }],
  ['an action named like an inherited object key', { intent: 'constructor' }],
  ['a target that is not a workspace', { intent: 'archiveWorkspace', name: 'users/ada' }],
  ['an owner that is not an organization member', { intent: 'assignOwner', owner: 'users/ada' }],
  [
    'a batch with one owner that is not an organization member',
    { intent: 'assignOwner', owner: ['organizationMemberships/ada', 'users/bo'] },
  ],
  ['an assignment with no owner', { intent: 'assignOwner' }],
])('refuses %s', async (_, fields) => {
  expect((await thrown(submit(fields)))?.status).toBe(400)
  expect(calls).toEqual([])
})
