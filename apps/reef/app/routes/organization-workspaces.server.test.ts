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
let refuseOwner: { owner: string; error: ConnectError } | null
/** Every owner the action asked Coral to assign, refused or not. */
let tried: string[]

beforeEach(() => {
  calls = []
  refuse = null
  refuseOwner = null
  tried = []
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
        tried.push(request.owner)
        if (request.owner === refuseOwner?.owner) throw refuseOwner.error
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

// Catches: an action sent to the wrong method, or without the etag that makes
// it safe to repeat.
it.each([
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

const ada = 'organizationMemberships/ada'
const bo = 'organizationMemberships/bo'
const cy = 'organizationMemberships/cy'
const assigned = () => calls.map((c) => (c as [string, { owner: string }])[1].owner)
const requestIds = () => calls.map((c) => (c as [string, { requestId: string }])[1].requestId)

// Catches: owners sent under one request ID, so Coral applies only the first;
// a key shared by every workspace row; a resubmitted form that assigns with
// new IDs; or one person assigned twice.
it.each([
  ['one owner', [ada], [ada], 'Owner assigned.'],
  ['the same owner twice', [ada, ada], [ada], 'Owner assigned.'],
  ['several owners', [ada, bo], [ada, bo], '2 owners assigned.'],
])('assigns %s, each with its own stable request ID', async (_, owners, expected, message) => {
  const result = await submit({ intent: 'assignOwner', owner: owners })
  expect(result).toMatchObject({ data: { error: null, message } })
  expect(calls).toEqual(
    expected.map((owner) => ['assign', { name, owner, requestId: expect.any(String) }]),
  )
  const ids = requestIds()
  expect(new Set([...ids, requestId]).size).toBe(expected.length + 1)
  calls = []
  await submit({ intent: 'assignOwner', owner: owners })
  expect(requestIds()).toEqual(ids)
  calls = []
  await submit({
    intent: 'assignOwner',
    owner: owners,
    name: 'workspaces/00000000-0000-4000-8000-000000000002',
  })
  expect(requestIds().some((id) => ids.includes(id))).toBe(false)
})

// Catches: a partial assignment that hides who Coral did not assign, so a
// retry cannot keep only them; or a batch-wide failure sent again for every
// remaining owner.
it.each([
  [
    'goes on past a person Coral refuses',
    [bo, ada, cy],
    new ConnectError('OWNER_TARGET_NOT_ENABLED', Code.FailedPrecondition),
    [bo, ada, cy],
    [bo],
    409,
    'Coral could not assign 1 of 3 owners. This person does not have an enabled organization membership. Choose another person.',
  ],
  [
    'stops when the workspace is archived',
    [ada, bo, cy],
    new ConnectError('WORKSPACE_ARCHIVED', Code.FailedPrecondition),
    [ada, bo],
    [bo, cy],
    409,
    'Coral could not assign 2 of 3 owners. This workspace is archived. Restore it before you assign an owner.',
  ],
  [
    'stops when Coral is not available',
    [bo, ada],
    new ConnectError('down', Code.Unavailable),
    [bo],
    [bo, ada],
    503,
    'Coral could not assign 2 of 2 owners. Coral could not assign this owner. Coral is not available.',
  ],
])('%s', async (_, owners, error, attempted, refused, status, message) => {
  refuseOwner = { owner: bo, error }
  const result = await submit({ intent: 'assignOwner', owner: owners })
  expect(result).toMatchObject({ init: { status }, data: { error: message, refused } })
  expect(tried).toEqual(attempted)
  expect(assigned()).toEqual(attempted.filter((o) => !refused.includes(o)))
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
  [
    'an owner longer than a form field',
    { intent: 'assignOwner', owner: `organizationMemberships/${'a'.repeat(4096)}` },
  ],
  ['a request ID that is not a UUID', { intent: 'assignOwner', owner: ada, requestId: 'x' }],
])('refuses %s', async (_, fields) => {
  expect((await thrown(submit(fields)))?.status).toBe(400)
  expect(calls).toEqual([])
})
