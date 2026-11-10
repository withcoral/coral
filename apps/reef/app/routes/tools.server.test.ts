import { Code, ConnectError } from '@connectrpc/connect'
import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { AuthoredToolDiagnosticCategory } from '@/generated/coral/management/v1/authored_tool_pb'
import { ToolAccessState } from '@/generated/coral/management/v1/policy_pb'
import { ProviderBlocker, ProviderState } from '@/generated/coral/management/v1/provider_pb'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { SharedToolEventAction } from '@/generated/coral/management/v1/shared_tool_pb'
import { coralRefusal, fakeManagementTransport } from '@/lib/management.test-support'
import { action, loader } from './tools'
import { loader as formerListLoader } from './providers-redirect'
import { loader as selectionLoader } from './tools-selection'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))
vi.mock('@/views/tools/tools-page', () => ({ ToolsPage: () => null }))

const workspace = '00000000-0000-4000-8000-000000000001'
const parent = `workspaces/${workspace}`
const page = `/workspaces/${workspace}`
const builtin = `${parent}/providers/00000000-0000-4000-8000-0000000000c0`
const github = `${parent}/providers/00000000-0000-4000-8000-0000000000a1`
const privateProvider = `${parent}/providers/00000000-0000-4000-8000-0000000000aa`
const privateId = privateProvider.split('/').at(-1)!
const sharedProvider = `${parent}/providers/00000000-0000-4000-8000-0000000000ab`
const remind = `${parent}/sharedTools/00000000-0000-4000-8000-0000000000d1`
const at = (seconds: number) => ({ seconds: BigInt(seconds), nanos: 0 })
const workerId = '00000000-0000-4000-8000-0000000000b1'
const worker = `${parent}/agents/${workerId}`

const toolName = (provider: string, key: string) =>
  `${provider}/tools/${Buffer.from(key).toString('base64url')}`
const row = (provider: string, key: string, relativePath: string, more: object = {}) => ({
  tool: toolName(provider, key),
  relativePath,
  available: true,
  accessState: ToolAccessState.ENABLED,
  ...more,
})

// The Private Tools Coral lists to each role: an Owner reads their own and
// every Agent's, a Member their own. The Tool Key tells apart two at one path.
const OWN_KEY = 'user/people.greet'
const AGENT_KEY = 'agent/b1/people.greet'
const privateRows = {
  [WorkspaceRole.OWNER]: [
    row(privateProvider, OWN_KEY, 'people.greet'),
    row(privateProvider, AGENT_KEY, 'people.greet', {
      agent: worker,
      accessState: ToolAccessState.DISABLED,
    }),
    row(privateProvider, 'user/reports.total', 'reports.total', {
      accessState: ToolAccessState.APPROVAL_REQUIRED,
    }),
  ],
  [WorkspaceRole.MEMBER]: [row(privateProvider, 'user/mine.only', 'mine.only')],
}

let calls: { method: string; request: Record<string, unknown> }[]
const called = (method: string) => calls.filter((call) => call.method === method)

/** Notes each call to a Coral method that a test asserts on. */
const record =
  <Request extends object, Response>(method: string, respond: (request: Request) => Response) =>
  (request: Request) => {
    calls.push({ method, request: { ...request } as Record<string, unknown> })
    return respond(request)
  }

/** A Coral that answers as `role`, with the Private Provider `disabled` or not. */
function serve(role: WorkspaceRole, { disabled = false } = {}) {
  coral.transport = fakeManagementTransport({
    workspaces: {
      getWorkspace: ({ name }) => ({
        name,
        displayName: 'Research',
        effectiveRole: role,
        state: WorkspaceState.ACTIVE,
      }),
    },
    providers: {
      // Coral leaves the Private Provider out of the list.
      listProviders: () => ({
        providers: [
          { name: github, displayName: 'GitHub', namespace: 'github', state: ProviderState.ACTIVE },
          { name: builtin, displayName: 'Coral', builtin: true, state: ProviderState.ACTIVE },
        ],
      }),
      getProvider: record('getProvider', ({ name }: { name: string }) => {
        if (name === sharedProvider)
          return {
            name,
            displayName: 'Shared',
            callRoot: 'coral.shared',
            etag: 'shared-etag',
            state: ProviderState.ACTIVE,
          }
        if (name !== privateProvider) throw new ConnectError('NOT_FOUND', Code.NotFound)
        return {
          name,
          displayName: 'Private',
          callRoot: 'coral.private',
          etag: 'private-etag',
          state: disabled ? ProviderState.DISABLED : ProviderState.ACTIVE,
          blockers: disabled ? [ProviderBlocker.PROVIDER_DISABLED] : [],
        }
      }),
    },
    policies: {
      listToolAccess: ({ parent: provider }) => ({
        tools:
          provider === privateProvider
            ? privateRows[role as keyof typeof privateRows]
            : provider === sharedProvider
              ? [
                  row(sharedProvider, 'shared/1', 'mail.remind', {
                    accessState: ToolAccessState.APPROVAL_REQUIRED,
                  }),
                  row(sharedProvider, 'shared/2', 'tides'),
                ]
              : provider === builtin
                ? [row(builtin, 'search', 'search'), row(builtin, 'addTool', 'addTool')]
                : [
                    row(github, 'issues/list', 'issues.list', { tags: ['issues'] }),
                    row(github, 'issues/greet', 'issues.greet', { tags: ['issues'] }),
                    row(github, 'repos/get', 'repos.get', { available: false }),
                  ],
      }),
      updateToolPolicies: record('updateToolPolicies', () => ({})),
    },
    privateTools: {
      listPrivateTools: () => ({ privateTools: [], privateProvider }),
      getPrivateTool: ({ path, agent }) => {
        // Coral conceals an agent's private tool from everyone but an owner.
        if (
          path !== 'people.greet' ||
          (agent && (agent !== worker || role !== WorkspaceRole.OWNER))
        )
          throw new ConnectError('NOT_FOUND', Code.NotFound)
        return {
          path,
          description: agent ? 'Greet a person, by the worker' : 'Greet a person',
          declaration: 'declare function greet(): string',
          source: 'export default () => "hello"',
          diagnostics: [
            {
              category: AuthoredToolDiagnosticCategory.WARNING,
              code: 6133n,
              message: 'Unused.',
              line: 1n,
            },
          ],
          updateTime: { seconds: 1_700_000_000n, nanos: 0 },
        }
      },
      deletePrivateTool: record(
        'deletePrivateTool',
        ({ path }: { path: string; agent: string }) => {
          if (path !== 'people.greet') throw new ConnectError('NOT_FOUND', Code.NotFound)
          return {}
        },
      ),
    },
    agents: {
      listAgents: record('listAgents', () => ({
        agents: [{ name: worker, displayName: 'Worker' }],
      })),
    },
    sharedTools: {
      // Coral leaves the Shared Provider out of the list too, and names it here.
      listSharedTools: () => ({ sharedTools: [], sharedProvider }),
      getSharedTool: ({ path }) => {
        if (path !== 'mail.remind') throw new ConnectError('NOT_FOUND', Code.NotFound)
        return {
          name: remind,
          path,
          creator: 'users/gone',
          currentRevision: {
            revision: 2n,
            description: 'Remind someone',
            declaration: 'export default function remind(): Promise<string>;',
            source: 'export default async () => "later"',
            publisher: 'users/ada',
            publishTime: at(1_700_000_200),
            restoredFromRevision: 1n,
          },
        }
      },
      listSharedToolRevisions: ({ sharedTool }) => ({
        revisions:
          sharedTool === remind
            ? [
                {
                  revision: 2n,
                  description: 'Remind someone',
                  publisher: 'users/ada',
                  publishTime: at(1_700_000_200),
                  restoredFromRevision: 1n,
                },
                {
                  revision: 1n,
                  description: 'Remind someone, at first',
                  publisher: 'users/gone',
                  publishTime: at(1_700_000_100),
                },
              ]
            : [],
      }),
      listSharedToolEvents: record(
        'listSharedToolEvents',
        ({ sharedTool }: { sharedTool: string }) => ({
          events:
            sharedTool === remind
              ? [
                  {
                    id: 'event-2',
                    action: SharedToolEventAction.PUBLISHED,
                    revision: 2n,
                    actor: 'users/ada',
                    eventTime: at(1_700_000_200),
                  },
                  // An action a newer Coral records, which this Reef does not know.
                  {
                    id: 'event-x',
                    action: 9 as SharedToolEventAction,
                    revision: 1n,
                    actor: 'users/ada',
                  },
                  {
                    id: 'event-1',
                    action: SharedToolEventAction.PUBLISHED,
                    revision: 1n,
                    actor: 'users/gone',
                    eventTime: at(1_700_000_100),
                  },
                ]
              : [],
          nextPageToken: 'more',
        }),
      ),
      removeSharedTool: record(
        'removeSharedTool',
        ({ path, expectedRevision }: { path: string; expectedRevision: bigint }) => {
          if (path === 'mail.send')
            throw coralRefusal(Code.FailedPrecondition, 'SHARED_TOOL_HAS_DEPENDENTS', {
              dependent: 'mail.remind',
            })
          if (path !== 'mail.remind') throw new ConnectError('NOT_FOUND', Code.NotFound)
          if (expectedRevision !== 2n) throw new ConnectError('ABORTED', Code.Aborted)
          return { sharedTool: remind }
        },
      ),
    },
    memberships: {
      // Every member reads the owners. Ada is one; whoever published first is not.
      listWorkspaceOwners: () => ({
        owners: [{ user: 'users/ada', profile: { user: 'users/ada', displayName: 'Ada' } }],
      }),
    },
    accounts: { listUserAccounts: () => ({ accounts: [] }) },
    activity: { summarizeActivity: () => ({}) },
  })
}

beforeEach(() => {
  calls = []
})

const args = (path = 'tools', params: Record<string, string> = {}) =>
  authRouteTestArgs(new Request(`http://reef${page}/${path}`), { workspace, ...params }, null)
const load = async () => (await loader(args() as never))!.data
const groupOf = async (kind: string) =>
  (await load()).providers.find((provider) => provider.kind === kind)!

// Catches: a tool listed under the wrong group, a group whose count is not
// its tools, or a page that leaves out the Shared or the Private Provider,
// which Coral's list does not name.
it('groups every tool under Coral, Shared, each provider and Private, with its count', async () => {
  serve(WorkspaceRole.OWNER)
  const { providers } = await load()
  expect(
    providers.map((group) => ({
      kind: group.kind,
      name: group.displayName,
      total: group.totalCount,
      ready: group.readyCount,
      path: group.path,
      tools: group.tools.map((tool) => tool.relativePath),
    })),
  ).toEqual([
    {
      kind: 'added',
      name: 'GitHub',
      total: 3,
      ready: 2,
      path: `/${github}`,
      tools: ['issues.list', 'issues.greet', 'repos.get'],
    },
    {
      kind: 'coral',
      name: 'Coral',
      total: 2,
      ready: 2,
      path: `/${builtin}`,
      tools: ['search', 'addTool'],
    },
    {
      kind: 'shared',
      name: 'Shared',
      total: 2,
      ready: 2,
      path: `${page}/tools/shared`,
      tools: ['mail.remind', 'tides'],
    },
    {
      kind: 'private',
      name: 'Private',
      total: 3,
      ready: 3,
      path: `${page}/tools/private`,
      tools: ['people.greet', 'people.greet', 'reports.total'],
    },
  ])
})

// Catches: an Agent's private tool shown as the Owner's own, a Member shown
// anything but what Coral lists to them, a Member's page reading the Agents,
// which Coral refuses, or a row that repeats the namespace its heading shows.
it.each([
  {
    role: 'An owner',
    as: WorkspaceRole.OWNER,
    tools: [
      { path: 'people.greet', name: 'greet', policy: 'enabled', agent: undefined },
      {
        path: 'people.greet',
        name: 'greet',
        policy: 'disabled',
        agent: { id: workerId, label: 'Worker', path: `/${worker}` },
      },
      { path: 'reports.total', name: 'total', policy: 'approval_required', agent: undefined },
    ],
    agentReads: 1,
  },
  {
    role: 'A member',
    as: WorkspaceRole.MEMBER,
    tools: [{ path: 'mine.only', name: 'only', policy: 'enabled', agent: undefined }],
    agentReads: 0,
  },
])('$role sees the private tools Coral lists to them', async ({ as, tools, agentReads }) => {
  serve(as)
  const group = await groupOf('private')
  expect(
    group.tools.map((tool) => ({
      path: tool.relativePath,
      // Its group heading shows the namespace, so its row shows the rest.
      name: tool.nameInGroup,
      policy: tool.policy,
      agent: tool.agent,
    })),
  ).toEqual(tools)
  expect(called('listAgents')).toHaveLength(agentReads)
})

// Catches: a disabled Private Provider that still reads as enabled, or the
// group losing the version an Owner's change must carry.
it.each([
  { state: 'enabled', disabled: false, blockers: [] },
  { state: 'disabled', disabled: true, blockers: ['Provider disabled'] },
])('shows the private provider $state', async ({ disabled, blockers }) => {
  serve(WorkspaceRole.OWNER, { disabled })
  const group = await groupOf('private')
  expect({ enabled: group.enabled, blockers: group.blockers, etag: group.etag }).toEqual({
    enabled: !disabled,
    blockers,
    etag: 'private-etag',
  })
})

// Catches: the private page's drawer opening without the source, or a path
// Coral does not know rendering as an empty drawer.
it('reads the open private tool with its source and saved diagnostics', async () => {
  serve(WorkspaceRole.MEMBER)
  const result = await selectionLoader(
    args('tools/private/people.greet', { group: 'private', tool: 'people.greet' }) as never,
  )
  expect(result!.data.group).toBe('private')
  expect(result!.data.selectedTool).toBe('people.greet')
  expect(result!.data.selectedToolDetail).toEqual({
    kind: 'private',
    agent: null,
    relativePath: 'people.greet',
    description: 'Greet a person',
    declaration: 'declare function greet(): string',
    source: 'export default () => "hello"',
    diagnostics: [{ category: 'warning', code: 6133, message: 'Unused.', line: 1, column: null }],
    diagnosticsTruncated: false,
    savedAt: '2023-11-14T22:13:20.000Z',
  })
  await expect(
    selectionLoader(args('tools/private/gone', { group: 'private', tool: 'gone' }) as never),
  ).rejects.toMatchObject({ status: 404 })
})

const agentTool = { group: 'private', tool: 'people.greet', agent: workerId }

// Catches: an agent's address opening the owner's own tool at the same path,
// so the owner would review and publish the wrong source; a member opening
// an agent's private tool; or an agent named under a group that holds none.
it('reads an agent’s private tool for an owner only', async () => {
  serve(WorkspaceRole.OWNER)
  const address = `tools/private/people.greet/agents/${workerId}`
  // The tree and the selection both check the address.
  await expect(loader(args(address, agentTool) as never)).resolves.toBeTruthy()
  const result = await selectionLoader(args(address, agentTool) as never)
  expect(result!.data.selectedToolAgent).toBe(workerId)
  expect(result!.data.selectedToolDetail).toMatchObject({
    kind: 'private',
    agent: workerId,
    relativePath: 'people.greet',
    description: 'Greet a person, by the worker',
  })
  for (const shared of [loader, selectionLoader])
    await expect(
      shared(
        args(`tools/shared/mail.remind/agents/${workerId}`, {
          ...agentTool,
          group: 'shared',
          tool: 'mail.remind',
        }) as never,
      ),
    ).rejects.toMatchObject({ status: 404 })

  serve(WorkspaceRole.MEMBER)
  for (const refused of [loader, selectionLoader])
    await expect(refused(args(address, agentTool) as never)).rejects.toMatchObject({ status: 404 })
})

// Catches: a drawer without the source, the revisions or the history; a
// publisher who left shown by an identifier, or a person named who is no
// longer an owner; an event this Reef cannot name shown as a publish; or a
// history that reads as complete when Coral holds more.
it('reads the open shared tool with its revisions, its history and who published', async () => {
  serve(WorkspaceRole.MEMBER)
  const result = await selectionLoader(
    args('tools/shared/mail.remind', { group: 'shared', tool: 'mail.remind' }) as never,
  )
  const ada = { user: 'users/ada', name: 'Ada', email: '', local: false }
  expect(result!.data.group).toBe('shared')
  expect(result!.data.selectedToolDetail).toEqual({
    kind: 'shared',
    relativePath: 'mail.remind',
    description: 'Remind someone',
    declaration: 'export default function remind(): Promise<string>;',
    source: 'export default async () => "later"',
    diagnostics: [],
    diagnosticsTruncated: false,
    revision: 2,
    publisher: ada,
    publishedAt: '2023-11-14T22:16:40.000Z',
    revisions: [
      {
        revision: 2,
        description: 'Remind someone',
        publisher: ada,
        publishedAt: '2023-11-14T22:16:40.000Z',
        restoredFrom: 1,
      },
      {
        revision: 1,
        description: 'Remind someone, at first',
        publisher: null,
        publishedAt: '2023-11-14T22:15:00.000Z',
        restoredFrom: null,
      },
    ],
    events: [
      {
        id: 'event-2',
        action: 'published',
        revision: 2,
        actor: ada,
        at: '2023-11-14T22:16:40.000Z',
      },
      {
        id: 'event-1',
        action: 'published',
        revision: 1,
        actor: null,
        at: '2023-11-14T22:15:00.000Z',
      },
    ],
    eventsTruncated: true,
  })
  // The history is this shared tool's own, not the workspace's.
  expect(called('listSharedToolEvents').map((call) => call.request)).toMatchObject([
    { parent, sharedTool: remind },
  ])
  await expect(
    selectionLoader(args('tools/shared/gone', { group: 'shared', tool: 'gone' }) as never),
  ).rejects.toMatchObject({ status: 404 })
})

const post = (fields: Record<string, string | string[]>) => {
  const form = new FormData()
  for (const [name, value] of Object.entries(fields))
    for (const item of [value].flat()) form.append(name, item)
  return action(
    authRouteTestArgs(
      new Request(`http://reef${page}/tools/private`, {
        method: 'POST',
        headers: { origin: 'http://reef' },
        body: form,
      }),
      { workspace },
      null,
    ) as never,
  )
}
const selected = (key: string) => JSON.stringify({ provider: privateId, tool: key })

// Catches: an Owner's rule sent to the wrong provider, or one on an Agent's
// private tool landing on the Owner's own at the same path.
it('lets an owner set rules on their own and on an agent’s private tools', async () => {
  serve(WorkspaceRole.OWNER)
  const response = await post({
    intent: 'policy',
    access: 'approval_required',
    selectedTool: [selected(OWN_KEY), selected(AGENT_KEY)],
  })
  expect(response).toMatchObject({ data: { error: null } })
  expect(called('updateToolPolicies').map((call) => call.request)).toMatchObject([
    {
      parent: privateProvider,
      policies: [
        {
          tool: toolName(privateProvider, OWN_KEY),
          accessState: ToolAccessState.APPROVAL_REQUIRED,
        },
        {
          tool: toolName(privateProvider, AGENT_KEY),
          accessState: ToolAccessState.APPROVAL_REQUIRED,
        },
      ],
    },
  ])
})

// Catches: a Member's write reaching Coral, where only its refusal would
// keep the policy read-only.
it('refuses a member a rule and sends Coral nothing', async () => {
  serve(WorkspaceRole.MEMBER)
  await expect(
    post({ intent: 'policy', access: 'disabled', selectedTool: selected('user/mine.only') }),
  ).rejects.toMatchObject({ status: 403 })
  expect(called('updateToolPolicies')).toEqual([])
})

// Catches: a Member unable to delete their own private tool because the
// page's other changes are an Owner's, or a refused delete that leaves the page.
it('deletes your own private tool as a member, and keeps a refused delete in place', async () => {
  serve(WorkspaceRole.MEMBER)
  const deleted = await post({ intent: 'deletePrivateTool', path: 'people.greet' })
  expect(deleted).toBeInstanceOf(Response)
  expect((deleted as Response).headers.get('location')).toBe(
    `${page}/tools/private?privateToolDeleted=1`,
  )
  expect(called('deletePrivateTool').map((call) => call.request)).toMatchObject([
    { parent, path: 'people.greet' },
  ])
  const refused = await post({ intent: 'deletePrivateTool', path: 'gone' })
  expect(refused).toMatchObject({
    init: { status: 404 },
    data: { error: expect.stringContaining('It no longer exists.') },
  })
})

// Catches: an owner's delete of an agent's private tool removing the owner's
// own tool at the same path.
it('deletes an agent’s private tool by naming the agent', async () => {
  serve(WorkspaceRole.OWNER)
  await post({ intent: 'deletePrivateTool', path: 'people.greet', agent: workerId })
  expect(called('deletePrivateTool').map((call) => call.request)).toMatchObject([
    { parent, path: 'people.greet', agent: worker },
  ])
})

// Catches: a removal at a revision the owner did not see, or one that leaves
// the owner on the drawer of a shared tool that is gone.
it('removes a shared tool at the revision the owner saw and returns to the shared page', async () => {
  serve(WorkspaceRole.OWNER)
  const removed = await post({
    intent: 'removeSharedTool',
    path: 'mail.remind',
    expectedRevision: '2',
  })
  expect(called('removeSharedTool').map((call) => call.request)).toMatchObject([
    { parent, path: 'mail.remind', expectedRevision: 2n },
  ])
  expect(removed).toBeInstanceOf(Response)
  expect((removed as Response).headers.get('location')).toBe(
    `${page}/tools/shared?sharedToolRemoved=1`,
  )
})

// Catches: a refused removal that leaves the dialog, or that does not name
// the shared tool the owner must deal with first.
it.each([
  {
    name: 'another shared tool calls it',
    fields: { path: 'mail.send', expectedRevision: '1' },
    status: 409,
    error:
      'The shared tool coral.shared.mail.remind calls this one. Change or remove that shared tool first.',
  },
  {
    name: 'another owner published it again',
    fields: { path: 'mail.remind', expectedRevision: '1' },
    status: 409,
    error:
      'Another owner published this shared tool again. Close this dialog and check the new revision.',
  },
  {
    name: 'it is gone',
    fields: { path: 'mail.gone', expectedRevision: '1' },
    status: 404,
    error: 'You cannot remove this shared tool. It no longer exists. Close this panel.',
  },
])('keeps a removal refused because $name in the dialog', async ({ fields, status, error }) => {
  serve(WorkspaceRole.OWNER)
  expect(await post({ intent: 'removeSharedTool', ...fields })).toMatchObject({
    init: { status },
    data: { error },
  })
})

// Catches: a removal form with no revision removing whatever is current.
it('refuses a removal form without the revision and sends Coral nothing', async () => {
  serve(WorkspaceRole.OWNER)
  await expect(
    post({ intent: 'removeSharedTool', path: 'mail.remind', expectedRevision: '' }),
  ).rejects.toMatchObject({ status: 400 })
  expect(called('removeSharedTool')).toEqual([])
})

// Catches: a bookmark or a link to the former Providers page ending on a
// page that does not exist, or losing the query it carried.
it('opens the Tools page from an address of the former Providers page', () => {
  const response = formerListLoader(args('providers?added=1') as never)
  expect(response.status).toBe(302)
  expect(response.headers.get('location')).toBe(`${page}/tools?added=1`)
})
