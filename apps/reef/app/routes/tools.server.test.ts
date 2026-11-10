import { Code, ConnectError } from '@connectrpc/connect'
import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { AuthoredToolDiagnosticCategory } from '@/generated/coral/management/v1/authored_tool_pb'
import { ToolAccessState } from '@/generated/coral/management/v1/policy_pb'
import { ProviderBlocker, ProviderState } from '@/generated/coral/management/v1/provider_pb'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
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
const worker = `${parent}/agents/00000000-0000-4000-8000-0000000000b1`

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
      getPrivateTool: ({ path }) => {
        if (path !== 'people.greet') throw new ConnectError('NOT_FOUND', Code.NotFound)
        return {
          path,
          description: 'Greet a person',
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
      deletePrivateTool: record('deletePrivateTool', ({ path }: { path: string }) => {
        if (path !== 'people.greet') throw new ConnectError('NOT_FOUND', Code.NotFound)
        return {}
      }),
    },
    agents: {
      listAgents: record('listAgents', () => ({
        agents: [{ name: worker, displayName: 'Worker' }],
      })),
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
// its tools, or a page that leaves out the Private Provider that Coral's list
// does not name.
it('groups every tool under Coral, each provider and Private, with its count', async () => {
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
        agent: { label: 'Worker', path: `/${worker}` },
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
  expect(result!.data.privateGroup).toBe(true)
  expect(result!.data.selectedTool).toBe('people.greet')
  expect(result!.data.selectedToolDetail).toEqual({
    kind: 'private',
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

// Catches: a bookmark or a link to the former Providers page ending on a
// page that does not exist, or losing the query it carried.
it('opens the Tools page from an address of the former Providers page', () => {
  const response = formerListLoader(args('providers?added=1') as never)
  expect(response.status).toBe(302)
  expect(response.headers.get('location')).toBe(`${page}/tools?added=1`)
})
