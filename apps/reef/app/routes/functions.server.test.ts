import { ConnectError, Code } from '@connectrpc/connect'
import { expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { ToolAccessState } from '@/generated/coral/management/v1/policy_pb'
import { AuthoredToolDiagnosticCategory } from '@/generated/coral/management/v1/authored_tool_pb'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport, type ManagementHandlers } from '@/lib/management.test-support'
import { action as functionAction, loader as functionLoader } from './function'
import { loader as functionsLoader, shouldRevalidate } from './functions'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))
const workspace = '00000000-0000-4000-8000-000000000001'
const parent = `workspaces/${workspace}`
const savedAt = { seconds: 1_700_000_000n, nanos: 0 }

const privateProvider = `${parent}/providers/00000000-0000-4000-8000-0000000000aa`

function api(
  privateTools: ManagementHandlers['privateTools'],
  policies: ManagementHandlers['policies'] = { listToolAccess: () => ({ tools: [] }) },
) {
  return fakeManagementTransport({
    workspaces: {
      getWorkspace: ({ name }) => ({
        name,
        displayName: 'Research',
        effectiveRole: WorkspaceRole.MEMBER,
        state: WorkspaceState.ACTIVE,
      }),
    },
    privateTools,
    policies,
  })
}
const listArgs = (query = '') =>
  authRouteTestArgs(
    new Request(`http://reef/workspaces/${workspace}/functions${query}`),
    { workspace },
    null,
  )
const detailArgs = (path: string) =>
  authRouteTestArgs(
    new Request(`http://reef/workspaces/${workspace}/functions/${encodeURIComponent(path)}`),
    { workspace, path },
    null,
  )

it('lists every page of a Member’s own functions as source-free summaries', async () => {
  const requests: unknown[] = []
  coral.transport = api({
    listPrivateTools: (request) => {
      requests.push({
        parent: request.parent,
        pageSize: request.pageSize,
        pageToken: request.pageToken,
      })
      return request.pageToken
        ? { privateTools: [{ path: 'zeta.last', description: 'Last', updateTime: savedAt }] }
        : {
            privateTools: [
              { path: 'reports.total', description: 'Sum the report', updateTime: savedAt },
            ],
            nextPageToken: 'after-reports',
          }
    },
  })
  const result = await functionsLoader(listArgs())
  expect(requests).toEqual([
    { parent, pageSize: 100, pageToken: '' },
    { parent, pageSize: 100, pageToken: 'after-reports' },
  ])
  // The workspace heads the breadcrumb.
  expect(result.data.workspace.path).toBe(`/workspaces/${workspace}`)
  // Summaries never carry source or diagnostics to the browser.
  expect(result.data.functions).toEqual([
    {
      path: 'reports.total',
      description: 'Sum the report',
      savedAt: '2023-11-14T22:13:20.000Z',
      href: `/workspaces/${workspace}/functions/reports.total`,
    },
    {
      path: 'zeta.last',
      description: 'Last',
      savedAt: '2023-11-14T22:13:20.000Z',
      href: `/workspaces/${workspace}/functions/zeta.last`,
    },
  ])
})

// Catches: a function shown with a policy other than the one Coral applies
// to it, or with an Agent's policy for a tool at the same path.
it('shows each function with the policy of its own row in the private provider', async () => {
  const requests: unknown[] = []
  coral.transport = api(
    {
      listPrivateTools: () => ({
        privateTools: [
          { path: 'people.blocked', description: 'Blocked', updateTime: savedAt },
          { path: 'people.greet', description: 'Greet', updateTime: savedAt },
        ],
        privateProvider,
      }),
    },
    {
      listToolAccess: (request) => {
        requests.push({ parent: request.parent, pageToken: request.pageToken })
        return request.pageToken
          ? {
              tools: [
                {
                  relativePath: 'people.blocked',
                  accessState: ToolAccessState.DISABLED,
                  explicit: false,
                },
              ],
            }
          : {
              tools: [
                {
                  relativePath: 'people.greet',
                  accessState: ToolAccessState.APPROVAL_REQUIRED,
                  explicit: true,
                },
                {
                  relativePath: 'people.blocked',
                  accessState: ToolAccessState.ENABLED,
                  explicit: false,
                  agent: `${parent}/agents/00000000-0000-4000-8000-0000000000bb`,
                },
              ],
              nextPageToken: 'after-greet',
            }
      },
    },
  )
  const result = await functionsLoader(listArgs())
  expect(requests).toEqual([
    { parent: privateProvider, pageToken: '' },
    { parent: privateProvider, pageToken: 'after-greet' },
  ])
  expect(result.data.functions.map((f) => [f.path, f.policy])).toEqual([
    ['people.blocked', 'disabled'],
    ['people.greet', 'approval_required'],
  ])
})

it('loads one function with its source, declaration, and saved diagnostics', async () => {
  let requested: unknown
  coral.transport = api({
    getPrivateTool: (request) => {
      requested = { parent: request.parent, path: request.path }
      return {
        path: request.path,
        description: 'Sum the report',
        declaration: 'declare const _default: () => number;',
        source: 'export default () => missing;',
        diagnostics: [
          {
            category: AuthoredToolDiagnosticCategory.ERROR,
            code: 2304n,
            message: "Cannot find name 'missing'.",
            line: 1n,
            column: 22n,
          },
          {
            category: AuthoredToolDiagnosticCategory.SUGGESTION,
            code: 80004n,
            message: 'Consider a type annotation.',
          },
        ],
        diagnosticsTruncated: true,
        updateTime: savedAt,
      }
    },
  })
  const result = await functionLoader(detailArgs('reports.total'))
  expect(requested).toEqual({ parent, path: 'reports.total' })
  expect(result.data.privateTool).toEqual({
    path: 'reports.total',
    description: 'Sum the report',
    declaration: 'declare const _default: () => number;',
    source: 'export default () => missing;',
    diagnostics: [
      {
        category: 'error',
        code: 2304,
        message: "Cannot find name 'missing'.",
        line: 1,
        column: 22,
      },
      {
        category: 'suggestion',
        code: 80004,
        message: 'Consider a type annotation.',
        line: null,
        column: null,
      },
    ],
    diagnosticsTruncated: true,
    savedAt: '2023-11-14T22:13:20.000Z',
  })
})

it.each([
  ['another User’s or an absent function', Code.NotFound],
  ['a malformed path', Code.InvalidArgument],
])('reads %s as a missing page', async (_case, code) => {
  coral.transport = api({
    getPrivateTool: () => {
      throw new ConnectError('concealed', code)
    },
  })
  await expect(functionLoader(detailArgs('reports.total'))).rejects.toMatchObject({ status: 404 })
})

const list = `http://reef/workspaces/${workspace}/functions`
it.each([
  ['opening a function keeps the list', list, `${list}/linear.issueBrief`, undefined, false],
  ['moving between functions keeps the list', `${list}/a.b`, `${list}/a.c`, undefined, false],
  ['closing a function keeps the list', `${list}/a.b`, list, undefined, false],
  ['opening a namespace keeps the list', list, `${list}/namespaces/linear`, undefined, false],
  [
    'closing a function to its tab keeps the list',
    `${list}/a.b`,
    `${list}/namespaces/a`,
    undefined,
    false,
  ],
  ['a refresh reloads the list', list, list, undefined, true],
  [
    'another workspace reloads the list',
    list,
    'http://reef/workspaces/00000000-0000-4000-8000-000000000002/functions/a.b',
    undefined,
    true,
  ],
  ['a submission reloads the list', `${list}/a.b`, list, 'POST', true],
] as const)('%s', (_case, current, next, formMethod, expected) => {
  expect(
    shouldRevalidate({
      currentUrl: new URL(current),
      nextUrl: new URL(next),
      formMethod,
      defaultShouldRevalidate: true,
    } as Parameters<typeof shouldRevalidate>[0]),
  ).toBe(expected)
})

// Local mode accepts submissions only from a loopback origin.
const origin = 'http://127.0.0.1:3000'
const remove = (path: string, intent = 'delete') => {
  const form = new FormData()
  form.set('intent', intent)
  form.set('requestId', 'request')
  return functionAction(
    authRouteTestArgs(
      new Request(`${origin}/workspaces/${workspace}/functions/${encodeURIComponent(path)}`, {
        method: 'POST',
        headers: { origin },
        body: form,
      }),
      { workspace, path },
      null,
    ),
  )
}

// Catches: deleting the wrong function, or leaving its drawer open after it is gone.
it('deletes the open function and returns to the list', async () => {
  const deleted: unknown[] = []
  coral.transport = api({
    deletePrivateTool: (request) => {
      deleted.push({ parent: request.parent, path: request.path })
      return {}
    },
  })
  const response = await remove('reports.total')
  expect(deleted).toEqual([{ parent, path: 'reports.total' }])
  expect(response).toBeInstanceOf(Response)
  // The list announces the delete when it loads.
  expect((response as Response).headers.get('location')).toBe(
    `/workspaces/${workspace}/functions?functionDeleted=1`,
  )
})

// Catches: a refused delete closing the dialog as if it worked.
it('keeps a refused delete in the dialog with its reason', async () => {
  coral.transport = api({
    deletePrivateTool: () => {
      throw new ConnectError('concealed', Code.NotFound)
    },
  })
  const result = await remove('reports.total')
  expect(result).toMatchObject({
    init: { status: 404 },
    data: { error: 'You cannot delete this function. It no longer exists. Close this panel.' },
  })
})

// Catches: a forged intent reaching Coral as a delete.
it('rejects an unknown intent without calling Coral', async () => {
  const deletePrivateTool = vi.fn(() => ({}))
  coral.transport = api({ deletePrivateTool })
  await expect(remove('reports.total', 'rename')).rejects.toMatchObject({ status: 400 })
  expect(deletePrivateTool).not.toHaveBeenCalled()
})
