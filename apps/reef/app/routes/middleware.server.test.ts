import { beforeEach, expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { MiddlewareMoveDirection } from '@/generated/coral/management/v1/middleware_pb'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import { action, loader } from './middleware'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))
const workspace = '00000000-0000-4000-8000-000000000001'
const audit = `workspaces/${workspace}/middleware/00000000-0000-4000-8000-0000000000aa`
const calls = {
  setMiddlewareEnabled: vi.fn(),
  updateMiddlewareScopes: vi.fn(),
  moveMiddleware: vi.fn(),
  deleteMiddleware: vi.fn(),
}
let role: WorkspaceRole

beforeEach(() => {
  vi.resetAllMocks()
  role = WorkspaceRole.OWNER
  coral.transport = fakeManagementTransport({
    workspaces: {
      getWorkspace: ({ name }) => ({
        name,
        displayName: 'Operations',
        effectiveRole: role,
        state: WorkspaceState.ACTIVE,
      }),
    },
    middleware: {
      listMiddleware: () => ({
        middleware: [
          {
            name: audit,
            displayName: 'audit',
            description: 'Count Operations',
            enabled: true,
            scopes: { run: false, function: false, operation: true },
            position: 0,
          },
        ],
      }),
      setMiddlewareEnabled: (request) => {
        calls.setMiddlewareEnabled(request)
        return { name: request.name }
      },
      updateMiddlewareScopes: (request) => {
        calls.updateMiddlewareScopes(request)
        return { name: request.name }
      },
      moveMiddleware: (request) => {
        calls.moveMiddleware(request)
        return { name: request.name }
      },
      deleteMiddleware: (request) => {
        calls.deleteMiddleware(request)
        return { name: request.name }
      },
    },
  })
})

const origin = 'http://127.0.0.1:3000'
const url = `${origin}/workspaces/${workspace}/middleware`
const load = () => loader(authRouteTestArgs(new Request(url), { workspace }, null))
const post = (fields: Record<string, string | undefined>) => {
  const form = new FormData()
  for (const [key, value] of Object.entries({ requestId: 'request', ...fields }))
    if (value !== undefined) form.set(key, value)
  return action(
    authRouteTestArgs(
      new Request(url, { method: 'POST', headers: { origin }, body: form }),
      { workspace },
      null,
    ),
  )
}
const noChanges = () => {
  for (const call of Object.values(calls)) expect(call).not.toHaveBeenCalled()
}

// Catches: an Owner page that drops a field the table needs.
it('lists the chain for a workspace owner', async () => {
  const result = await load()
  expect(result.data).toMatchObject({
    canChange: true,
    middleware: [
      {
        name: audit,
        path: `/${audit}`,
        displayName: 'audit',
        enabled: true,
        scopes: { run: false, function: false, operation: true },
        position: 0,
      },
    ],
  })
})

// Catches: a Member reaching Owner-only middleware by address.
it('refuses a member', async () => {
  role = WorkspaceRole.MEMBER
  await expect(load()).rejects.toMatchObject({ status: 403 })
  await expect(post({ intent: 'enable', name: audit, enabled: 'true' })).rejects.toMatchObject({
    status: 403,
  })
  noChanges()
})

// Catches: a control that sends Coral the wrong change or says the wrong thing about it.
it.each([
  {
    fields: { intent: 'enable', enabled: 'true' },
    call: 'setMiddlewareEnabled' as const,
    request: { name: audit, enabled: true },
    message: 'Middleware enabled. New executions use it.',
  },
  {
    fields: { intent: 'enable', enabled: 'false' },
    call: 'setMiddlewareEnabled' as const,
    request: { name: audit, enabled: false },
    message: 'Middleware disabled. New executions do not use it.',
  },
  {
    fields: { intent: 'scopes', run: 'on', operation: 'on' },
    call: 'updateMiddlewareScopes' as const,
    request: {
      name: audit,
      scopes: expect.objectContaining({ run: true, function: false, operation: true }),
    },
    message: 'Scopes saved. Coral disabled the middleware. Review it, then enable it again.',
  },
  {
    fields: { intent: 'move', direction: 'down' },
    call: 'moveMiddleware' as const,
    request: { name: audit, direction: MiddlewareMoveDirection.DOWN },
    message: 'Order saved. Coral disabled the moved middleware. Review it, then enable it again.',
  },
  {
    fields: { intent: 'delete' },
    call: 'deleteMiddleware' as const,
    request: { name: audit },
    message: 'Middleware deleted. Running executions keep the middleware they started with.',
  },
])('$fields.intent sends one change', async ({ fields, call, request, message }) => {
  const result = await post({ name: audit, ...fields })
  expect(result.data).toMatchObject({ error: null, message })
  expect(calls[call]).toHaveBeenCalledTimes(1)
  expect(calls[call]).toHaveBeenCalledWith(expect.objectContaining(request))
  for (const [other, fn] of Object.entries(calls))
    if (other !== call) expect(fn).not.toHaveBeenCalled()
})

// Catches: a form that names another workspace's middleware or an unknown action.
it.each([
  { name: `workspaces/00000000-0000-4000-8000-000000000002/middleware/x`, intent: 'delete' },
  { name: audit, intent: 'rename' },
  { name: audit, intent: 'move', direction: 'sideways' },
])('rejects $intent for $name without a change', async (fields) => {
  await expect(post(fields)).rejects.toMatchObject({ status: 400 })
  noChanges()
})
