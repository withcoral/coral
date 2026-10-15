import { expect, it, vi } from 'vitest'
import { csrfTokenForRequest } from '@/auth/csrf.server'
import { requestAuthContext } from '@/auth/server-context'
import { authRouteTestArgs, localRuntimeConfig } from '@/auth/server-context.test-helper'
import { hostedRuntimeConfig, requiredAuthConfig } from '@/auth/test-config'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import type { CoralWebRuntimeConfig } from '@/lib/runtime-config.server'
import { fakeManagementTransport } from '@/lib/management.test-support'
import * as overview from './workspace'
import * as workspaces from './workspaces'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))

const created = '00000000-0000-4000-8000-000000000001'
const other = '00000000-0000-4000-8000-000000000002'
coral.transport = fakeManagementTransport({
  workspaces: {
    createWorkspace: ({ displayName }) => ({
      name: `workspaces/${created}`,
      displayName,
      effectiveRole: WorkspaceRole.OWNER,
      state: WorkspaceState.ACTIVE,
    }),
    getWorkspace: ({ name }) => ({
      name,
      displayName: 'Finance',
      effectiveRole: WorkspaceRole.OWNER,
      state: WorkspaceState.ACTIVE,
      mcpEndpoint: `https://mcp.example.test/${name}/mcp`,
    }),
  },
  agents: { listAgents: () => ({ agents: [] }) },
  providers: { listProviders: () => ({ providers: [] }) },
  memberships: { listWorkspaceOwners: () => ({ owners: [] }) },
  activity: {
    browseActivityRoots: () => ({ roots: [] }),
    browseRequiredActions: () => ({ actions: [] }),
  },
})

/** The cookies a browser keeps across the responses it receives. */
class Browser {
  private cookies = new Map<string, string>()
  receive(headers: HeadersInit | undefined) {
    for (const line of new Headers(headers).getSetCookie()) {
      const [pair] = line.split(';', 1)
      const [name, value] = [pair.slice(0, pair.indexOf('=')), pair.slice(pair.indexOf('=') + 1)]
      if (/max-age=0/i.test(line) || value === '') this.cookies.delete(name)
      else this.cookies.set(name, value)
    }
  }
  header() {
    return [...this.cookies].map(([name, value]) => `${name}=${value}`).join('; ')
  }
}

const deployments: [string, CoralWebRuntimeConfig, string | null][] = [
  ['a local deployment', localRuntimeConfig, null],
  ['a hosted deployment', hostedRuntimeConfig, 'coral-token'],
]

async function createWorkspace(
  browser: Browser,
  runtime: CoralWebRuntimeConfig,
  token: string | null,
) {
  const origin = runtime.auth.mode === 'required' ? runtime.auth.publicUrl : 'http://reef.test'
  const form = new FormData()
  form.set('intent', 'createWorkspace')
  form.set('displayName', 'Finance')
  form.set('requestId', 'request-1')
  const args = authRouteTestArgs(new Request(`${origin}/workspaces`), {}, token, runtime)
  const auth = args.context.get(requestAuthContext)
  if (auth.mode === 'required') {
    const csrf = await csrfTokenForRequest(new Request(origin), requiredAuthConfig, auth.session)
    browser.receive({ 'Set-Cookie': csrf.setCookie! })
    form.set('csrf', csrf.token)
  }
  const request = new Request(`${origin}/workspaces`, {
    method: 'POST',
    headers: { origin, cookie: browser.header() },
    body: form,
  })
  const response = await workspaces.action({ ...args, request })
  if (!(response instanceof Response)) throw new Error('expected a redirect')
  browser.receive(response.headers)
  return response
}

async function openOverview(
  browser: Browser,
  runtime: CoralWebRuntimeConfig,
  token: string | null,
  workspace: string,
) {
  const origin = runtime.auth.mode === 'required' ? runtime.auth.publicUrl : 'http://reef.test'
  const request = new Request(`${origin}/workspaces/${workspace}`, {
    headers: { cookie: browser.header() },
  })
  const result = await overview.loader(authRouteTestArgs(request, { workspace }, token, runtime))
  browser.receive(result.init?.headers)
  return result.data.onboarding
}

// Catches: onboarding that never opens after creation, or reopens on every visit.
it.each(deployments)(
  'in %s, a new workspace opens onboarding on its overview once',
  async (_, runtime, token) => {
    const browser = new Browser()
    const response = await createWorkspace(browser, runtime, token)
    expect(response.status).toBe(302)
    expect(response.headers.get('Location')).toBe(`/workspaces/${created}`)

    expect(await openOverview(browser, runtime, token, created)).toBe(true)
    expect(await openOverview(browser, runtime, token, created)).toBe(false)
  },
)

// Catches: another workspace's overview showing, or using up, the new workspace's onboarding.
it('opens onboarding only on the overview of the workspace just created', async () => {
  const browser = new Browser()
  await createWorkspace(browser, localRuntimeConfig, null)

  expect(await openOverview(browser, localRuntimeConfig, null, other)).toBe(false)
  expect(await openOverview(browser, localRuntimeConfig, null, created)).toBe(true)
})

// Catches: a visit without a creation (a shared link, a reload) opening onboarding.
it('does not open onboarding on an overview reached without creating the workspace', async () => {
  expect(await openOverview(new Browser(), localRuntimeConfig, null, created)).toBe(false)
})
