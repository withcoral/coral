import { expect, it, vi } from 'vitest'
import { csrfTokenForRequest } from '@/auth/csrf.server'
import { requestAuthContext } from '@/auth/server-context'
import { authRouteTestArgs, localRuntimeConfig } from '@/auth/server-context.test-helper'
import { hostedRuntimeConfig, requiredAuthConfig } from '@/auth/test-config'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import type { CoralWebRuntimeConfig } from '@/lib/runtime-config.server'
import { fakeManagementTransport } from '@/lib/management.test-support'
import * as workspaces from './workspaces'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))

const created = '00000000-0000-4000-8000-000000000001'
coral.transport = fakeManagementTransport({
  workspaces: {
    createWorkspace: ({ displayName }) => ({
      name: `workspaces/${created}`,
      displayName,
      effectiveRole: WorkspaceRole.OWNER,
      state: WorkspaceState.ACTIVE,
    }),
  },
})

const deployments: [string, CoralWebRuntimeConfig, string | null][] = [
  ['a local deployment', localRuntimeConfig, null],
  ['a hosted deployment', hostedRuntimeConfig, 'coral-token'],
]

// Catches: creation that still sets the removed onboarding cookie. The workspaces/quick-start
// browser journey proves that no dialog opens.
it.each(deployments)(
  'in %s, creating a workspace redirects to its overview without the onboarding cookie',
  async (_, runtime, token) => {
    const origin = runtime.auth.mode === 'required' ? runtime.auth.publicUrl : 'http://reef.test'
    const form = new FormData()
    form.set('intent', 'createWorkspace')
    form.set('displayName', 'Finance')
    form.set('requestId', 'request-1')
    const args = authRouteTestArgs(new Request(`${origin}/workspaces`), {}, token, runtime)
    const auth = args.context.get(requestAuthContext)
    let cookie = ''
    if (auth.mode === 'required') {
      const csrf = await csrfTokenForRequest(new Request(origin), requiredAuthConfig, auth.session)
      cookie = csrf.setCookie!.split(';', 1)[0]
      form.set('csrf', csrf.token)
    }
    const request = new Request(`${origin}/workspaces`, {
      method: 'POST',
      headers: { origin, cookie },
      body: form,
    })

    const response = await workspaces.action({ ...args, request })

    if (!(response instanceof Response)) throw new Error('expected a redirect')
    expect(response.status).toBe(302)
    expect(response.headers.get('Location')).toBe(`/workspaces/${created}`)
    expect(
      response.headers
        .getSetCookie()
        .filter((setCookie) => setCookie.includes('coral_web_workspace_onboarding')),
    ).toEqual([])
  },
)
