import { create } from '@bufbuild/protobuf'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const {
  impliedWorkspaceForRequest,
  listWorkspacesForRequest,
  loadOnboardingSampleQuery,
  loadSourcesRouteData,
  runSourcesAction,
} = vi.hoisted(() => ({
  impliedWorkspaceForRequest: vi.fn(),
  listWorkspacesForRequest: vi.fn(),
  loadOnboardingSampleQuery: vi.fn(),
  loadSourcesRouteData: vi.fn(),
  runSourcesAction: vi.fn(),
}))

// `pickImpliedWorkspace` stays real, so these cases exercise the actual choice.
vi.mock('@/lib/workspaces.server', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/workspaces.server')>()),
  impliedWorkspaceForRequest,
  listWorkspacesForRequest,
}))
vi.mock('@/lib/onboarding-query.server', () => ({ loadOnboardingSampleQuery }))
vi.mock('./sources-loader', () => ({ loadSourcesRouteData }))
vi.mock('./sources-action', () => ({ runSourcesAction }))

import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { WorkspaceSchema } from '@/generated/coral/v1/resources_pb'

import { loader as indexLoader } from './index'
import { action, loader } from './onboarding'

const workspace = create(WorkspaceSchema, { name: 'analytics' })
const defaultWorkspace = create(WorkspaceSchema, { name: 'default' })

beforeEach(() => {
  impliedWorkspaceForRequest.mockReset()
  listWorkspacesForRequest.mockReset()
  loadOnboardingSampleQuery.mockReset()
  loadSourcesRouteData.mockReset()
  runSourcesAction.mockReset()
  impliedWorkspaceForRequest.mockResolvedValue(workspace)
  listWorkspacesForRequest.mockResolvedValue([workspace])
  loadSourcesRouteData.mockResolvedValue({
    entries: [{ installed: true, name: 'github' }],
    loadError: null,
  })
  loadOnboardingSampleQuery.mockResolvedValue({ rows: [], status: 'success' })
})

describe('onboarding route authentication', () => {
  it('passes the hosted token through workspace, source, and query loading', async () => {
    const request = new Request('http://coral-ui.test/onboarding?step=query')

    await loader(authRouteTestArgs(request, {}, 'coral-access-token'))

    expect(listWorkspacesForRequest).toHaveBeenCalledWith(request, 'coral-access-token')
    expect(loadSourcesRouteData).toHaveBeenCalledWith(request, workspace, 'coral-access-token')
    expect(loadOnboardingSampleQuery).toHaveBeenCalledWith(
      request,
      'coral-access-token',
      'analytics',
    )
  })

  it('keeps local onboarding requests unauthenticated', async () => {
    const request = new Request('http://coral-ui.test/onboarding?step=sources')

    await loader(authRouteTestArgs(request, {}, null))

    expect(listWorkspacesForRequest).toHaveBeenCalledWith(request, null)
    expect(loadSourcesRouteData).toHaveBeenCalledWith(request, workspace, null)
    // Not evidence about token threading: the loader only reaches the query
    // loader on `step=query`, so this holds on `step=sources` however the token
    // is threaded. The case below is the one that watches it.
    expect(loadOnboardingSampleQuery).not.toHaveBeenCalled()
  })

  it('threads the absent token into the query loader on the step that uses it', async () => {
    const request = new Request('http://coral-ui.test/onboarding?step=query')

    await loader(authRouteTestArgs(request, {}, null))

    expect(loadOnboardingSampleQuery).toHaveBeenCalledWith(request, null, 'analytics')
  })

  it('passes the hosted token through source actions', async () => {
    const request = formRequest({ _intent: 'install', name: 'github' })
    runSourcesAction.mockResolvedValue({ ok: true })

    await action(authRouteTestArgs(request, {}, 'coral-access-token'))

    expect(impliedWorkspaceForRequest).toHaveBeenCalledWith(request, 'coral-access-token')
    expect(runSourcesAction).toHaveBeenCalledWith(request, workspace, 'coral-access-token')
  })

  it('passes the hosted token through the finishing action', async () => {
    await action(authRouteTestArgs(completionRequest(), {}, 'coral-access-token'))

    expect(impliedWorkspaceForRequest).toHaveBeenCalledWith(
      expect.any(Request),
      'coral-access-token',
    )
  })
})

describe('onboarding server route', () => {
  it('loads the default workspace even when another workspace is listed first', async () => {
    listWorkspacesForRequest.mockResolvedValue([workspace, defaultWorkspace])
    const request = new Request('http://coral-ui.test/onboarding?step=query')

    const result = await loader(authRouteTestArgs(request, {}, null))

    expect(loadSourcesRouteData).toHaveBeenCalledWith(request, defaultWorkspace, null)
    expect(loadOnboardingSampleQuery).toHaveBeenCalledWith(request, null, 'default')
    expect(result).toMatchObject({
      workspaceId: 'default',
      // The switcher still needs the rest, so the choice must not filter them out.
      workspaces: [{ name: 'analytics' }, { name: 'default' }],
    })
  })

  it('offers workspace creation instead of a dead-end when the caller has no workspace', async () => {
    // On a shared server a fresh sign-in carries no memberships, and locally a
    // fresh install has no workspaces; either way the only creation dialog
    // lives behind the app shell this route gates, so a 404 here would strand
    // the caller with no path forward.
    listWorkspacesForRequest.mockResolvedValue([])
    const request = new Request('http://coral-ui.test/onboarding')

    const result = await loader(authRouteTestArgs(request, {}, null))

    expect(result).toEqual({ mode: 'create-workspace', runtime: 'web' })
    expect(loadSourcesRouteData).not.toHaveBeenCalled()
  })

  it('still renders onboarding for a caller who already walked it', async () => {
    // Onboarding records nothing now, so nothing can send a returning caller
    // away from the route: it is walkable as often as someone opens it.
    const request = new Request('http://coral-ui.test/onboarding?step=query')

    const result = await loader(authRouteTestArgs(request, {}, null))

    expect(result).toMatchObject({ step: { step: 'query' }, workspaceId: 'analytics' })
  })

  it('redirects to the normal app when onboarding is finished', async () => {
    const response = await action(authRouteTestArgs(completionRequest(), {}, null))

    expect(response).toBeInstanceOf(Response)
    expect((response as Response).headers.get('Location')).toBe('/workspaces/analytics/traces')
    expect(runSourcesAction).not.toHaveBeenCalled()
  })

  it('preserves route responses when the workspace is unavailable', async () => {
    const missingWorkspace = new Response('No Coral workspace is configured.', {
      status: 404,
      statusText: 'Workspace Not Found',
    })
    impliedWorkspaceForRequest.mockRejectedValueOnce(missingWorkspace)

    await expect(action(authRouteTestArgs(completionRequest(), {}, null))).rejects.toBe(
      missingWorkspace,
    )
  })

  it('returns typed internal-error action data when finishing fails', async () => {
    impliedWorkspaceForRequest.mockRejectedValueOnce(new Error('workspace lookup failed'))

    const result = await action(authRouteTestArgs(completionRequest(), {}, null))

    expect(result).toMatchObject({
      data: {
        intent: 'complete-onboarding',
        message: 'workspace lookup failed',
        status: 'error',
      },
      init: { status: 500 },
    })
  })

  it('keeps source intents on the existing source-action path', async () => {
    const result = { intent: 'install', name: 'github', status: 'success' }
    runSourcesAction.mockResolvedValue(result)
    const request = formRequest({ _intent: 'install', name: 'github' })

    await expect(action(authRouteTestArgs(request, {}, null))).resolves.toBe(result)
    expect(runSourcesAction).toHaveBeenCalledWith(request, workspace, null)
  })
})

describe('app index route', () => {
  it('goes straight to the workspace traces without offering onboarding', async () => {
    listWorkspacesForRequest.mockResolvedValue([workspace, defaultWorkspace])
    const request = new Request('http://coral-ui.test/?since=1h')

    const response = await indexLoader(authRouteTestArgs(request, {}, 'coral-token'))

    // One call only: the redirect helper now takes the workspace this loader
    // already picked, instead of listing the workspaces a second time.
    expect(listWorkspacesForRequest).toHaveBeenCalledOnce()
    expect(listWorkspacesForRequest).toHaveBeenCalledWith(request, 'coral-token')
    expect(response.headers.get('Location')).toBe('/workspaces/default/traces?since=1h')
  })

  it('sends a caller with no workspace to onboarding for its creation form', async () => {
    listWorkspacesForRequest.mockResolvedValue([])

    const response = await indexLoader(
      authRouteTestArgs(new Request('http://coral-ui.test/'), {}, null),
    )

    expect(response.headers.get('Location')).toBe('/onboarding')
    expect(response.headers.get('X-Remix-Replace')).toBe('true')
  })
})

function completionRequest() {
  return formRequest({ intent: 'complete-onboarding' })
}

function formRequest(fields: Record<string, string>) {
  return new Request('http://coral-ui.test/onboarding?step=next-steps', {
    body: new URLSearchParams(fields),
    method: 'POST',
  })
}
