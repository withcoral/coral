import { create } from '@bufbuild/protobuf'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const {
  completeGuiOnboarding,
  firstWorkspaceForRequest,
  listWorkspacesForRequest,
  loadOnboardingSampleQuery,
  loadSourcesRouteData,
  runSourcesAction,
} = vi.hoisted(() => ({
  completeGuiOnboarding: vi.fn(),
  firstWorkspaceForRequest: vi.fn(),
  listWorkspacesForRequest: vi.fn(),
  loadOnboardingSampleQuery: vi.fn(),
  loadSourcesRouteData: vi.fn(),
  runSourcesAction: vi.fn(),
}))

// The loader lists workspaces (it renders a switcher and 404s when there are
// none); the action still resolves just the first one.
vi.mock('@/lib/workspaces.server', () => ({
  firstWorkspaceForRequest,
  listWorkspacesForRequest,
}))
vi.mock('@/lib/gui-onboarding.server', () => ({ completeGuiOnboarding }))
vi.mock('@/lib/onboarding-query.server', () => ({ loadOnboardingSampleQuery }))
vi.mock('./sources-loader', () => ({ loadSourcesRouteData }))
vi.mock('./sources-action', () => ({ runSourcesAction }))

import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { WorkspaceSchema } from '@/generated/coral/v1/resources_pb'

import { action, loader } from './onboarding'

const workspace = create(WorkspaceSchema, { name: 'analytics' })

beforeEach(() => {
  completeGuiOnboarding.mockReset()
  firstWorkspaceForRequest.mockReset()
  listWorkspacesForRequest.mockReset()
  loadOnboardingSampleQuery.mockReset()
  loadSourcesRouteData.mockReset()
  runSourcesAction.mockReset()
  firstWorkspaceForRequest.mockResolvedValue(workspace)
  listWorkspacesForRequest.mockResolvedValue([workspace])
  loadSourcesRouteData.mockResolvedValue({
    entries: [{ installed: true, name: 'github' }],
    loadError: null,
  })
  loadOnboardingSampleQuery.mockResolvedValue({ rows: [], status: 'success' })
})

describe('onboarding route authentication', () => {
  it('passes the hosted token through workspace, source, and query loading', async () => {
    const request = new Request('http://reef.test/onboarding?step=query')

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
    const request = new Request('http://reef.test/onboarding?step=sources')

    await loader(authRouteTestArgs(request, {}, null))

    expect(listWorkspacesForRequest).toHaveBeenCalledWith(request, null)
    expect(loadSourcesRouteData).toHaveBeenCalledWith(request, workspace, null)
    // Not evidence about token threading: the loader only reaches the query
    // loader on `step=query`, so this holds on `step=sources` however the token
    // is threaded. The case below is the one that watches it.
    expect(loadOnboardingSampleQuery).not.toHaveBeenCalled()
  })

  it('threads the absent token into the query loader on the step that uses it', async () => {
    const request = new Request('http://reef.test/onboarding?step=query')

    await loader(authRouteTestArgs(request, {}, null))

    expect(loadOnboardingSampleQuery).toHaveBeenCalledWith(request, null, 'analytics')
  })

  it('passes the hosted token through source actions', async () => {
    const request = formRequest({ _intent: 'install', name: 'github' })
    runSourcesAction.mockResolvedValue({ ok: true })

    await action(authRouteTestArgs(request, {}, 'coral-access-token'))

    expect(firstWorkspaceForRequest).toHaveBeenCalledWith(request, 'coral-access-token')
    expect(runSourcesAction).toHaveBeenCalledWith(request, workspace, 'coral-access-token')
  })

  it('passes the hosted token through onboarding completion', async () => {
    completeGuiOnboarding.mockResolvedValue(undefined)

    await action(authRouteTestArgs(completionRequest(), {}, 'coral-access-token'))

    expect(completeGuiOnboarding).toHaveBeenCalledWith(expect.any(Request), 'coral-access-token')
  })
})

describe('onboarding server route', () => {
  it('persists completion before redirecting to the normal app', async () => {
    completeGuiOnboarding.mockResolvedValue(undefined)
    const request = completionRequest()

    const response = await action(authRouteTestArgs(request, {}, null))

    expect(completeGuiOnboarding).toHaveBeenCalledWith(request, null)
    expect(response).toBeInstanceOf(Response)
    expect((response as Response).headers.get('Location')).toBe('/workspaces/analytics/traces')
    expect(runSourcesAction).not.toHaveBeenCalled()
  })

  it('preserves route responses when the workspace is unavailable', async () => {
    const missingWorkspace = new Response('No Coral workspace is configured.', {
      status: 404,
      statusText: 'Workspace Not Found',
    })
    firstWorkspaceForRequest.mockRejectedValueOnce(missingWorkspace)

    await expect(action(authRouteTestArgs(completionRequest(), {}, null))).rejects.toBe(
      missingWorkspace,
    )
    expect(completeGuiOnboarding).not.toHaveBeenCalled()
  })

  it('returns typed internal-error action data when completion fails', async () => {
    completeGuiOnboarding.mockRejectedValueOnce(new Error('completion database failed'))

    const result = await action(authRouteTestArgs(completionRequest(), {}, null))

    expect(result).toMatchObject({
      data: {
        intent: 'complete-onboarding',
        message: 'completion database failed',
        status: 'error',
      },
      init: { status: 500 },
    })
    expect(completeGuiOnboarding).toHaveBeenCalledOnce()
  })

  it('keeps source intents on the existing source-action path', async () => {
    const result = { intent: 'install', name: 'github', status: 'success' }
    runSourcesAction.mockResolvedValue(result)
    const request = formRequest({ _intent: 'install', name: 'github' })

    await expect(action(authRouteTestArgs(request, {}, null))).resolves.toBe(result)
    expect(runSourcesAction).toHaveBeenCalledWith(request, workspace, null)
    expect(completeGuiOnboarding).not.toHaveBeenCalled()
  })
})

function completionRequest() {
  return formRequest({ intent: 'complete-onboarding' })
}

function formRequest(fields: Record<string, string>) {
  return new Request('http://reef.test/onboarding?step=next-steps', {
    body: new URLSearchParams(fields),
    method: 'POST',
  })
}
