import { create } from '@bufbuild/protobuf'
import { createMemoryRouter, redirect } from 'react-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const {
  completeGuiOnboarding,
  getGuiOnboardingCompleted,
  impliedWorkspaceForRequest,
  listWorkspacesForRequest,
  loadOnboardingSampleQuery,
  loadSourcesRouteData,
  runSourcesAction,
} = vi.hoisted(() => ({
  completeGuiOnboarding: vi.fn(),
  getGuiOnboardingCompleted: vi.fn(),
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
vi.mock('@/lib/gui-onboarding.server', () => ({
  completeGuiOnboarding,
  getGuiOnboardingCompleted,
}))
vi.mock('@/lib/onboarding-query.server', () => ({ loadOnboardingSampleQuery }))
vi.mock('./sources-loader', () => ({ loadSourcesRouteData }))
vi.mock('./sources-action', () => ({ runSourcesAction }))

import { authRouteTestArgs, authTestContext } from '@/auth/server-context.test-helper'
import { WorkspaceSchema } from '@/generated/coral/v1/resources_pb'

import { action, loader } from './onboarding'

const workspace = create(WorkspaceSchema, { name: 'analytics' })
const defaultWorkspace = create(WorkspaceSchema, { name: 'default' })

beforeEach(() => {
  completeGuiOnboarding.mockReset()
  impliedWorkspaceForRequest.mockReset()
  listWorkspacesForRequest.mockReset()
  getGuiOnboardingCompleted.mockReset()
  loadOnboardingSampleQuery.mockReset()
  loadSourcesRouteData.mockReset()
  runSourcesAction.mockReset()
  getGuiOnboardingCompleted.mockResolvedValue(false)
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

  it('passes the hosted token through onboarding completion', async () => {
    completeGuiOnboarding.mockResolvedValue(undefined)

    await action(authRouteTestArgs(completionRequest(), {}, 'coral-access-token'))

    expect(completeGuiOnboarding).toHaveBeenCalledWith(expect.any(Request), 'coral-access-token')
  })

  it('passes the hosted token through the completion-state check', async () => {
    await loader(
      authRouteTestArgs(new Request('http://coral-ui.test/onboarding'), {}, 'coral-token'),
    )

    expect(getGuiOnboardingCompleted).toHaveBeenCalledWith(expect.any(Request), 'coral-token')
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

  it('replaces completed users directly into the normal app before loading onboarding data', async () => {
    getGuiOnboardingCompleted.mockResolvedValue(true)
    const request = new Request('http://coral-ui.test/onboarding')

    const response = await loader(authRouteTestArgs(request, {}, null))

    expect(response).toBeInstanceOf(Response)
    expect((response as Response).status).toBe(302)
    expect((response as Response).headers.get('Location')).toBe('/workspaces/analytics/traces')
    expect((response as Response).headers.get('X-Remix-Replace')).toBe('true')
    expect(impliedWorkspaceForRequest).toHaveBeenCalledWith(request, null)
    expect(listWorkspacesForRequest).not.toHaveBeenCalled()
    expect(loadSourcesRouteData).not.toHaveBeenCalled()
  })

  it('does not trap completed users when they navigate back into onboarding history', async () => {
    getGuiOnboardingCompleted.mockResolvedValue(true)
    const router = createMemoryRouter(
      [
        { path: '/before' },
        { loader: () => redirect(`/workspaces/${workspace.name}/traces`), path: '/' },
        { loader, path: '/onboarding' },
        { path: '/workspaces/:workspaceId/traces' },
      ],
      {
        getContext: () => authTestContext(null),
        initialEntries: [
          '/before',
          '/onboarding?step=query',
          `/workspaces/${workspace.name}/traces`,
        ],
        initialIndex: 2,
      },
    )

    try {
      await router.navigate(-1)

      expect(router.state.location.pathname).toBe(`/workspaces/${workspace.name}/traces`)
      expect(router.state.historyAction).toBe('REPLACE')

      await router.navigate(-1)

      expect(router.state.location.pathname).toBe('/before')
      expect(getGuiOnboardingCompleted).toHaveBeenCalledOnce()
    } finally {
      router.dispose()
    }
  })

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
    impliedWorkspaceForRequest.mockRejectedValueOnce(missingWorkspace)

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
  return new Request('http://coral-ui.test/onboarding?step=next-steps', {
    body: new URLSearchParams(fields),
    method: 'POST',
  })
}
