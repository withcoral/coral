import { create } from '@bufbuild/protobuf'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const {
  firstWorkspaceForRequest,
  loadOnboardingSampleQuery,
  loadSourcesRouteData,
  runSourcesAction,
} = vi.hoisted(() => ({
  firstWorkspaceForRequest: vi.fn(),
  loadOnboardingSampleQuery: vi.fn(),
  loadSourcesRouteData: vi.fn(),
  runSourcesAction: vi.fn(),
}))

vi.mock('@/lib/workspaces.server', () => ({ firstWorkspaceForRequest }))
vi.mock('@/lib/onboarding-query.server', () => ({ loadOnboardingSampleQuery }))
vi.mock('./sources-loader', () => ({ loadSourcesRouteData }))
vi.mock('./sources-action', () => ({ runSourcesAction }))

import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { WorkspaceSchema } from '@/generated/coral/v1/resources_pb'

import { action, loader } from './onboarding'

const workspace = create(WorkspaceSchema, { name: 'analytics' })

describe('onboarding route authentication', () => {
  beforeEach(() => {
    firstWorkspaceForRequest.mockReset()
    loadOnboardingSampleQuery.mockReset()
    loadSourcesRouteData.mockReset()
    runSourcesAction.mockReset()
    firstWorkspaceForRequest.mockResolvedValue(workspace)
    loadSourcesRouteData.mockResolvedValue({
      entries: [{ installed: true, name: 'github' }],
      loadError: null,
    })
    loadOnboardingSampleQuery.mockResolvedValue({ rows: [], status: 'success' })
  })

  it('passes the hosted token through workspace, source, and query loading', async () => {
    const request = new Request('http://reef.test/onboarding?step=query')

    await loader(authRouteTestArgs(request, {}, 'coral-access-token'))

    expect(firstWorkspaceForRequest).toHaveBeenCalledWith(request, 'coral-access-token')
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

    expect(firstWorkspaceForRequest).toHaveBeenCalledWith(request, null)
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
    const request = new Request('http://reef.test/onboarding', { method: 'POST' })
    runSourcesAction.mockResolvedValue({ ok: true })

    await action(authRouteTestArgs(request, {}, 'coral-access-token'))

    expect(firstWorkspaceForRequest).toHaveBeenCalledWith(request, 'coral-access-token')
    expect(runSourcesAction).toHaveBeenCalledWith(request, workspace, 'coral-access-token')
  })
})
