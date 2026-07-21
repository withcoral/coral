import { beforeEach, describe, expect, it, vi } from 'vitest'

const { firstWorkspaceForRequest } = vi.hoisted(() => ({
  firstWorkspaceForRequest: vi.fn(),
}))

vi.mock('@/lib/workspaces.server', () => ({ firstWorkspaceForRequest }))

import { redirectToFirstWorkspaceSources } from './workspace-redirect.server'

describe('redirectToFirstWorkspaceSources', () => {
  beforeEach(() => {
    firstWorkspaceForRequest.mockReset()
    firstWorkspaceForRequest.mockResolvedValue({ name: 'team alpha' })
  })

  it('redirects the app index to the first workspace sources and preserves its query', async () => {
    const response = await redirectToFirstWorkspaceSources(
      new Request('http://localhost/?from=index'),
      'coral-access-token',
    )

    expect(response.status).toBe(302)
    expect(response.headers.get('location')).toBe('/workspaces/team%20alpha/sources?from=index')
    expect(firstWorkspaceForRequest).toHaveBeenCalledWith(expect.any(Request), 'coral-access-token')
  })
})
