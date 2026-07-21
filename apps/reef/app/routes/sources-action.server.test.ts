import { beforeEach, describe, expect, it, vi } from 'vitest'

const { deleteSource, sourceClientForRequest } = vi.hoisted(() => {
  const deleteSourceMock = vi.fn()
  return {
    deleteSource: deleteSourceMock,
    sourceClientForRequest: vi.fn(() => ({ deleteSource: deleteSourceMock })),
  }
})

vi.mock('@/lib/coral-request.server', () => ({
  sourceClientForRequest,
}))

import { authTestContext } from '@/auth/server-context.test-helper'

import { action } from './sources-action'

describe('sources action workspace routing', () => {
  beforeEach(() => {
    deleteSource.mockReset()
    deleteSource.mockResolvedValue({})
  })

  it('deletes from and redirects back to the route workspace', async () => {
    const request = new Request('http://localhost/workspaces/analytics/sources/github', {
      body: new URLSearchParams({ _intent: 'delete', name: 'github' }),
      method: 'POST',
    })

    const response = await action({
      context: authTestContext('coral-access-token'),
      params: { workspaceId: 'analytics' },
      request,
    })

    expect(deleteSource).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'github',
        workspace: expect.objectContaining({ name: 'analytics' }),
      }),
    )
    expect(sourceClientForRequest).toHaveBeenCalledWith(request, 'coral-access-token')
    expect(response).toBeInstanceOf(Response)
    expect((response as Response).headers.get('location')).toBe('/workspaces/analytics/sources')
  })
})
