import { beforeEach, describe, expect, it, vi } from 'vitest'

const { deleteSource } = vi.hoisted(() => ({ deleteSource: vi.fn() }))

vi.mock('@/lib/coral-request.server', () => ({
  sourceClientForRequest: () => ({ deleteSource }),
}))

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

    const response = await action({ params: { workspaceId: 'analytics' }, request })

    expect(deleteSource).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'github',
        workspace: expect.objectContaining({ name: 'analytics' }),
      }),
    )
    expect(response).toBeInstanceOf(Response)
    expect((response as Response).headers.get('location')).toBe('/workspaces/analytics/sources')
  })
})
