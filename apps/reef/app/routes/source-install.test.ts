import { beforeEach, describe, expect, it, vi } from 'vitest'

const { addToast } = vi.hoisted(() => ({ addToast: vi.fn() }))

vi.mock('@/wax/components/toast', () => ({ addToast }))

import type { SourcesActionData } from './sources-action'
import { clientAction } from './source-install'

describe('source install client action', () => {
  beforeEach(() => {
    addToast.mockReset()
  })

  it('shows a success toast before redirecting to the source catalog', async () => {
    const serverAction = vi.fn().mockResolvedValue({
      intent: 'import',
      name: 'spotify',
      status: 'success',
    } satisfies SourcesActionData)

    const response = await clientAction({
      params: { workspaceId: 'analytics' },
      serverAction,
    } as never)

    expect(addToast).toHaveBeenCalledWith('success', {
      title: 'Created spotify',
      description: 'The source was validated and installed.',
    })
    expect(response).toBeInstanceOf(Response)
    expect((response as Response).headers.get('location')).toBe('/workspaces/analytics/sources')
  })

  it('returns server validation errors without showing a toast or redirecting', async () => {
    const error = {
      intent: 'import',
      message: 'The descriptor could not be loaded.',
      name: 'spotify',
      status: 'error',
    } satisfies SourcesActionData

    const result = await clientAction({
      params: { workspaceId: 'analytics' },
      serverAction: vi.fn().mockResolvedValue(error),
    } as never)

    expect(result).toBe(error)
    expect(addToast).not.toHaveBeenCalled()
  })
})
