import { afterEach, describe, expect, it, vi } from 'vitest'
import { renderHook } from 'vitest-browser-react'

import type { CoralDesktopApi, DesktopUpdateState } from '@/lib/coral-desktop'

import { useDesktopUpdateState } from './use-desktop-update-state'

afterEach(() => {
  delete window.coralDesktop
  vi.restoreAllMocks()
})

describe('useDesktopUpdateState', () => {
  it('subscribes before reading the current snapshot and cleans up the exact subscription', async () => {
    const calls: string[] = []
    const unsubscribe = vi.fn()
    const api = desktopApi({
      getUpdateState: vi.fn(async () => {
        calls.push('snapshot')
        return { status: 'available' as const, version: '0.9.0' }
      }),
      onUpdateStateChange: vi.fn(() => {
        calls.push('subscribe')
        return unsubscribe
      }),
    })
    window.coralDesktop = api

    const hook = await renderHook(() => useDesktopUpdateState(true))

    await expect
      .poll(() => hook.result.current)
      .toEqual({
        status: 'available',
        version: '0.9.0',
      })
    expect(calls).toEqual(['subscribe', 'snapshot'])

    await hook.unmount()
    expect(unsubscribe).toHaveBeenCalledOnce()
  })

  it('does not let a slow snapshot overwrite a newer pushed state', async () => {
    let listener: ((state: DesktopUpdateState) => void) | undefined
    let resolveSnapshot: ((state: DesktopUpdateState) => void) | undefined
    const snapshot = new Promise<DesktopUpdateState>((resolve) => {
      resolveSnapshot = resolve
    })
    const unsubscribe = vi.fn()
    window.coralDesktop = desktopApi({
      getUpdateState: vi.fn(() => snapshot),
      onUpdateStateChange: vi.fn((nextListener) => {
        listener = nextListener
        return unsubscribe
      }),
    })

    const hook = await renderHook(() => useDesktopUpdateState(true))
    await expect.poll(() => listener).toBeTypeOf('function')

    await hook.act(() => listener?.({ status: 'ready', version: '0.9.1' }))
    await expect
      .poll(() => hook.result.current)
      .toEqual({
        status: 'ready',
        version: '0.9.1',
      })

    await hook.act(async () => {
      resolveSnapshot?.({ status: 'available', version: '0.9.0' })
      await snapshot
    })
    expect(hook.result.current).toEqual({ status: 'ready', version: '0.9.1' })

    await hook.unmount()
    expect(unsubscribe).toHaveBeenCalledOnce()
  })

  it('does not access the Desktop bridge when disabled', async () => {
    const api = desktopApi()
    window.coralDesktop = api

    const hook = await renderHook(() => useDesktopUpdateState(false))

    expect(hook.result.current).toEqual({ status: 'idle' })
    expect(api.getUpdateState).not.toHaveBeenCalled()
    expect(api.onUpdateStateChange).not.toHaveBeenCalled()
  })
})

function desktopApi(overrides: Partial<CoralDesktopApi> = {}): CoralDesktopApi {
  return {
    configureMcp: vi.fn(async () => {}),
    getMcpLaunchConfig: vi.fn(async () => ({ args: [], command: 'coral' })),
    getUpdateState: vi.fn(async () => ({ status: 'idle' as const })),
    listMcpClients: vi.fn(async () => []),
    onUpdateStateChange: vi.fn(() => () => {}),
    removeMcp: vi.fn(async () => {}),
    ...overrides,
  }
}
