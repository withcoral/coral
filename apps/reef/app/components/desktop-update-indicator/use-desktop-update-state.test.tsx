import { afterEach, describe, expect, it, vi } from 'vitest'
import { renderHook } from 'vitest-browser-react'

import type { DesktopUpdateState } from '@/lib/coral-desktop'
import { createDesktopApi } from '@/test-utils/desktop-api'

import { useDesktopUpdateState } from './use-desktop-update-state'

afterEach(() => {
  delete window.coralDesktop
  vi.restoreAllMocks()
})

describe('useDesktopUpdateState', () => {
  it('subscribes before reading the current snapshot and cleans up the exact subscription', async () => {
    const calls: string[] = []
    const unsubscribe = vi.fn()
    const api = createDesktopApi({
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
    window.coralDesktop = createDesktopApi({
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
    const api = createDesktopApi()
    window.coralDesktop = api

    const hook = await renderHook(() => useDesktopUpdateState(false))

    expect(hook.result.current).toEqual({ status: 'idle' })
    expect(api.getUpdateState).not.toHaveBeenCalled()
    expect(api.onUpdateStateChange).not.toHaveBeenCalled()
  })
})
