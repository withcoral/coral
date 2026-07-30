import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { CoralDesktopApi, DesktopUpdateState } from '../shared/types'

const electronMocks = vi.hoisted(() => ({
  exposeInMainWorld: vi.fn(),
  invoke: vi.fn(),
  on: vi.fn(),
  removeListener: vi.fn(),
}))

vi.mock('electron', () => ({
  contextBridge: {
    exposeInMainWorld: electronMocks.exposeInMainWorld,
  },
  ipcRenderer: {
    invoke: electronMocks.invoke,
    on: electronMocks.on,
    removeListener: electronMocks.removeListener,
  },
}))

await import('./index')

const api = electronMocks.exposeInMainWorld.mock.calls[0]?.[1] as CoralDesktopApi

beforeEach(() => {
  electronMocks.invoke.mockReset()
  electronMocks.on.mockReset()
  electronMocks.removeListener.mockReset()
})

describe('desktop update bridge', () => {
  it('gets the retained update state from the main process', async () => {
    const state: DesktopUpdateState = { status: 'downloading', version: '1.2.4' }
    electronMocks.invoke.mockResolvedValue(state)

    await expect(api.getUpdateState()).resolves.toEqual(state)
    expect(electronMocks.invoke).toHaveBeenCalledWith('coral:get-update-state')
  })

  it('forwards state changes and removes the exact IPC listener on unsubscribe', () => {
    const listener = vi.fn()
    const unsubscribe = api.onUpdateStateChange(listener)
    const ipcListener = electronMocks.on.mock.calls[0]?.[1]
    const state: DesktopUpdateState = { status: 'ready', version: '1.2.4' }

    ipcListener({}, state)

    expect(electronMocks.on).toHaveBeenCalledWith('coral:update-state-changed', ipcListener)
    expect(listener).toHaveBeenCalledWith(state)

    unsubscribe()
    expect(electronMocks.removeListener).toHaveBeenCalledWith(
      'coral:update-state-changed',
      ipcListener,
    )
  })
})
