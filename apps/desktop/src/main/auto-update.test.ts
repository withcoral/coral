import { describe, expect, it, vi } from 'vitest'

const electronMocks = vi.hoisted(() => ({
  exit: vi.fn(),
  showMessageBox: vi.fn(),
}))

vi.mock('electron', () => ({
  Notification: class {},
  app: {
    exit: electronMocks.exit,
    isPackaged: false,
  },
  autoUpdater: {
    once: vi.fn(),
  },
  dialog: {
    showMessageBox: electronMocks.showMessageBox,
  },
}))

import { getDesktopUpdateState, onDesktopUpdateStateChange } from './auto-update'

describe('unsupported desktop updates', () => {
  it('exposes a retained unsupported state without subscribing to an updater', () => {
    const listener = vi.fn()

    expect(getDesktopUpdateState()).toEqual({ status: 'unsupported' })
    const unsubscribe = onDesktopUpdateStateChange(listener)

    expect(listener).not.toHaveBeenCalled()
    expect(unsubscribe).not.toThrow()
  })
})
