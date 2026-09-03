import { EventEmitter } from 'node:events'

import { beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  spawn: vi.fn(),
  getPath: vi.fn(),
  ensureDesktopCoralConfig: vi.fn(),
}))

vi.mock('node:child_process', () => ({ spawn: mocks.spawn }))
vi.mock('electron', () => ({
  app: {
    isPackaged: true,
    getPath: mocks.getPath,
  },
}))
// Stubbed so the test never creates a real config directory on disk.
vi.mock('./coral-config', () => ({
  desktopCoralConfigDir: (userDataDir: string, directory = 'coral') => `${userDataDir}/${directory}`,
  ensureDesktopCoralConfig: mocks.ensureDesktopCoralConfig,
}))

import { startCoralSidecar } from './sidecar'

// Enough of a ChildProcess for the startup handshake: startCoralSidecar resolves
// on the ready line.
function fakeChild() {
  const child = new EventEmitter() as EventEmitter & {
    stdout: EventEmitter
    stderr: EventEmitter
    kill: () => void
  }
  child.stdout = new EventEmitter()
  child.stderr = new EventEmitter()
  child.kill = vi.fn()
  return child
}

describe('packaged sidecar spawn', () => {
  const userData = '/home/coral/.config/Coral'

  beforeEach(() => {
    vi.clearAllMocks()
    mocks.getPath.mockReturnValue(userData)
    mocks.ensureDesktopCoralConfig.mockResolvedValue(`${userData}/coral`)
    // Electron defines resourcesPath; plain Node does not.
    Object.defineProperty(process, 'resourcesPath', {
      value: '/tmp/.mount_Coral/usr/lib/coral-desktop/resources',
      configurable: true,
    })
  })

  // An AppImage mounts resourcesPath as a read-only squashfs, so anything the
  // sidecar resolves relative to cwd has to land somewhere writable instead.
  it('runs in the writable userData directory, not the read-only resources path', async () => {
    const child = fakeChild()
    mocks.spawn.mockReturnValue(child)

    const starting = startCoralSidecar()
    // The config directory is prepared before the spawn, so wait for the spawn
    // rather than emitting the ready line into a listener that does not exist.
    await vi.waitFor(() => expect(mocks.spawn).toHaveBeenCalled())
    child.stdout.emit(
      'data',
      Buffer.from('Coral gRPC server listening on http://127.0.0.1:8778\n'),
    )

    const sidecar = await starting
    expect(sidecar.url).toBe('http://127.0.0.1:8778')

    const [, , options] = mocks.spawn.mock.calls[0]
    expect(options.cwd).toBe(userData)
  })
})
