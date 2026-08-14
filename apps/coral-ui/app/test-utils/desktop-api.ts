import { vi } from 'vitest'

import type { CoralDesktopApi } from '@/lib/coral-desktop'

export function createDesktopApi(overrides: Partial<CoralDesktopApi> = {}): CoralDesktopApi {
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
