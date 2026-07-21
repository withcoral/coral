import { afterEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-react'

import SettingsRoute from './settings'

afterEach(() => {
  vi.unstubAllEnvs()
  delete window.coralDesktop
})

describe('SettingsRoute', () => {
  it('does not show desktop-only MCP clients on the web', async () => {
    vi.stubEnv('VITE_CORAL_DESKTOP_APP', '')

    const screen = await render(<SettingsRoute />)

    await expect.element(screen.getByRole('heading', { level: 1, name: 'Settings' })).toBeVisible()
    await expect
      .element(screen.getByRole('heading', { name: 'MCP Clients' }))
      .not.toBeInTheDocument()
    await expect.element(screen.getByText('Desktop bridge unavailable.')).not.toBeInTheDocument()
  })

  it('shows MCP clients in the desktop build', async () => {
    vi.stubEnv('VITE_CORAL_DESKTOP_APP', '1')
    const listMcpClients = vi
      .fn()
      .mockResolvedValue([
        { configPath: '/tmp/codex-config.toml', id: 'codex' as const, name: 'Codex' },
      ])
    window.coralDesktop = {
      configureMcp: vi.fn(),
      listMcpClients,
    }

    const screen = await render(<SettingsRoute />)

    await expect
      .element(screen.getByRole('heading', { level: 2, name: 'MCP Clients' }))
      .toBeVisible()
    await expect.element(screen.getByText('Codex', { exact: true })).toBeVisible()
    expect(listMcpClients).toHaveBeenCalledOnce()
  })
})
