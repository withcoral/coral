import { mkdtemp, readFile, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { ensureDesktopCoralConfig } from './coral-config'

describe('ensureDesktopCoralConfig', () => {
  it('creates an isolated ephemeral-loopback server configuration', async () => {
    const userData = await mkdtemp(join(tmpdir(), 'coral-desktop-'))
    const configDir = await ensureDesktopCoralConfig(userData)

    expect(configDir).toBe(join(userData, 'coral'))
    await expect(readFile(join(configDir, 'config.toml'), 'utf8')).resolves.toBe(
      'version = 1\n\n[server]\nbind_addr = "127.0.0.1:0"\n',
    )
  })

  it('preserves an existing Desktop configuration', async () => {
    const userData = await mkdtemp(join(tmpdir(), 'coral-desktop-'))
    const configDir = await ensureDesktopCoralConfig(userData)
    await writeFile(join(configDir, 'config.toml'), '[server]\nbind_addr = "127.0.0.1:1457"\n')

    await ensureDesktopCoralConfig(userData)

    await expect(readFile(join(configDir, 'config.toml'), 'utf8')).resolves.toBe(
      '[server]\nbind_addr = "127.0.0.1:1457"\n',
    )
  })

  it('creates a separate fixed-port configuration for the external development server', async () => {
    const userData = await mkdtemp(join(tmpdir(), 'coral-desktop-'))
    await ensureDesktopCoralConfig(userData)

    const configDir = await ensureDesktopCoralConfig(userData, {
      bindAddr: '127.0.0.1:8778',
      directory: 'coral-dev-8778',
    })

    expect(configDir).toBe(join(userData, 'coral-dev-8778'))
    await expect(readFile(join(configDir, 'config.toml'), 'utf8')).resolves.toBe(
      'version = 1\n\n[server]\nbind_addr = "127.0.0.1:8778"\n',
    )
    await expect(readFile(join(userData, 'coral', 'config.toml'), 'utf8')).resolves.toContain(
      'bind_addr = "127.0.0.1:0"',
    )
  })
})
