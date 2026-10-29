import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { writeRuntimeConfig } from './runtime'

describe('writeRuntimeConfig', () => {
  let dir: string
  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'coral-runtime-config-'))
  })
  afterEach(() => rm(dir, { recursive: true, force: true }))

  it.each([
    { checkUpdates: true, check: 'true' },
    { checkUpdates: false, check: 'false' },
  ])('writes [updates] check = $check from checkUpdates', async ({ checkUpdates, check }) => {
    const stateDir = join(dir, 'runtime')

    await writeRuntimeConfig(
      stateDir,
      { grpc: 1, mcp: 2, reef: 3 },
      { backend: 'sqlite' },
      checkUpdates,
    )

    const config = await readFile(join(stateDir, 'config.toml'), 'utf8')
    expect(config).toMatch(new RegExp(`^\\[updates\\]\\ncheck = ${check}$`, 'm'))
  })
})
