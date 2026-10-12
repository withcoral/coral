import { mkdtemp, readdir, rm, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, expect, it } from 'vitest'

import { logSink } from './logs'

let dir: string

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'coral-desktop-logs-'))
})

afterEach(async () => {
  await rm(dir, { recursive: true, force: true })
})

it('rotates a log that grows past 5 MB while the process runs', async () => {
  const write = logSink(dir, 'coral')
  const megabyte = `${'x'.repeat(1024 * 1024 - 1)}\n`
  for (let written = 0; written < 7; written++) write(megabyte)

  await expect.poll(async () => (await readdir(dir)).sort()).toEqual(['coral.1.log', 'coral.log'])
  // Writes reach the file asynchronously; wait for all seven megabytes.
  const sizes = async () =>
    Promise.all(
      ['coral.log', 'coral.1.log'].map(async (name) => (await stat(join(dir, name))).size),
    )
  await expect.poll(async () => (await sizes()).reduce((a, b) => a + b)).toBe(7 * 1024 * 1024)
  const [current, rotated] = await sizes()
  expect(current).toBeLessThanOrEqual(5 * 1024 * 1024)
  expect(rotated).toBeLessThanOrEqual(5 * 1024 * 1024)
})
