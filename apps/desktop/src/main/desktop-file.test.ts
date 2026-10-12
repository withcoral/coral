import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { readToldStillRunning, writeToldStillRunning } from './background'
import { readDesktopFile, updateDesktopFile } from './desktop-file'
import { resolvePorts } from './ports'

describe('desktop.json', () => {
  let dir: string
  let file: string
  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'coral-desktop-file-'))
    file = join(dir, 'desktop.json')
  })
  afterEach(() => rm(dir, { recursive: true, force: true }))

  it('keeps the ports when the notice is kept, and the notice when a port moves', async () => {
    const first = await resolvePorts(file)
    expect(await readToldStillRunning(file)).toBe(false)
    await writeToldStillRunning(file)
    expect(await readToldStillRunning(file)).toBe(true)
    const second = await resolvePorts(file)
    expect({ mcp: second.mcp, reef: second.reef }).toEqual({ mcp: first.mcp, reef: first.reef })

    const moved = await resolvePorts(file, { replace: 'mcp' })
    expect(moved.mcp).not.toBe(first.mcp)
    expect(await readDesktopFile(file)).toEqual({
      ports: { mcp: moved.mcp, reef: first.reef },
      toldStillRunning: true,
    })
  })

  it('applies updates that start together one after another', async () => {
    await Promise.all([
      updateDesktopFile(file, { keepRunning: false }),
      updateDesktopFile(file, { other: 1 }),
      updateDesktopFile(file, { keepRunning: true }),
    ])
    expect(await readDesktopFile(file)).toEqual({ keepRunning: true, other: 1 })
  })

  it('reads a missing file as empty, and refuses one that holds no object', async () => {
    expect(await readDesktopFile(file)).toEqual({})
    await writeFile(file, '[]')
    await expect(readDesktopFile(file)).rejects.toThrow(/desktop\.json/)
    await expect(updateDesktopFile(file, { keepRunning: true })).rejects.toThrow(/desktop\.json/)
    // The refused update changed nothing.
    expect(await readFile(file, 'utf8')).toBe('[]')
  })
})
