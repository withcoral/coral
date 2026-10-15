import { createHash } from 'node:crypto'
import { mkdtemp, mkdir, readFile, readdir, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { stageToolbox, TOOLBOX_RELEASE } from './toolbox.ts'

const binary = 'fixture executable\n'
const license = 'fixture upstream license\n'
const digest = (text: string) => createHash('sha256').update(text).digest('hex')
const release = {
  ...TOOLBOX_RELEASE,
  binaries: {
    darwin: { ...TOOLBOX_RELEASE.binaries.darwin, sha256: digest(binary) },
    linux: { ...TOOLBOX_RELEASE.binaries.linux, sha256: digest(binary) },
    win32: { ...TOOLBOX_RELEASE.binaries.win32, sha256: digest(binary) },
  },
  licenseSha256: digest(license),
}

describe('staging the pinned Toolbox release', () => {
  let root: string
  let directory: string
  const fetch = vi.fn<typeof globalThis.fetch>()

  beforeEach(async () => {
    root = await mkdtemp(join(tmpdir(), 'coral-toolbox-test-'))
    directory = join(root, 'toolbox')
    fetch.mockReset()
    fetch.mockImplementation(
      async (url) => new Response(String(url).endsWith('/LICENSE') ? license : binary),
    )
    vi.stubGlobal('fetch', fetch)
  })

  afterEach(async () => {
    vi.unstubAllGlobals()
    await rm(root, { recursive: true, force: true })
  })

  it.each([
    ['darwin', 'darwin/arm64/toolbox', 'toolbox'],
    ['linux', 'linux/amd64/toolbox', 'toolbox'],
    ['win32', 'windows/amd64/toolbox.exe', 'toolbox.exe'],
  ] as const)(
    'stages an executable and license for the %s package',
    async (platform, path, name) => {
      await stageToolbox(directory, platform, release)

      expect(fetch.mock.calls.map(([url]) => url)).toEqual([
        `https://storage.googleapis.com/mcp-toolbox-for-databases/v${release.version}/${path}`,
        `https://raw.githubusercontent.com/googleapis/mcp-toolbox/v${release.version}/LICENSE`,
      ])
      expect(await readFile(join(directory, name), 'utf8')).toBe(binary)
      expect(await readFile(join(directory, 'LICENSE'), 'utf8')).toBe(license)
      expect(await readFile(join(directory, 'VERSION'), 'utf8')).toBe(`${release.version}\n`)
      if (process.platform !== 'win32')
        expect((await stat(join(directory, name))).mode & 0o111).toBe(0o111)
      expect(await readdir(root)).toEqual(['toolbox'])
    },
  )

  it.each([
    ['binary checksum', 1, 'tampered executable', 200, 'SHA-256 mismatch'],
    ['license checksum', 2, 'tampered license', 200, 'SHA-256 mismatch'],
    ['missing binary', 1, 'Not Found', 404, 'HTTP 404'],
    ['license download', 2, 'Unavailable', 503, 'HTTP 503'],
  ] as const)(
    'preserves the previous stage on %s failure',
    async (_name, call, body, status, error) => {
      await mkdir(directory)
      await writeFile(join(directory, 'toolbox'), 'previous executable')
      if (call === 2) fetch.mockResolvedValueOnce(new Response(binary))
      fetch.mockResolvedValueOnce(new Response(body, { status }))

      await expect(stageToolbox(directory, 'linux', release)).rejects.toThrow(error)

      expect(await readFile(join(directory, 'toolbox'), 'utf8')).toBe('previous executable')
      expect(await readdir(directory)).toEqual(['toolbox'])
      expect(await readdir(root)).toEqual(['toolbox'])
    },
  )

  it('rejects unsupported platforms before downloading', async () => {
    await expect(stageToolbox(directory, 'freebsd', release)).rejects.toThrow(
      'Toolbox is not packaged for freebsd',
    )
    expect(fetch).not.toHaveBeenCalled()
    expect(await readdir(root)).toEqual([])
  })
})
