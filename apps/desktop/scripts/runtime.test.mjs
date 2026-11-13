import { spawnSync } from 'node:child_process'

import { beforeEach, describe, expect, it, vi } from 'vitest'

import { buildRuntime } from './runtime.mjs'

vi.mock('node:child_process', () => ({ spawnSync: vi.fn() }))

const LINUX = 'x86_64-unknown-linux-gnu'
const MACOS = 'aarch64-apple-darwin'
const WINDOWS = 'x86_64-pc-windows-msvc'
// What `cargo metadata` reports of [workspace.metadata.v8-archive].
const RECORDED = {
  version: '1.2.3',
  sha256: { [LINUX]: 'a'.repeat(64), [MACOS]: 'b'.repeat(64), [WINDOWS]: 'c'.repeat(64) },
}

/**
 * Stands in for rustc and cargo on a host whose `rustc -vV` prints `version`.
 * Returns the `cargo build` runs, which stay empty until one starts.
 */
function toolchain(version) {
  const builds = []
  vi.mocked(spawnSync).mockImplementation((command, args, options) => {
    if (command === 'rustc') return { status: 0, stdout: version }
    if (args[0] === 'metadata') {
      return { status: 0, stdout: JSON.stringify({ metadata: { 'v8-archive': RECORDED } }) }
    }
    builds.push({ args, env: options.env })
    const artifact = {
      reason: 'compiler-artifact',
      target: { name: 'coral-ee' },
      executable: '/target/release/coral-ee',
    }
    return { status: 0, stdout: `${JSON.stringify(artifact)}\n` }
  })
  return builds
}

beforeEach(() => {
  vi.mocked(spawnSync).mockReset()
})

describe('a release runtime build', () => {
  it.each([
    {
      name: 'the host, when it names no target',
      version: `rustc 1.98.0\nhost: ${LINUX}\nrelease: 1.98.0\n`,
      target: undefined,
      digest: RECORDED.sha256[LINUX],
    },
    {
      name: 'a Windows host, whose lines can end in CRLF',
      version: `rustc 1.98.0\r\nhost: ${WINDOWS}\r\nrelease: 1.98.0\r\n`,
      target: undefined,
      digest: RECORDED.sha256[WINDOWS],
    },
    {
      name: 'the target it names, whatever the host',
      version: `rustc 1.98.0\nhost: ${LINUX}\n`,
      target: MACOS,
      digest: RECORDED.sha256[MACOS],
    },
  ])('gives cargo the recorded V8 archive digest of $name', ({ version, target, digest }) => {
    const builds = toolchain(version)

    expect(buildRuntime({ release: true, target })).toBe('/target/release/coral-ee')

    expect(builds).toHaveLength(1)
    expect(builds[0].args).toContain('--release')
    expect(builds[0].env.RUSTY_V8_ARCHIVE_SHA256).toBe(digest)
  })

  // Without this, a new target would ship with an archive that nothing checked.
  it('stops before cargo builds for a target without a recorded digest', () => {
    const builds = toolchain(`host: ${LINUX}\n`)

    expect(() => buildRuntime({ release: true, target: 'riscv64gc-unknown-linux-gnu' })).toThrow(
      'no V8 archive digest for riscv64gc-unknown-linux-gnu',
    )
    expect(builds).toEqual([])
  })
})
