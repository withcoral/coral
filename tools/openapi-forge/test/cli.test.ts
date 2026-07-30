import { describe, expect, it } from 'vitest'

import { parseArgs, UsageError } from '../src/cli.ts'

describe('parseArgs', () => {
  it('parses a build command', () => {
    expect(parseArgs(['build', '--api', 'slack'])).toEqual({
      name: 'build',
      api: 'slack',
      check: false,
    })
  })

  it('parses a fetch command', () => {
    expect(parseArgs(['fetch', '--api', 'slack'])).toEqual({
      name: 'fetch',
      api: 'slack',
      check: false,
    })
  })

  it('accepts --check on build', () => {
    expect(parseArgs(['build', '--api', 'slack', '--check']).check).toBe(true)
  })

  it('rejects --check on fetch, which writes the snapshot rather than reading it', () => {
    expect(() => parseArgs(['fetch', '--api', 'slack', '--check'])).toThrow(
      /--check applies to build only/,
    )
  })

  it('requires an API', () => {
    expect(() => parseArgs(['build'])).toThrow(/requires --api/)
  })

  it('rejects a missing --api value rather than swallowing the next flag', () => {
    expect(() => parseArgs(['build', '--api', '--check'])).toThrow(/--api requires a value/)
  })

  it('rejects unknown commands and options', () => {
    expect(() => parseArgs(['publish', '--api', 'slack'])).toThrow(/unknown command 'publish'/)
    expect(() => parseArgs(['build', '--api', 'slack', '--force'])).toThrow(
      /unknown option '--force'/,
    )
  })

  it('reports usage with no arguments', () => {
    expect(() => parseArgs([])).toThrow(UsageError)
  })
})
