import { describe, expect, it } from 'vitest'

import { ConfigError, loadConfig, parseConfig } from '../src/core/config.ts'

const VALID = `
api: demo
title: Demo API
description: A demo.
serverUrl: https://example.com/api
output: ../../../out/openapi.yaml
scope:
  methods:
    - b.two
    - a.one
`

describe('parseConfig', () => {
  it('resolves the output path relative to the config file', () => {
    const config = parseConfig(VALID, '/repo/tools/openapi-forge/apis/demo', 'config.yaml')

    expect(config.outputPath).toBe('/repo/tools/out/openapi.yaml')
    expect(config.snapshotDir).toBe('/repo/tools/openapi-forge/apis/demo/snapshot')
    expect(config.overlayPath).toBe('/repo/tools/openapi-forge/apis/demo/overlay.yaml')
  })

  /** Sorting the scope keeps snapshot and descriptor diffs order-independent. */
  it('sorts the configured methods', () => {
    expect(parseConfig(VALID, '/dir', 'config.yaml').methods).toEqual(['a.one', 'b.two'])
  })

  it('rejects a duplicate method regardless of casing', () => {
    const raw = VALID.replace('    - a.one', '    - a.one\n    - A.One')

    expect(() => parseConfig(raw, '/dir', 'config.yaml')).toThrow(/more than once/)
  })

  it('rejects an empty scope', () => {
    const raw = VALID.replace(/scope:[\s\S]*$/, 'scope:\n  methods: []\n')

    expect(() => parseConfig(raw, '/dir', 'config.yaml')).toThrow(/non-empty list/)
  })

  it('names the field that is missing', () => {
    const raw = VALID.replace('serverUrl: https://example.com/api\n', '')

    expect(() => parseConfig(raw, '/dir', 'config.yaml')).toThrow(/'serverUrl'/)
  })

  it('rejects a non-mapping document', () => {
    expect(() => parseConfig('- a\n- b\n', '/dir', 'config.yaml')).toThrow(ConfigError)
  })
})

describe('loadConfig', () => {
  it('loads the committed Slack configuration', async () => {
    const config = await loadConfig('slack')

    expect(config.api).toBe('slack')
    expect(config.serverUrl).toBe('https://slack.com/api')
    expect(config.outputPath).toMatch(/sources\/v4\/slack\/openapi\.yaml$/)
    expect(config.methods).toContain('conversations.history')
  })

  it('reports an unknown API by path', async () => {
    await expect(loadConfig('nope')).rejects.toThrow(/apis\/nope\/config\.yaml/)
  })
})
