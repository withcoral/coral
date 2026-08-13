import { create } from '@bufbuild/protobuf'
import { describe, expect, it } from 'vitest'

import {
  SourceInfoSchema,
  SourceOrigin,
  SourceSchema,
  type Source,
  type SourceInfo,
} from '@/generated/coral/v1/sources_pb'
import {
  SOURCE_PRESETS,
  sourceCreatePath,
  sourceCreatePrefill,
  type SourcePreset,
} from '@/lib/source-presets'
import { catalogEntries } from '@/lib/sources'

const preset: SourcePreset = {
  description: 'Query projects, branches, databases, and endpoints from Neon.',
  name: 'neon',
  specUrl: 'https://neon.tech/api_spec/release/v2.json',
  surfaceType: 'openapi',
}

function bundled(name: string): SourceInfo {
  return create(SourceInfoSchema, {
    description: 'A curated source.',
    installed: false,
    name,
    origin: SourceOrigin.BUNDLED,
    version: '1.0.0',
  })
}

function installed(name: string, origin: SourceOrigin): Source {
  return create(SourceSchema, { name, origin, version: '1.0.0' })
}

describe('catalogEntries presets', () => {
  it('appends a preset nothing else provides', () => {
    const entries = catalogEntries([], [], [preset])

    expect(entries).toEqual([
      {
        description: preset.description,
        installed: false,
        name: 'neon',
        origin: 'preset',
        preset: { specUrl: preset.specUrl, surfaceType: 'openapi' },
        version: '',
      },
    ])
  })

  it('drops a preset that the compiled catalog already ships', () => {
    const entries = catalogEntries([bundled('neon')], [], [preset])

    expect(entries).toHaveLength(1)
    expect(entries[0]?.origin).toBe('bundled')
    expect(entries[0]?.preset).toBeUndefined()
  })

  it('drops a preset the user has already installed', () => {
    const entries = catalogEntries([], [installed('neon', SourceOrigin.IMPORTED)], [preset])

    expect(entries).toHaveLength(1)
    expect(entries[0]?.installed).toBe(true)
    expect(entries[0]?.preset).toBeUndefined()
  })

  it('keeps unrelated presets alongside bundled sources', () => {
    const entries = catalogEntries([bundled('github')], [], [preset])

    expect(entries.map((entry) => entry.name)).toEqual(['github', 'neon'])
  })

  it('ships presets with unique names and https spec URLs', () => {
    const names = SOURCE_PRESETS.map((entry) => entry.name)
    expect(new Set(names).size).toBe(names.length)

    for (const entry of SOURCE_PRESETS) {
      expect(entry.specUrl.startsWith('https://')).toBe(true)
      expect(entry.description.length).toBeGreaterThan(0)
    }
  })
})

describe('sourceCreatePath', () => {
  it('encodes the spec URL into the create link', () => {
    expect(sourceCreatePath('/workspaces/w/sources/install', { name: 'neon', preset })).toBe(
      '/workspaces/w/sources/install?spec=https%3A%2F%2Fneon.tech%2Fapi_spec%2Frelease%2Fv2.json&kind=openapi&name=neon',
    )
  })

  it('round-trips through sourceCreatePrefill', () => {
    // Parsed as a real URL so the link the card renders is what gets read back.
    const path = sourceCreatePath('/install', { name: 'neon', preset })

    expect(sourceCreatePrefill(new URL(path, 'https://coral.test').searchParams)).toEqual({
      name: 'neon',
      surfaceType: 'openapi',
      url: preset.specUrl,
    })
  })
})

describe('sourceCreatePrefill', () => {
  it('returns null without a spec param', () => {
    expect(sourceCreatePrefill(new URLSearchParams())).toBeNull()
  })

  it('rejects a non-https spec so the flow opens empty', () => {
    expect(
      sourceCreatePrefill(new URLSearchParams('spec=http://insecure.test/spec.json')),
    ).toBeNull()
    expect(sourceCreatePrefill(new URLSearchParams('spec=javascript%3Aalert(1)'))).toBeNull()
  })

  it('drops an unrecognised kind rather than trusting it', () => {
    const prefill = sourceCreatePrefill(
      new URLSearchParams('spec=https://api.test/spec.json&kind=graphql'),
    )

    expect(prefill).toEqual({ url: 'https://api.test/spec.json' })
  })

  it('keeps an mcp kind', () => {
    const prefill = sourceCreatePrefill(new URLSearchParams('spec=https://mcp.test/mcp&kind=mcp'))

    expect(prefill?.surfaceType).toBe('mcp')
  })
})
