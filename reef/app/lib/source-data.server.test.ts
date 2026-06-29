import { describe, expect, it } from 'vitest'

import type { Source, SourceInfo } from '@/generated/coral/v1/sources_pb'
import { SourceOrigin } from '@/generated/coral/v1/sources_pb'

import { catalogEntries } from './source-data'

describe('catalogEntries', () => {
  it('reports the installed origin when it differs from the discovered manifest', () => {
    const discovered: SourceInfo[] = [
      {
        description: 'GitHub',
        installed: true,
        name: 'github',
        origin: SourceOrigin.BUNDLED,
        version: '1.2.0',
      } as SourceInfo,
    ]
    const installed: Source[] = [
      {
        name: 'github',
        origin: SourceOrigin.IMPORTED,
        version: '2.0.0',
      } as Source,
    ]

    const entries = catalogEntries(discovered, installed)

    expect(entries).toHaveLength(1)
    expect(entries[0]).toMatchObject({
      installed: true,
      name: 'github',
      origin: 'imported',
      version: '2.0.0',
    })
    expect(entries[0].origin).not.toBe('bundled')
  })

  it('keeps the discovered origin when the installed source reports no usable origin', () => {
    const discovered: SourceInfo[] = [
      {
        description: 'GitHub',
        installed: false,
        name: 'github',
        origin: SourceOrigin.BUNDLED,
        version: '1.2.0',
      } as SourceInfo,
    ]
    const installed: Source[] = [
      {
        name: 'github',
        origin: SourceOrigin.UNSPECIFIED,
        version: '',
      } as Source,
    ]

    const entries = catalogEntries(discovered, installed)

    expect(entries[0]).toMatchObject({
      installed: true,
      origin: 'bundled',
      version: '1.2.0',
    })
  })
})
