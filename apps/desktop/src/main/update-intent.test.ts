import {
  existsSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import {
  UPDATE_INTENT_TTL_MS,
  clearUpdateIntent,
  shouldExitForUpdateIntent,
  writeUpdateIntent,
} from './update-intent'

const NOW = 1_800_000_000_000

let temporaryDirectory: string
let markerPath: string

beforeEach(() => {
  temporaryDirectory = mkdtempSync(join(tmpdir(), 'coral-update-intent-'))
  markerPath = join(temporaryDirectory, 'update-intent.json')
})

afterEach(() => {
  rmSync(temporaryDirectory, { force: true, recursive: true })
})

function writeMarker(targetVersion = '1.2.4', createdAt = NOW): void {
  writeFileSync(markerPath, JSON.stringify({ targetVersion, createdAt }))
}

describe('update intent', () => {
  it('writes the target version and creation time', () => {
    writeUpdateIntent(markerPath, '1.2.4', NOW)

    expect(JSON.parse(readFileSync(markerPath, 'utf8'))).toEqual({
      targetVersion: '1.2.4',
      createdAt: NOW,
    })
  })

  it('keeps a fresh marker and exits an older rapidly reopened app', () => {
    writeMarker()

    expect(shouldExitForUpdateIntent(markerPath, '1.2.3', NOW + 1000)).toBe(true)
    expect(readFileSync(markerPath, 'utf8')).toBe(
      JSON.stringify({ targetVersion: '1.2.4', createdAt: NOW }),
    )
  })

  it('detects a marker written while an old app waits for the lock', () => {
    expect(shouldExitForUpdateIntent(markerPath, '1.2.3', NOW)).toBe(false)

    writeMarker()

    expect(shouldExitForUpdateIntent(markerPath, '1.2.3', NOW + 1000)).toBe(true)
    expect(existsSync(markerPath)).toBe(true)
  })

  it('keeps the marker until the target version owns the app lock', () => {
    writeMarker()

    expect(shouldExitForUpdateIntent(markerPath, '1.2.4', NOW + 1000)).toBe(false)
    expect(existsSync(markerPath)).toBe(true)

    clearUpdateIntent(markerPath)
    expect(existsSync(markerPath)).toBe(false)
  })

  it('clears a stale marker instead of locking out an older app', () => {
    writeMarker('1.2.4', NOW - UPDATE_INTENT_TTL_MS - 1)

    expect(shouldExitForUpdateIntent(markerPath, '1.2.3', NOW)).toBe(false)
    expect(existsSync(markerPath)).toBe(false)
  })

  it('fails open and clears a malformed marker', () => {
    writeFileSync(markerPath, 'not json')

    expect(shouldExitForUpdateIntent(markerPath, '1.2.3', NOW)).toBe(false)
    expect(existsSync(markerPath)).toBe(false)
  })
})
