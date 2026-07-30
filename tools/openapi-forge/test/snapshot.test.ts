import { mkdtemp, readFile, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { beforeEach, describe, expect, it } from 'vitest'

import { Snapshot, SnapshotError, SnapshotWriter } from '../src/core/snapshot.ts'

const FETCHED_AT = '2026-07-30T00:00:00.000Z'

async function tempDir(): Promise<string> {
  return mkdtemp(join(tmpdir(), 'forge-snapshot-'))
}

function bytes(text: string): Uint8Array {
  return new TextEncoder().encode(text)
}

describe('SnapshotWriter', () => {
  let dir: string

  beforeEach(async () => {
    dir = await tempDir()
  })

  it('writes inputs and a manifest sorted by path', async () => {
    const writer = new SnapshotWriter(dir)
    writer.add('samples/b.json', 'https://example.com/b', bytes('raw-b'), 'stored-b')
    writer.add('docs/a.md', 'https://example.com/a', bytes('raw-a'), 'stored-a')

    const manifest = await writer.commit(FETCHED_AT)

    expect(manifest.inputs.map((input) => input.path)).toEqual(['docs/a.md', 'samples/b.json'])
    expect(await readFile(join(dir, 'docs/a.md'), 'utf8')).toBe('stored-a')
    expect(manifest.fetchedAt).toBe(FETCHED_AT)
  })

  /**
   * Samples are stored pruned, so the stored digest cannot answer "did upstream
   * change?". The upstream digest is recorded separately for exactly that.
   */
  it('records the upstream digest separately from the stored digest', async () => {
    const writer = new SnapshotWriter(dir)
    writer.add('samples/a.json', 'https://example.com/a', bytes('the raw body'), 'pruned')

    const [input] = (await writer.commit(FETCHED_AT)).inputs

    expect(input?.upstreamSha256).not.toBe(input?.sha256)
    expect(input?.bytes).toBe('pruned'.length)
  })

  /** Narrowing the scope should shrink the snapshot, not leave orphans. */
  it('deletes files the new manifest no longer references', async () => {
    const first = new SnapshotWriter(dir)
    first.add('docs/a.md', 'https://example.com/a', bytes('a'), 'a')
    first.add('docs/b.md', 'https://example.com/b', bytes('b'), 'b')
    await first.commit(FETCHED_AT)

    const second = new SnapshotWriter(dir)
    second.add('docs/a.md', 'https://example.com/a', bytes('a'), 'a')
    await second.commit(FETCHED_AT)

    const snapshot = await Snapshot.open(dir)
    expect(snapshot.has('docs/b.md')).toBe(false)
    await expect(readFile(join(dir, 'docs/b.md'), 'utf8')).rejects.toThrow()
  })
})

describe('Snapshot', () => {
  let dir: string

  beforeEach(async () => {
    dir = await tempDir()
    const writer = new SnapshotWriter(dir)
    writer.add('docs/a.md', 'https://example.com/a', bytes('raw'), '# A\n')
    writer.add('samples/a.json', 'https://example.com/a.json', bytes('raw'), '{"ok":true}')
    await writer.commit(FETCHED_AT)
  })

  it('reads text and JSON inputs', async () => {
    const snapshot = await Snapshot.open(dir)

    expect(await snapshot.readText('docs/a.md')).toBe('# A\n')
    expect(await snapshot.readJson('samples/a.json')).toEqual({ ok: true })
  })

  it('lists inputs by prefix', async () => {
    const snapshot = await Snapshot.open(dir)

    expect(snapshot.list('docs/').map((input) => input.path)).toEqual(['docs/a.md'])
  })

  /**
   * The overlay is meant to be the only hand-edited input. Verifying digests on
   * read is what keeps a "quick fix" to a snapshot file from surviving the next
   * fetch and quietly disappearing.
   */
  it('rejects a snapshot file that was edited by hand', async () => {
    await writeFile(join(dir, 'docs/a.md'), '# Edited\n')
    const snapshot = await Snapshot.open(dir)

    await expect(snapshot.readText('docs/a.md')).rejects.toThrow(/overlay\.yaml/)
  })

  it('rejects a path that is not in the manifest', async () => {
    const snapshot = await Snapshot.open(dir)

    await expect(snapshot.readText('docs/missing.md')).rejects.toThrow(SnapshotError)
  })

  it('explains how to populate a missing snapshot', async () => {
    await expect(Snapshot.open(await tempDir())).rejects.toThrow(/forge fetch/)
  })
})
