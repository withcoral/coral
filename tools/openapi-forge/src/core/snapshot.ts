/**
 * The pinned snapshot of upstream inputs.
 *
 * `fetch` writes it, `build` reads it, and it is committed. That split is what
 * makes builds reproducible: the same snapshot always produces the same
 * descriptor, so CI can drift-check the generated output and an upstream change
 * arrives as a reviewable snapshot diff rather than a surprise.
 *
 * Each entry records two digests. `upstreamSha256` is the hash of the bytes as
 * served, so a re-fetch can tell whether upstream actually changed even when
 * the stored file is a pruned form of it. `sha256` is the hash of the stored
 * file, verified on read — the overlay is meant to be the only hand-edited
 * input, and this is what enforces that.
 */

import { createHash } from 'node:crypto'
import { mkdir, readdir, readFile, rm, writeFile } from 'node:fs/promises'
import { dirname, join, relative, sep } from 'node:path'

const MANIFEST_NAME = 'manifest.json'

export interface SnapshotInput {
  /** Path within the snapshot directory, using forward slashes. */
  path: string
  url: string
  /** Digest of the bytes as served upstream. */
  upstreamSha256: string
  /** Digest of the stored file. */
  sha256: string
  bytes: number
}

export interface SnapshotManifest {
  /** When `fetch` last ran. Informational; builds do not read it. */
  fetchedAt: string
  inputs: SnapshotInput[]
}

export class SnapshotError extends Error {}

export function sha256(data: Uint8Array | string): string {
  return createHash('sha256')
    .update(typeof data === 'string' ? new TextEncoder().encode(data) : data)
    .digest('hex')
}

/** Read side, used by `build`. */
export class Snapshot {
  readonly #dir: string
  readonly #inputs: Map<string, SnapshotInput>
  readonly fetchedAt: string

  private constructor(dir: string, manifest: SnapshotManifest) {
    this.#dir = dir
    this.fetchedAt = manifest.fetchedAt
    this.#inputs = new Map(manifest.inputs.map((input) => [input.path, input]))
  }

  static async open(dir: string): Promise<Snapshot> {
    let raw: string
    try {
      raw = await readFile(join(dir, MANIFEST_NAME), 'utf8')
    } catch {
      throw new SnapshotError(
        `no snapshot at ${dir}; run 'forge fetch' first to populate it from upstream`,
      )
    }
    return new Snapshot(dir, JSON.parse(raw) as SnapshotManifest)
  }

  /** Inputs whose path starts with `prefix`, in manifest order. */
  list(prefix = ''): SnapshotInput[] {
    return [...this.#inputs.values()].filter((input) => input.path.startsWith(prefix))
  }

  has(path: string): boolean {
    return this.#inputs.has(path)
  }

  async readText(path: string): Promise<string> {
    const input = this.#inputs.get(path)
    if (input === undefined) {
      throw new SnapshotError(`'${path}' is not in the snapshot manifest`)
    }
    const bytes = await readFile(join(this.#dir, path))
    const digest = sha256(bytes)
    if (digest !== input.sha256) {
      throw new SnapshotError(
        `'${path}' does not match its manifest digest. Snapshot files are fetched, not authored: ` +
          `re-run 'forge fetch', and put deliberate corrections in overlay.yaml instead.`,
      )
    }
    return new TextDecoder().decode(bytes)
  }

  async readJson<T>(path: string): Promise<T> {
    return JSON.parse(await this.readText(path)) as T
  }
}

/** Write side, used by `fetch`. */
export class SnapshotWriter {
  readonly #dir: string
  readonly #inputs: SnapshotInput[] = []
  readonly #bodies = new Map<string, string>()

  constructor(dir: string) {
    this.#dir = dir
  }

  /**
   * Record one input.
   *
   * `upstream` is what the server returned; `stored` is what gets committed,
   * which may be a pruned form of it.
   */
  add(path: string, url: string, upstream: Uint8Array, stored: string): void {
    this.#inputs.push({
      path,
      url,
      upstreamSha256: sha256(upstream),
      sha256: sha256(stored),
      bytes: new TextEncoder().encode(stored).length,
    })
    this.#bodies.set(path, stored)
  }

  /**
   * Write every recorded input and the manifest, then delete files the
   * manifest no longer references.
   *
   * Pruning matters: narrowing the scope should shrink the snapshot, not leave
   * orphans behind that look like current inputs.
   */
  async commit(fetchedAt: string): Promise<SnapshotManifest> {
    const inputs = this.#inputs.toSorted((left, right) => left.path.localeCompare(right.path))
    const manifest: SnapshotManifest = { fetchedAt, inputs }

    await mkdir(this.#dir, { recursive: true })
    for (const input of inputs) {
      const target = join(this.#dir, input.path)
      await mkdir(dirname(target), { recursive: true })
      await writeFile(target, this.#bodies.get(input.path) ?? '')
    }
    await writeFile(join(this.#dir, MANIFEST_NAME), `${JSON.stringify(manifest, undefined, 2)}\n`)
    await this.#pruneOrphans(new Set(inputs.map((input) => input.path)))
    return manifest
  }

  async #pruneOrphans(keep: Set<string>): Promise<void> {
    for (const path of await listFiles(this.#dir)) {
      if (path !== MANIFEST_NAME && !keep.has(path)) {
        await rm(join(this.#dir, path))
      }
    }
  }
}

async function listFiles(dir: string): Promise<string[]> {
  const entries = await readdir(dir, { recursive: true, withFileTypes: true })
  return entries
    .filter((entry) => entry.isFile())
    .map((entry) => relative(dir, join(entry.parentPath, entry.name)).split(sep).join('/'))
}
