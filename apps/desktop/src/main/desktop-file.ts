// `desktop.json`, Desktop's own state in its data folder. Each top-level
// section has one owner: the kept ports (ports.ts) and the background notice
// (background.ts). An owner replaces its section and never another's. The
// person owns the database section (database.ts), which Desktop only reads.
import { mkdir, readFile, rename, writeFile } from 'node:fs/promises'
import { basename, dirname, join } from 'node:path'

/** Reads every section. A missing file has none. Throws when the file is not a JSON object. */
export async function readDesktopFile(file: string): Promise<Record<string, unknown>> {
  let raw: string
  try {
    raw = await readFile(file, 'utf8')
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return {}
    throw error
  }
  const parsed: unknown = JSON.parse(raw)
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed))
    throw new Error(`${file} does not hold a JSON object`)
  return parsed as Record<string, unknown>
}

const updates = new Map<string, Promise<void>>()

/**
 * Replaces the named sections and keeps the others. Updates to one file run
 * one after another, so two owners never write over each other.
 */
export function updateDesktopFile(file: string, sections: Record<string, unknown>): Promise<void> {
  const update = (updates.get(file) ?? Promise.resolve())
    .catch(() => {})
    .then(async () => {
      const current = await readDesktopFile(file)
      await writeAtomic(file, `${JSON.stringify({ ...current, ...sections }, null, 2)}\n`)
    })
  updates.set(file, update)
  return update
}

/** Writes `file` whole or not at all. The temporary file lives in `scratchDir`. */
export async function writeAtomic(
  file: string,
  content: string,
  scratchDir = dirname(file),
): Promise<void> {
  await mkdir(dirname(file), { recursive: true, mode: 0o700 })
  const temporary = join(scratchDir, `.${basename(file)}.${process.pid}.tmp`)
  await writeFile(temporary, content, { encoding: 'utf8', mode: 0o600 })
  await rename(temporary, file)
}
