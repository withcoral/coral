import { readDesktopFile } from './desktop-file'

/**
 * Whether the runtime runs semantic search. Without a `search` section in
 * `desktop.json`, it does: the runtime's default.
 */
export type Search = { semantic: boolean }

/**
 * Reads the `search` section of `desktop.json`. Desktop never writes it; the
 * person sets it while Coral is not running.
 *
 * Throws when the section is invalid, so that a mistake never turns semantic
 * search back on.
 */
export async function readSearch(file: string): Promise<Search> {
  const section = (await readDesktopFile(file)).search
  if (section === undefined) return { semantic: true }
  if (typeof section !== 'object' || section === null || Array.isArray(section))
    throw new Error(`${file}: search must be an object`)
  const { semantic, ...rest } = section as Record<string, unknown>
  const unknown = Object.keys(rest)
  if (unknown.length > 0) throw new Error(`${file}: search has an unknown key, ${unknown[0]}`)
  if (typeof semantic !== 'boolean')
    throw new Error(`${file}: search.semantic must be true or false`)
  return { semantic }
}
