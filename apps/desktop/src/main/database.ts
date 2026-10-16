import { readDesktopFile } from './desktop-file'

/**
 * The database the runtime keeps its state in. Without a `database` section in
 * `desktop.json`, the runtime uses its own SQLite file in `runtime/`.
 */
export type Database = { backend: 'sqlite' } | { backend: 'postgres'; url: string }

/**
 * Reads the `database` section of `desktop.json`. Desktop never writes it; the
 * person sets it while Coral is not running.
 *
 * Throws when the section is invalid, so that a mistake never puts the runtime
 * back on SQLite. No error names the URL, which can hold a password.
 */
export async function readDatabase(file: string): Promise<Database> {
  const section = (await readDesktopFile(file)).database
  if (section === undefined) return { backend: 'sqlite' }
  if (typeof section !== 'object' || section === null || Array.isArray(section))
    throw new Error(`${file}: database must be an object`)
  const { backend, url, ...rest } = section as Record<string, unknown>
  const unknown = Object.keys(rest)
  if (unknown.length > 0) throw new Error(`${file}: database has an unknown key, ${unknown[0]}`)
  if (backend !== 'postgres') throw new Error(`${file}: database.backend must be "postgres"`)
  if (typeof url !== 'string' || !/^postgres(ql)?:\/\//.test(url))
    throw new Error(`${file}: database.url must be a postgres:// URL`)
  return { backend, url }
}
