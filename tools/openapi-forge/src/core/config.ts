/**
 * Per-API configuration: `apis/<name>/config.yaml`.
 *
 * Holds what a human decides — which operations are in scope, what the
 * descriptor is called, where it is written — as opposed to what is extracted
 * from upstream.
 */

import { readFile } from 'node:fs/promises'
import { dirname, isAbsolute, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import { parse } from 'yaml'

/** Package root, resolved from this module rather than the process cwd. */
export const PACKAGE_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..')

export class ConfigError extends Error {}

/**
 * One declared security scheme.
 *
 * The endpoints are here rather than extracted because providers document them
 * in prose, not in a form worth scraping. `tokenClass` is what ties the scheme
 * to the credential class an operation's documentation names.
 */
export interface SecuritySchemeConfig {
  name: string
  /** Credential class this scheme represents, e.g. `bot` or `user`. */
  tokenClass: string
  description: string
  authorizationUrl: string
  tokenUrl: string
}

export interface ForgeConfig {
  api: string
  title: string
  description: string
  /** Absolute URL every operation path is relative to. */
  serverUrl: string
  /** Operation identifiers in scope, in the provider's own casing. */
  methods: string[]
  /** Empty when the API's auth is not modelled in the descriptor. */
  securitySchemes: SecuritySchemeConfig[]
  /** Directory holding config, overlay, and snapshot. */
  apiDir: string
  snapshotDir: string
  overlayPath: string
  /** Where `build` writes the descriptor. */
  outputPath: string
}

interface RawConfig {
  api?: unknown
  title?: unknown
  description?: unknown
  serverUrl?: unknown
  output?: unknown
  scope?: { methods?: unknown }
  security?: { schemes?: unknown }
}

export function apiDir(api: string): string {
  return join(PACKAGE_ROOT, 'apis', api)
}

export async function loadConfig(api: string): Promise<ForgeConfig> {
  const dir = apiDir(api)
  const path = join(dir, 'config.yaml')
  let raw: string
  try {
    raw = await readFile(path, 'utf8')
  } catch {
    throw new ConfigError(`no configuration at ${path}`)
  }
  return parseConfig(raw, dir, path)
}

export function parseConfig(raw: string, dir: string, path: string): ForgeConfig {
  const parsed = parse(raw) as RawConfig | null
  if (parsed === null || typeof parsed !== 'object') {
    throw new ConfigError(`${path} is not a YAML mapping`)
  }

  const api = requireString(parsed.api, 'api', path)
  const output = requireString(parsed.output, 'output', path)
  const methods = parsed.scope?.methods
  if (!Array.isArray(methods) || methods.length === 0) {
    throw new ConfigError(`${path}: scope.methods must be a non-empty list`)
  }
  const seen = new Set<string>()
  for (const method of methods) {
    if (typeof method !== 'string') {
      throw new ConfigError(`${path}: scope.methods entries must be strings`)
    }
    const key = method.toLowerCase()
    if (seen.has(key)) {
      throw new ConfigError(`${path}: scope.methods lists '${method}' more than once`)
    }
    seen.add(key)
  }

  return {
    api,
    title: requireString(parsed.title, 'title', path),
    description: requireString(parsed.description, 'description', path),
    serverUrl: requireString(parsed.serverUrl, 'serverUrl', path),
    methods: (methods as string[]).toSorted((left, right) => left.localeCompare(right)),
    securitySchemes: parseSecuritySchemes(parsed.security?.schemes, path),
    apiDir: dir,
    snapshotDir: join(dir, 'snapshot'),
    overlayPath: join(dir, 'overlay.yaml'),
    outputPath: isAbsolute(output) ? output : resolve(dir, output),
  }
}

function parseSecuritySchemes(value: unknown, path: string): SecuritySchemeConfig[] {
  if (value === undefined) {
    return []
  }
  if (!Array.isArray(value)) {
    throw new ConfigError(`${path}: security.schemes must be a list`)
  }
  const schemes = value.map((entry, index) => {
    const where = `${path}: security.schemes[${index}]`
    if (typeof entry !== 'object' || entry === null) {
      throw new ConfigError(`${where} must be a mapping`)
    }
    const scheme = entry as Record<string, unknown>
    return {
      name: requireString(scheme.name, 'name', where),
      tokenClass: requireString(scheme.tokenClass, 'tokenClass', where),
      description: requireString(scheme.description, 'description', where),
      authorizationUrl: requireString(scheme.authorizationUrl, 'authorizationUrl', where),
      tokenUrl: requireString(scheme.tokenUrl, 'tokenUrl', where),
    }
  })

  // Both are keys elsewhere — scheme names in components, token classes when
  // matching an operation's documented credentials — so duplicates would
  // silently drop one.
  for (const field of ['name', 'tokenClass'] as const) {
    const seen = new Set<string>()
    for (const scheme of schemes) {
      if (seen.has(scheme[field])) {
        throw new ConfigError(`${path}: two security schemes share the ${field} '${scheme[field]}'`)
      }
      seen.add(scheme[field])
    }
  }
  return schemes
}

function requireString(value: unknown, field: string, path: string): string {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new ConfigError(`${path}: '${field}' must be a non-empty string`)
  }
  return value
}
