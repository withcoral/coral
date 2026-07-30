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

export interface ForgeConfig {
  api: string
  title: string
  description: string
  /** Absolute URL every operation path is relative to. */
  serverUrl: string
  /** Operation identifiers in scope, in the provider's own casing. */
  methods: string[]
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
    apiDir: dir,
    snapshotDir: join(dir, 'snapshot'),
    overlayPath: join(dir, 'overlay.yaml'),
    outputPath: isAbsolute(output) ? output : resolve(dir, output),
  }
}

function requireString(value: unknown, field: string, path: string): string {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new ConfigError(`${path}: '${field}' must be a non-empty string`)
  }
  return value
}
