import {
  mkdirSync,
  readFileSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import { dirname } from 'node:path'
import { gte, valid } from 'semver'

export const UPDATE_INTENT_TTL_MS = 10 * 60 * 1000

interface UpdateIntent {
  targetVersion: string
  createdAt: number
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message
  return String(error)
}

function isMissingFile(error: unknown): boolean {
  return (
    error instanceof Error &&
    'code' in error &&
    (error as NodeJS.ErrnoException).code === 'ENOENT'
  )
}

export function writeUpdateIntent(
  filePath: string,
  targetVersion: string,
  now = Date.now(),
): void {
  if (!valid(targetVersion)) {
    throw new Error(`Invalid update target version: ${targetVersion}`)
  }
  mkdirSync(dirname(filePath), { recursive: true })
  const temporaryPath = `${filePath}.${process.pid}.tmp`
  try {
    writeFileSync(
      temporaryPath,
      JSON.stringify({ targetVersion, createdAt: now } satisfies UpdateIntent),
      { encoding: 'utf8', mode: 0o600 },
    )
    renameSync(temporaryPath, filePath)
  } catch (error) {
    try {
      rmSync(temporaryPath, { force: true })
    } catch {
      // Preserve the original write failure.
    }
    throw error
  }
}

export function clearUpdateIntent(filePath: string): void {
  rmSync(filePath, { force: true })
}

export function discardUpdateIntent(filePath: string): void {
  try {
    clearUpdateIntent(filePath)
  } catch (error) {
    console.error(`[coral-updater] failed to clear update intent: ${errorMessage(error)}`)
  }
}

function parseUpdateIntent(value: string): UpdateIntent | null {
  let parsed: unknown
  try {
    parsed = JSON.parse(value)
  } catch {
    return null
  }
  if (!parsed || typeof parsed !== 'object') return null

  const { targetVersion, createdAt } = parsed as Partial<UpdateIntent>
  if (typeof targetVersion !== 'string' || !valid(targetVersion)) return null
  if (typeof createdAt !== 'number' || !Number.isFinite(createdAt)) return null
  return { targetVersion, createdAt }
}

export function shouldExitForUpdateIntent(
  filePath: string,
  currentVersion: string,
  now = Date.now(),
): boolean {
  let serialized: string | null
  try {
    serialized = readFileSync(filePath, 'utf8')
  } catch (error) {
    if (isMissingFile(error)) return false
    console.error(`[coral-updater] failed to read update intent: ${errorMessage(error)}`)
    return false
  }

  const intent = parseUpdateIntent(serialized)
  const age = intent ? now - intent.createdAt : Number.NaN
  if (
    !intent ||
    age < 0 ||
    age > UPDATE_INTENT_TTL_MS ||
    !valid(currentVersion)
  ) {
    discardUpdateIntent(filePath)
    return false
  }

  return !gte(currentVersion, intent.targetVersion)
}
