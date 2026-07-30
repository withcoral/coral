/**
 * Command-line surface for the forge.
 *
 * Parsing lives apart from the entry point so it can be tested without running
 * a build or touching the network.
 */

/** The two halves of the pipeline, deliberately kept separate. */
export type CommandName = 'fetch' | 'build'

export interface Command {
  name: CommandName
  /** API identifier, matching a directory under `apis/`. */
  api: string
  /** For `build`: report what would change instead of writing it. */
  check: boolean
}

export class UsageError extends Error {}

export const USAGE = `openapi-forge — build OpenAPI 3.0 descriptors from provider docs and samples

Usage:
  node src/index.ts fetch --api <name>          refresh apis/<name>/snapshot from upstream
  node src/index.ts build --api <name>          regenerate the descriptor from the snapshot
  node src/index.ts build --api <name> --check  fail if the descriptor is out of date

'fetch' is the only command that uses the network. 'build' is deterministic:
the same snapshot always produces the same descriptor, which is what lets CI
check the committed output for drift.`

export function parseArgs(argv: readonly string[]): Command {
  const [name, ...rest] = argv
  if (name === undefined || name === '--help' || name === '-h') {
    throw new UsageError(USAGE)
  }
  if (name !== 'fetch' && name !== 'build') {
    throw new UsageError(`unknown command '${name}'\n\n${USAGE}`)
  }

  let api: string | undefined
  let check = false
  for (let index = 0; index < rest.length; index += 1) {
    const argument = rest[index]
    if (argument === '--api') {
      const value = rest[index + 1]
      if (value === undefined || value.startsWith('--')) {
        throw new UsageError('--api requires a value')
      }
      api = value
      index += 1
      continue
    }
    if (argument === '--check') {
      check = true
      continue
    }
    throw new UsageError(`unknown option '${argument}'\n\n${USAGE}`)
  }

  if (api === undefined) {
    throw new UsageError(`${name} requires --api <name>`)
  }
  if (check && name !== 'build') {
    throw new UsageError('--check applies to build only')
  }
  return { name, api, check }
}
