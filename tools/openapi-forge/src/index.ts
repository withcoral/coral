/**
 * Entry point. Resolves the requested API to an adapter and runs one stage of
 * the pipeline.
 */

import process from 'node:process'

import { adapterNames, findAdapter } from './adapters/registry.ts'
import { parseArgs, UsageError } from './cli.ts'

async function main(): Promise<number> {
  let command
  try {
    command = parseArgs(process.argv.slice(2))
  } catch (error) {
    if (error instanceof UsageError) {
      process.stderr.write(`${error.message}\n`)
      return 1
    }
    throw error
  }

  const adapter = findAdapter(command.api)
  if (adapter === undefined) {
    const known = adapterNames()
    const suffix = known.length === 0 ? 'none are registered' : `known APIs: ${known.join(', ')}`
    process.stderr.write(`no adapter for API '${command.api}' (${suffix})\n`)
    return 1
  }

  switch (command.name) {
    case 'fetch':
      await adapter.fetch()
      return 0
    case 'build':
      return (await adapter.build({ check: command.check })) ? 0 : 1
  }
}

main()
  .then((code) => {
    process.exitCode = code
  })
  .catch((error: unknown) => {
    process.stderr.write(`openapi-forge: ${error instanceof Error ? error.message : error}\n`)
    process.exitCode = 2
  })
