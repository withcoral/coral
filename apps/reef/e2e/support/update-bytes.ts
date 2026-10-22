import type { RunUpdate } from './providers.ts'

/**
 * The bytes an observed Run's MCP updates take, without its Run identity. Run
 * identities can differ in length, so two Runs' updates can differ in size by
 * them alone. Each update holds the identity in its text and structured copies.
 */
export const updateBytes = ({
  responses,
  update,
}: {
  responses: string[]
  update: Pick<RunUpdate, 'run_id'>
}) =>
  responses.reduce(
    (total, response) => total + Buffer.byteLength(response.replaceAll(update.run_id, '')),
    0,
  )
