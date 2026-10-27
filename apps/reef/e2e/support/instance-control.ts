import { execFile } from 'node:child_process'
import { createServer, type IncomingMessage } from 'node:http'
import { promisify } from 'node:util'

import type { startRoundRobinProxy } from './round-robin-proxy.ts'

const exec = promisify(execFile)
// Coral identifiers are URL-safe text; anything else never reaches the query.
const identifier = /^[A-Za-z0-9_-]{1,128}$/

async function body(request: IncomingMessage) {
  let text = ''
  for await (const chunk of request) text += chunk
  return JSON.parse(text)
}

/**
 * Lets the multi-instance suite's scenarios steer the round-robin proxy and
 * read which Coral Instance holds a Run. Coral exposes a Coral Instance's id
 * only in the Run row, so the control learns each proxy index's id from the
 * first Run a scenario starts through it: the accepting instance holds it.
 * It reads the database with `psql` and never writes to it.
 */
export async function startInstanceControl(
  proxy: Awaited<ReturnType<typeof startRoundRobinProxy>>,
  postgresURL: string,
  env: NodeJS.ProcessEnv,
) {
  try {
    await exec('psql', ['--version'], { env, timeout: 10_000 })
  } catch {
    throw new Error('The multi-instance suite reads Run holders with psql; install it')
  }
  const ids: (string | undefined)[] = []
  const holderOf = async (run: string) => {
    const [, workspace, id] = /^workspaces\/([^/]+)\/runs\/([^/]+)$/.exec(run) ?? []
    if (!identifier.test(workspace ?? '') || !identifier.test(id ?? ''))
      throw new Error(`Name a Run, not ${JSON.stringify(run)}`)
    const { stdout } = await exec(
      'psql',
      [
        '-XAtq',
        '-v',
        'ON_ERROR_STOP=1',
        '-d',
        postgresURL,
        '-c',
        `SELECT holder_instance_id FROM program_run_records WHERE workspace_id = '${workspace}' AND id = '${id}'`,
      ],
      { env, timeout: 10_000 },
    )
    return stdout.trim() || undefined
  }
  const server = createServer(async (request, response) => {
    const reply = (status: number, value: unknown) => {
      response.writeHead(status, { 'Content-Type': 'application/json' })
      response.end(JSON.stringify(value))
    }
    try {
      const url = new URL(request.url ?? '/', 'http://control')
      if (request.method === 'POST' && url.pathname === '/pin') {
        const { instance } = await body(request)
        proxy.pin(instance ?? undefined)
        return reply(200, {})
      }
      if (request.method === 'GET' && url.pathname === '/requests')
        return reply(200, proxy.requests())
      if (request.method === 'POST' && url.pathname === '/accepted') {
        const { instance, run } = await body(request)
        const holder = await holderOf(run)
        if (!holder) return reply(409, { error: `${run} has no holder` })
        const known = ids.indexOf(holder)
        if ((ids[instance] ?? holder) !== holder || (known !== -1 && known !== instance))
          return reply(409, { error: `${run} is held by another Coral Instance` })
        ids[instance] = holder
        return reply(200, { holder })
      }
      if (request.method === 'GET' && url.pathname === '/holder') {
        const holder = await holderOf(url.searchParams.get('run') ?? '')
        const instance = holder ? ids.indexOf(holder) : -1
        return reply(200, { holder: holder ?? null, instance: instance === -1 ? null : instance })
      }
      reply(404, { error: 'Unknown control' })
    } catch (error) {
      reply(500, { error: error instanceof Error ? error.message : 'Control failed' })
    }
  })
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })
  return {
    url: `http://127.0.0.1:${(server.address() as { port: number }).port}`,
    stop: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections()
        server.close(() => resolve())
      }),
  }
}
