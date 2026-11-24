import { execFile } from 'node:child_process'
import { createServer, type IncomingMessage } from 'node:http'
import { promisify } from 'node:util'

import type { startRoundRobinProxy } from './round-robin-proxy.ts'

const exec = promisify(execFile)
async function body(request: IncomingMessage) {
  let text = ''
  for await (const chunk of request) text += chunk
  return JSON.parse(text)
}

/**
 * Lets the multi-instance suite's scenarios steer the round-robin proxy and
 * read which Coral Instance holds a Run. `ids` names the Coral Instance behind
 * each proxy index, as each printed it at startup. The control reads the
 * database with `psql` and never writes to it.
 */
export async function startInstanceControl(
  proxy: Awaited<ReturnType<typeof startRoundRobinProxy>>,
  postgresURL: string,
  env: NodeJS.ProcessEnv,
  ids: string[],
) {
  try {
    await exec('psql', ['--version'], { env, timeout: 10_000 })
  } catch {
    throw new Error('The multi-instance suite reads Run holders with psql; install it')
  }
  const holderOf = async (run: string) => {
    const [, workspace, id] = /^workspaces\/([^/]+)\/runs\/([^/]+)$/.exec(run) ?? []
    if (!workspace || !id) throw new Error(`Name a Run, not ${JSON.stringify(run)}`)
    // psql quotes each variable itself, and reads variables only from a script.
    const query = exec(
      'psql',
      [
        '-XAtq',
        '-v',
        'ON_ERROR_STOP=1',
        '-v',
        `workspace=${workspace}`,
        '-v',
        `run=${id}`,
        '-d',
        postgresURL,
      ],
      { env, timeout: 10_000 },
    )
    query.child.stdin?.end(
      'SELECT holder_instance_id, execution_started_at_unix_nanos IS NOT NULL, run_executor_kind' +
        " FROM program_run_records WHERE workspace_id = :'workspace' AND id = :'run';",
    )
    const { stdout } = await query
    // Unaligned output separates columns with `|` and prints NULL as nothing.
    const [holder, started, kind] = stdout.trim().split('|')
    return { holder: holder || null, started: started === 't', kind: kind || null }
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
      if (request.method === 'GET' && url.pathname === '/holder') {
        const row = await holderOf(url.searchParams.get('run') ?? '')
        const instance = row.holder ? ids.indexOf(row.holder) : -1
        return reply(200, { ...row, instance: instance === -1 ? null : instance })
      }
      reply(404, { error: 'Unknown control' })
    } catch (error) {
      reply(500, { error: error instanceof Error ? error.message : 'Control failed' })
    }
  })
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject)
      resolve()
    })
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
