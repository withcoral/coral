import { createServer, type Server } from 'node:net'

import { readDesktopFile, updateDesktopFile } from './desktop-file'

/** The listeners whose ports outlive a launch: clients and links store their URLs. */
export type PersistedListener = 'mcp' | 'reef'

export interface Ports {
  grpc: number
  mcp: number
  reef: number
}

/** A persisted port that another process holds. Desktop never moves it silently. */
export class PortInUseError extends Error {
  constructor(
    readonly listener: PersistedListener,
    readonly port: number,
  ) {
    super(`Loopback port ${port} for ${listener} is in use`)
  }
}

const LISTENERS: readonly PersistedListener[] = ['mcp', 'reef']
export const LOOPBACK = '127.0.0.1'

/**
 * Returns this launch's ports. The MCP and Reef ports are allocated once and
 * kept in `file`; `replace` allocates a new one for that listener. gRPC is
 * private to Desktop, so it gets a fresh port every launch.
 *
 * Throws `PortInUseError` when a kept port is taken.
 */
export async function resolvePorts(
  file: string,
  { replace }: { replace?: PersistedListener } = {},
): Promise<Ports> {
  const saved = await readSaved(file)
  // Hold each kept port while new ones are chosen, so the OS cannot hand one
  // out again. The check is advisory: the runtime and Reef bind later.
  const held: Server[] = []
  let grpc: number
  let allocated: number[]
  const missing = LISTENERS.filter(
    (listener) => saved[listener] === undefined || listener === replace,
  )
  try {
    for (const listener of LISTENERS) {
      const port = saved[listener]
      if (port === undefined || listener === replace) continue
      try {
        held.push(await listen(port))
      } catch {
        throw new PortInUseError(listener, port)
      }
    }
    ;[grpc, ...allocated] = await freePorts(1 + missing.length)
  } finally {
    await Promise.all(held.map(close))
  }
  const persisted = { ...saved }
  missing.forEach((listener, index) => (persisted[listener] = allocated[index]))
  if (missing.length > 0) await updateDesktopFile(file, { ports: persisted })
  return { grpc, mcp: persisted.mcp!, reef: persisted.reef! }
}

async function readSaved(file: string): Promise<Partial<Record<PersistedListener, number>>> {
  const ports = ((await readDesktopFile(file)).ports ?? {}) as Record<string, unknown>
  const saved: Partial<Record<PersistedListener, number>> = {}
  for (const listener of LISTENERS) {
    const port = ports[listener]
    if (port === undefined) continue
    if (!Number.isInteger(port) || (port as number) < 1 || (port as number) > 65535)
      throw new Error(`${file} has an invalid ${listener} port`)
    saved[listener] = port as number
  }
  return saved
}

function listen(port: number): Promise<Server> {
  return new Promise((resolve, reject) => {
    // A probe accepts no clients: a connection left open would stall close().
    const server = createServer((socket) => socket.destroy())
    server.once('error', reject)
    server.listen({ host: LOOPBACK, port, exclusive: true }, () => resolve(server))
  })
}

function close(server: Server): Promise<void> {
  return new Promise((resolve) => server.close(() => resolve()))
}

/** Holds every port until all are chosen, so the OS hands out distinct ones. */
async function freePorts(count: number): Promise<number[]> {
  const results = await Promise.allSettled(Array.from({ length: count }, () => listen(0)))
  const servers = results.flatMap((result) => (result.status === 'fulfilled' ? [result.value] : []))
  try {
    const failed = results.find((result) => result.status === 'rejected')
    if (failed) throw failed.reason
    return servers.map((server) => {
      const address = server.address()
      if (typeof address !== 'object' || !address) throw new Error('No loopback port was assigned')
      return address.port
    })
  } finally {
    // Release every listener that did bind, even when another did not.
    await Promise.all(servers.map(close))
  }
}
