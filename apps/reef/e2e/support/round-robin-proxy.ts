import {
  createServer as createHttpServer,
  request as httpRequest,
  Agent,
  type IncomingHttpHeaders,
  type OutgoingHttpHeaders,
  type Server as HttpServer,
} from 'node:http'
import {
  connect,
  constants,
  createServer as createHttp2Server,
  type ClientHttp2Session,
  type Http2Server,
  type Http2Session,
  type ServerHttp2Stream,
} from 'node:http2'
import type { Socket } from 'node:net'

/** One request the proxy forwarded, as the client saw it. */
export interface ProxiedRequest {
  /** The proxy front the client called: `grpc` (HTTP/2), or `mcp` or `auth` (HTTP/1.1). */
  front: 'grpc' | 'mcp' | 'auth'
  /** The client connection the request arrived on, numbered per front from 1. */
  connection: number
  /** The index of the Coral Instance that served it. */
  instance: number
  method: string
  path: string
  /** The HTTP status, or 0 when the backend sent no response. */
  status: number
  /** gRPC status and message from the response's headers or trailers. */
  grpcStatus?: string
  grpcMessage?: string
}

/** One Coral Instance's own listener origins. */
export interface Backend {
  grpcURL: string
  mcpURL: string
  authURL: string
}

const listen = (server: HttpServer | Http2Server) =>
  new Promise<number>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => resolve((server.address() as { port: number }).port))
  })

const close = (server: HttpServer | Http2Server) =>
  new Promise<void>((resolve) => server.close(() => resolve()))

// HTTP/2 forbids connection-specific headers; Node rejects them on forwarding.
const hopByHop = new Set([
  'connection',
  'keep-alive',
  'proxy-connection',
  'transfer-encoding',
  'upgrade',
  'http2-settings',
])

function forwardable(headers: IncomingHttpHeaders | OutgoingHttpHeaders) {
  return Object.fromEntries(Object.entries(headers).filter(([name]) => !hopByHop.has(name)))
}

/**
 * Balances each request, not each connection, across Coral Instances. Reef
 * keeps one HTTP/2 connection to Coral, so a per-connection balancer would
 * never move its requests between instances. Requests alternate between
 * instances unless a scenario pins them to one. A request in a 2025-11-25 MCP
 * session goes to the instance that issued the session, which holds it in
 * memory; a 2026-07-28 request has no session. Each forwarded request is
 * recorded with the connection it arrived on and the instance that served it.
 * The proxy listens first, so each Coral Instance can name it as its public
 * origin; `serve` then names the instances behind it.
 */
export async function startRoundRobinProxy() {
  let backends: Backend[] = []
  let next = 0
  let pinned: number | undefined
  const requests: ProxiedRequest[] = []
  const choose = () => {
    if (pinned !== undefined) return pinned
    const chosen = next
    next = (next + 1) % backends.length
    return chosen
  }

  // gRPC: one HTTP/2 session per Coral Instance, shared by every client stream.
  const upstreams = new Map<number, ClientHttp2Session>()
  const upstream = (instance: number) => {
    const existing = upstreams.get(instance)
    if (existing && !existing.closed && !existing.destroyed) return existing
    const session = connect(backends[instance].grpcURL)
    const forget = () => {
      if (upstreams.get(instance) === session) upstreams.delete(instance)
    }
    session.on('error', forget)
    session.on('close', forget)
    upstreams.set(instance, session)
    return session
  }
  const grpcSessions = new Set<Http2Session>()
  const sessionIds = new WeakMap<Http2Session, number>()
  let grpcConnections = 0
  const grpc = createHttp2Server()
  grpc.on('session', (session) => {
    sessionIds.set(session, ++grpcConnections)
    grpcSessions.add(session)
    session.once('close', () => grpcSessions.delete(session))
  })
  grpc.on('stream', (stream: ServerHttp2Stream, headers) => {
    if (!backends.length) return stream.close(constants.NGHTTP2_REFUSED_STREAM)
    const instance = choose()
    const record: ProxiedRequest = {
      front: 'grpc',
      connection: sessionIds.get(stream.session!) ?? 0,
      instance,
      method: String(headers[':method']),
      path: String(headers[':path']),
      status: 0,
    }
    requests.push(record)
    let forwarded: ReturnType<ClientHttp2Session['request']>
    try {
      forwarded = upstream(instance).request(forwardable(headers), {
        endStream: stream.endAfterHeaders,
      })
    } catch {
      stream.close(constants.NGHTTP2_INTERNAL_ERROR)
      return
    }
    let trailers: OutgoingHttpHeaders | undefined
    const grpcResult = (block: IncomingHttpHeaders) => {
      if (block['grpc-status'] !== undefined) record.grpcStatus = String(block['grpc-status'])
      if (block['grpc-message'] !== undefined)
        record.grpcMessage = decodeURIComponent(String(block['grpc-message']))
    }
    forwarded.on('response', (response, flags) => {
      record.status = Number(response[':status'])
      grpcResult(response)
      const ended = Boolean(flags & constants.NGHTTP2_FLAG_END_STREAM)
      if (stream.closed) return
      stream.respond(forwardable(response), { endStream: ended, waitForTrailers: !ended })
    })
    forwarded.on('trailers', (block) => {
      grpcResult(block)
      trailers = forwardable(block)
    })
    stream.on('wantTrailers', () => {
      if (trailers && Object.keys(trailers).length) stream.sendTrailers(trailers)
      else stream.close()
    })
    if (!stream.endAfterHeaders) stream.pipe(forwarded)
    forwarded.pipe(stream)
    // A reset on either side resets the other, as a client cancellation must.
    // A response that ended normally finishes through the pipe and its trailers.
    forwarded.on('close', () => {
      if (!forwarded.readableEnded && !stream.closed)
        stream.close(forwarded.rstCode || constants.NGHTTP2_INTERNAL_ERROR)
    })
    forwarded.on('error', () => {})
    stream.on('error', () => {})
    stream.on('close', () => {
      if (!forwarded.closed) forwarded.close(constants.NGHTTP2_CANCEL)
    })
  })

  // MCP and auth: plain HTTP/1.1, streamed both ways so held responses stay open.
  const agents = new Map<string, Agent>()
  const sockets = new Set<Socket>()
  // A 2025-11-25 MCP session lives in the memory of the instance that issued it.
  const mcpSessions = new Map<string, number>()
  const http1 = (front: 'mcp' | 'auth') => {
    const socketIds = new WeakMap<Socket, number>()
    let connections = 0
    const server = createHttpServer((request, response) => {
      if (!backends.length) return void response.writeHead(502).end()
      const session = request.headers['mcp-session-id']
      const instance =
        (typeof session === 'string' ? mcpSessions.get(session) : undefined) ?? choose()
      const record: ProxiedRequest = {
        front,
        connection: socketIds.get(request.socket) ?? 0,
        instance,
        method: request.method ?? '',
        path: request.url ?? '',
        status: 0,
      }
      requests.push(record)
      const target = new URL(backends[instance][`${front}URL`])
      const key = `${front}:${instance}`
      const forwarded = httpRequest(
        {
          host: target.hostname,
          port: target.port,
          method: request.method,
          path: request.url,
          // The Host header stays the proxy's origin, which Coral's public URL names.
          headers: forwardable(request.headers),
          agent: agents.get(key) ?? agents.set(key, new Agent({ keepAlive: true })).get(key),
        },
        (answer) => {
          record.status = answer.statusCode ?? 0
          const issued = answer.headers['mcp-session-id']
          if (front === 'mcp' && typeof issued === 'string') mcpSessions.set(issued, instance)
          response.writeHead(answer.statusCode ?? 502, forwardable(answer.headers))
          answer.pipe(response)
        },
      )
      forwarded.on('error', () => {
        if (!response.headersSent) response.writeHead(502).end()
        else response.destroy()
      })
      response.on('close', () => forwarded.destroy())
      request.pipe(forwarded)
    })
    server.on('connection', (socket: Socket) => {
      socketIds.set(socket, ++connections)
      sockets.add(socket)
      socket.once('close', () => sockets.delete(socket))
    })
    return server
  }
  const mcp = http1('mcp')
  const auth = http1('auth')

  const servers = [grpc, mcp, auth]
  let ports: number[]
  try {
    ports = await Promise.all(servers.map(listen))
  } catch (error) {
    // A front that failed to bind takes the others down with it.
    await Promise.all(servers.map(close))
    throw error
  }
  const [grpcPort, mcpPort, authPort] = ports
  return {
    grpcURL: `http://127.0.0.1:${grpcPort}`,
    mcpURL: `http://127.0.0.1:${mcpPort}`,
    authURL: `http://127.0.0.1:${authPort}`,
    /** Names the Coral Instances behind the proxy, in index order. */
    serve(instances: Backend[]) {
      if (instances.length < 2) throw new Error('A round-robin proxy needs two Coral Instances')
      backends = instances
    },
    /** Sends every later request outside an MCP session to one instance, or alternates again with `undefined`. */
    pin(instance: number | undefined) {
      if (instance !== undefined && !backends[instance]) throw new Error('No such Coral Instance')
      pinned = instance
    },
    /** Every request forwarded so far, oldest first. */
    requests: () => requests.map((request) => ({ ...request })),
    async stop() {
      for (const session of grpcSessions) session.destroy()
      for (const socket of sockets) socket.destroy()
      await Promise.all([close(grpc), close(mcp), close(auth)])
      for (const session of upstreams.values()) session.destroy()
      for (const agent of agents.values()) agent.destroy()
    },
  }
}
