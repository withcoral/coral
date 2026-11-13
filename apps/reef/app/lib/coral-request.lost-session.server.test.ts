import http2 from 'node:http2'
import type { AddressInfo } from 'node:net'

import { Code, ConnectError, createClient } from '@connectrpc/connect'
import { afterEach, describe, expect, it } from 'vitest'

import { WorkspaceService } from '@/generated/coral/management/v1/workspace_pb'

import { managementTransportForRequest } from './coral-request.server'

/**
 * A gRPC server whose first HTTP/2 session ends as Coral's does when h2's
 * rapid-reset guard trips: GOAWAY ENHANCE_YOUR_CALM ("too_many_resets").
 * Later sessions answer every call with an empty message.
 */
async function serverThatDropsItsFirstSession() {
  const received: string[] = []
  const sessions: http2.ServerHttp2Session[] = []
  const server = http2.createServer()
  server.on('session', (session) => {
    sessions.push(session)
    const first = sessions.length === 1
    session.on('stream', (stream, headers) => {
      received.push(String(headers[':path']))
      stream.on('error', () => undefined)
      stream.resume()
      if (first) {
        session.goaway(http2.constants.NGHTTP2_ENHANCE_YOUR_CALM, 0, Buffer.from('too_many_resets'))
        session.destroy()
        return
      }
      stream.respond(
        { ':status': 200, 'content-type': 'application/grpc' },
        { waitForTrailers: true },
      )
      stream.on('wantTrailers', () => stream.sendTrailers({ 'grpc-status': '0' }))
      stream.end(Buffer.alloc(5))
    })
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const { port } = server.address() as AddressInfo
  return {
    baseUrl: `http://127.0.0.1:${port}`,
    received,
    // The client keeps its new session open, and close waits for every session.
    close: () =>
      new Promise<void>((resolve) => {
        server.close(() => resolve())
        for (const session of sessions) session.destroy()
      }),
  }
}

describe('a Coral session that ends mid-call', () => {
  let close: (() => Promise<void>) | undefined
  afterEach(async () => {
    await close?.()
    close = undefined
  })

  async function workspaces() {
    const server = await serverThatDropsItsFirstSession()
    close = server.close
    const transport = managementTransportForRequest(
      { mode: 'disabled', accessToken: null },
      { baseUrl: server.baseUrl, authenticatedCleartextOrigin: null },
    )
    return { client: createClient(WorkspaceService, transport), received: server.received }
  }

  it('sends a call free of side effects again on a new session', async () => {
    const { client, received } = await workspaces()

    await expect(client.listWorkspaces({})).resolves.toBeDefined()
    expect(received).toEqual([
      '/coral.management.v1.WorkspaceService/ListWorkspaces',
      '/coral.management.v1.WorkspaceService/ListWorkspaces',
    ])
  })

  it('never sends a change twice, since Coral may have applied it', async () => {
    const { client, received } = await workspaces()

    const failure = await client.createWorkspace({}).catch((error: unknown) => error)
    expect(failure).toBeInstanceOf(ConnectError)
    expect((failure as ConnectError).code).toBe(Code.Internal)
    expect(received).toEqual(['/coral.management.v1.WorkspaceService/CreateWorkspace'])
  })
})
