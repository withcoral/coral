import { readFile } from 'node:fs/promises'
import { connect } from 'node:http2'
import { join } from 'node:path'
import { expect, inject, type TestContext } from 'vitest'

import { WorkspaceService } from '../../../generated/coral/management/v1/workspace_pb.ts'
import { repoRoot } from '../../../support/config.ts'
import { managementServices } from '../../../support/management.ts'
import { openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  createWorkspace,
  programRunner,
  publishWorkspaceProvider,
} from '../../../support/providers.ts'
import { bounded } from '../../../support/stack.ts'
import { withOwner } from '../../../support/topology.ts'

const initialize = JSON.stringify({
  jsonrpc: '2.0',
  id: 1,
  method: 'initialize',
  params: {
    protocolVersion: '2025-11-25',
    capabilities: {},
    clientInfo: { name: 'Reel route probe', version: '1' },
  },
})

/** The MCP tool set that `crates/coral-mcp` pins; one list for both test layers. */
async function pinnedToolNames() {
  const source = await readFile(
    join(repoRoot, 'crates/coral-mcp/tests/credential_free_tracer.rs'),
    'utf8',
  )
  const list = /const TOOL_NAMES: \[&str; \d+\] = \[([^\]]*)\]/.exec(source)?.[1]
  if (!list) throw new Error('crates/coral-mcp no longer pins TOOL_NAMES')
  return [...list.matchAll(/"([^"]+)"/g)].map((match) => match[1])
}

/**
 * The paths a listener serves as gRPC methods over HTTP/2, as a gRPC client calls
 * them. A listener that speaks no HTTP/2 serves none.
 */
async function servedOverHttp2(origin: string, paths: string[], headers: Record<string, string>) {
  const session = connect(origin)
  session.on('error', () => {})
  const served: string[] = []
  try {
    for (const path of paths) {
      if (session.destroyed) break
      const seen: Record<string, unknown> = {}
      const stream = session.request({
        ':method': 'POST',
        ':path': path,
        'content-type': 'application/grpc',
        te: 'trailers',
        ...headers,
      })
      stream.on('response', (values) => Object.assign(seen, values))
      stream.on('trailers', (values) => Object.assign(seen, values))
      stream.on('error', () => {})
      stream.resume()
      // One empty, uncompressed message frame.
      stream.end(new Uint8Array(5))
      await bounded(
        new Promise((resolve) => stream.once('close', resolve)),
        5000,
        `HTTP/2 ${origin}${path}`,
      )
      if (
        seen['grpc-status'] !== undefined ||
        `${seen['content-type']}`.startsWith('application/grpc')
      )
        served.push(path)
    }
    return served
  } finally {
    session.destroy()
  }
}

/** Whether a listener serves a path as a Connect method over HTTP/1.1. */
async function servedOverHttp1(origin: string, path: string, headers: Record<string, string>) {
  const response = await fetch(`${origin}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'connect-protocol-version': '1', ...headers },
    body: '{}',
    signal: AbortSignal.timeout(5000),
  })
  await response.body?.cancel()
  return response.ok && (response.headers.get('content-type') ?? '').startsWith('application/json')
}

/**
 * What one composed deployment does not expose: no MCP route beside each
 * Workspace's and the fixed `/mcp`, and no public listener that serves a private gRPC method or
 * calls a Tool outside a Program. Audience rejection on the private
 * listener stays in `crates/coral-grpc`.
 */
export async function absentSurfaces(context: TestContext) {
  const { target } = inject('reel')
  await withOwner(context, async ({ page, step, accessToken }) => {
    const workspace = await createWorkspace(accessToken, `Absent surfaces ${crypto.randomUUID()}`)
    await publishWorkspaceProvider(
      accessToken,
      `workspaces/${workspace}`,
      openApiDocument('Absent surfaces', { lookup: { summary: 'Look something up' } }),
      'surfaces',
    )
    await step('The public MCP listener serves only each Workspace endpoint and /mcp', async () => {
      const mcp = target.mcpURL!
      for (const path of [
        '/',
        `/workspaces/${workspace}`,
        `/workspaces/${workspace}/mcp/tools`,
        '/mcp/tools',
      ]) {
        for (const method of ['GET', 'POST']) {
          const response = await fetch(`${mcp}${path}`, {
            method,
            headers: {
              Accept: 'application/json, text/event-stream',
              'Content-Type': 'application/json',
            },
            body: method === 'POST' ? initialize : undefined,
          })
          expect(response.status, `${method} ${path}`).toBe(404)
          await response.body?.cancel()
        }
      }
      // A published provider tool adds no MCP tool and cannot be called as one.
      const run = programRunner(page, target, workspace)
      const found = await run.search('lookup')
      expect(found.structuredContent.hits.map((hit) => hit.call_path)).toContain(
        'coral.providers.surfaces.lookup',
      )
      expect((await run.listTools()).map((tool) => tool.name)).toEqual(await pinnedToolNames())
      for (const name of ['coral.providers.surfaces.lookup', 'surfaces.lookup', 'lookup']) {
        expect((await run.callTool(name, {})).error).toMatchObject({
          code: -32602,
          message: `tool '${name}' not found`,
        })
      }
    })
    await step('No public listener serves a private gRPC method', async () => {
      const methods = managementServices.flatMap((service) =>
        service.methods.map((method) => `/${service.typeName}/${method.name}`),
      )
      const headers = {
        'coral-audience': 'reef',
        ...(accessToken ? { authorization: `Bearer ${accessToken}` } : {}),
      }
      // The private listener answers, so the probe can tell a served method. It
      // takes one unary method: a watch never ends where it is served.
      const listWorkspaces = `/${WorkspaceService.typeName}/ListWorkspaces`
      expect(await servedOverHttp2(target.grpcURL!, [listWorkspaces], headers)).toEqual([
        listWorkspaces,
      ])
      for (const origin of target.publicOrigins!) {
        expect(await servedOverHttp2(origin, methods, headers), origin).toEqual([])
        for (const path of methods)
          expect(await servedOverHttp1(origin, path, headers), `${origin}${path}`).toBe(false)
      }
    })
  })
}
