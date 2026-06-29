import { create, toBinary } from '@bufbuild/protobuf'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { WorkspaceSchema } from '../generated/coral/v1/resources_pb'
import { GrpcWebError, grpcWebServerStream, grpcWebUnary } from './grpc-web'

const DATA_FRAME = 0
const TRAILER_FRAME = 0x80

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('grpcWebUnary', () => {
  it('returns the decoded message on a status 0 success', async () => {
    const payload = toBinary(WorkspaceSchema, create(WorkspaceSchema, { name: 'workspace-1' }))
    stubFetch(concat(frame(DATA_FRAME, payload), trailerFrame('grpc-status:0')))

    const result = await grpcWebUnary({
      input: { name: 'workspace-1' },
      inputSchema: WorkspaceSchema,
      outputSchema: WorkspaceSchema,
      path: 'https://api.coral.test/coral.v1.SourceService/GetSource',
    })

    expect(result.name).toBe('workspace-1')
  })

  it('treats a 200 with data but no grpc-status as a protocol error', async () => {
    const payload = toBinary(WorkspaceSchema, create(WorkspaceSchema, { name: 'workspace-1' }))
    stubFetch(frame(DATA_FRAME, payload))

    await expect(
      grpcWebUnary({
        input: {},
        inputSchema: WorkspaceSchema,
        outputSchema: WorkspaceSchema,
        path: 'https://api.coral.test/coral.v1.SourceService/GetSource',
      }),
    ).rejects.toThrow(/did not include a gRPC status/)
  })

  it('does not throw a URIError on a malformed grpc-message', async () => {
    stubFetch(trailerFrame('grpc-status:13\r\ngrpc-message:%E0%A4%A'))

    const error = await grpcWebUnary({
      input: {},
      inputSchema: WorkspaceSchema,
      outputSchema: WorkspaceSchema,
      path: 'https://api.coral.test/coral.v1.SourceService/GetSource',
    }).catch((caught: unknown) => caught)

    expect(error).toBeInstanceOf(GrpcWebError)
    expect((error as GrpcWebError).status).toBe('13')
    expect((error as GrpcWebError).message).toBe('%E0%A4%A')
  })
})

describe('grpcWebServerStream', () => {
  it('treats an empty 200 stream with no trailers as a protocol error', async () => {
    stubFetch(new Uint8Array())
    const onMessage = vi.fn()

    await expect(
      grpcWebServerStream({
        input: {},
        inputSchema: WorkspaceSchema,
        onMessage,
        outputSchema: WorkspaceSchema,
        path: 'https://api.coral.test/coral.v1.SourceService/DiscoverSources',
      }),
    ).rejects.toThrow(/did not include a gRPC status/)
    expect(onMessage).not.toHaveBeenCalled()
  })
})

function stubFetch(body: Uint8Array, init?: ResponseInit): void {
  const buffer = new ArrayBuffer(body.byteLength)
  new Uint8Array(buffer).set(body)
  vi.stubGlobal(
    'fetch',
    vi.fn(async () => new Response(buffer, { status: 200, ...init })),
  )
}

function frame(type: number, payload: Uint8Array): Uint8Array {
  const out = new Uint8Array(5 + payload.length)
  out[0] = type
  new DataView(out.buffer).setUint32(1, payload.length, false)
  out.set(payload, 5)
  return out
}

function trailerFrame(text: string): Uint8Array {
  return frame(TRAILER_FRAME, new TextEncoder().encode(text))
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((sum, part) => sum + part.length, 0)
  const out = new Uint8Array(total)
  let offset = 0
  for (const part of parts) {
    out.set(part, offset)
    offset += part.length
  }
  return out
}
