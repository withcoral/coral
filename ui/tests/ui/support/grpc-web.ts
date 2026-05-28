import { toBinary, type DescMessage, type MessageShape } from '@bufbuild/protobuf'
import { HttpResponse, type HttpResponseInit } from 'msw'

const GRPC_WEB_CONTENT_TYPE = 'application/grpc-web+proto'
const DATA_FRAME = 0x00
const TRAILER_FRAME = 0x80

function frame(flag: number, payload: Uint8Array): Uint8Array {
  const framed = new Uint8Array(5 + payload.byteLength)
  framed[0] = flag
  new DataView(framed.buffer).setUint32(1, payload.byteLength, false)
  framed.set(payload, 5)
  return framed
}

function concat(chunks: Uint8Array[]): ArrayBuffer {
  const byteLength = chunks.reduce((total, chunk) => total + chunk.byteLength, 0)
  const merged = new Uint8Array(byteLength)
  let offset = 0

  for (const chunk of chunks) {
    merged.set(chunk, offset)
    offset += chunk.byteLength
  }

  return merged.buffer
}

function trailers(status: number, message?: string): Uint8Array {
  const lines = [`grpc-status: ${status}`]
  if (message) lines.push(`grpc-message: ${encodeURIComponent(message)}`)
  return new TextEncoder().encode(`${lines.join('\r\n')}\r\n`)
}

export function grpcWebResponse<Desc extends DescMessage>(
  schema: Desc,
  message: MessageShape<Desc>,
  init?: HttpResponseInit,
) {
  const data = toBinary(schema, message)
  const body = concat([frame(DATA_FRAME, data), frame(TRAILER_FRAME, trailers(0))])

  return HttpResponse.arrayBuffer(body, {
    status: 200,
    ...init,
    headers: {
      'content-type': GRPC_WEB_CONTENT_TYPE,
      ...init?.headers,
    },
  })
}

export function grpcWebError(status: number, message: string, init?: HttpResponseInit) {
  return HttpResponse.arrayBuffer(frame(TRAILER_FRAME, trailers(status, message)).buffer, {
    status: 200,
    ...init,
    headers: {
      'content-type': GRPC_WEB_CONTENT_TYPE,
      ...init?.headers,
    },
  })
}
