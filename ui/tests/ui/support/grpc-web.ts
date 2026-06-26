import { fromBinary, toBinary, type DescMessage, type MessageShape } from '@bufbuild/protobuf'
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

function dataFramePayload(body: ArrayBuffer): Uint8Array {
  const bytes = new Uint8Array(body)
  if (bytes.byteLength < 5) throw new Error('gRPC-web request missing data frame')
  if (bytes[0] !== DATA_FRAME) throw new Error(`unexpected gRPC-web frame flag ${bytes[0]}`)

  const length = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(1, false)
  if (bytes.byteLength < 5 + length) {
    throw new Error(`gRPC-web request data frame truncated: expected ${length} bytes`)
  }
  return bytes.slice(5, 5 + length)
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

export function grpcWebStreamResponse<Desc extends DescMessage>(
  schema: Desc,
  messages: MessageShape<Desc>[],
  options?: { delayMs?: number; init?: HttpResponseInit },
) {
  const chunks = [
    ...messages.map((message) => frame(DATA_FRAME, toBinary(schema, message))),
    frame(TRAILER_FRAME, trailers(0)),
  ]

  if (!options?.delayMs) {
    return HttpResponse.arrayBuffer(concat(chunks), {
      status: 200,
      ...options?.init,
      headers: {
        'content-type': GRPC_WEB_CONTENT_TYPE,
        ...options?.init?.headers,
      },
    })
  }

  let index = 0
  const stream = new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (index >= chunks.length) {
        controller.close()
        return
      }

      controller.enqueue(chunks[index])
      index += 1

      if (index < chunks.length) {
        await new Promise((resolve) => setTimeout(resolve, options.delayMs))
      } else {
        controller.close()
      }
    },
  })

  return new HttpResponse(stream, {
    status: 200,
    ...options.init,
    headers: {
      'content-type': GRPC_WEB_CONTENT_TYPE,
      ...options.init?.headers,
    },
  })
}

export async function grpcWebRequest<Desc extends DescMessage>(
  schema: Desc,
  request: Request,
): Promise<MessageShape<Desc>> {
  return fromBinary(schema, dataFramePayload(await request.arrayBuffer()))
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
