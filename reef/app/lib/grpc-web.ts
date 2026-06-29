import { create, fromBinary, toBinary } from '@bufbuild/protobuf'
import type { DescMessage, MessageInitShape, MessageShape } from '@bufbuild/protobuf'

const DATA_FRAME = 0
const TRAILER_FRAME = 0x80

export class GrpcWebError extends Error {
  constructor(
    message: string,
    readonly status?: string,
  ) {
    super(message)
    this.name = 'GrpcWebError'
  }
}

export async function grpcWebUnary<Input extends DescMessage, Output extends DescMessage>({
  input,
  inputSchema,
  outputSchema,
  path,
}: {
  input: MessageInitShape<Input>
  inputSchema: Input
  outputSchema: Output
  path: string
}): Promise<MessageShape<Output>> {
  const response = await fetch(path, {
    body: bodyFromBytes(frameMessage(toBinary(inputSchema, create(inputSchema, input)))),
    headers: grpcWebHeaders(),
    method: 'POST',
  })
  const bytes = new Uint8Array(await response.arrayBuffer())
  const frames = parseFrames(bytes)
  assertGrpcSuccess(response, frames.trailers, 'Coral request failed')
  const data = frames.data.at(0)
  if (!data) throw new GrpcWebError('Coral response did not include a data frame', '0')
  return fromBinary(outputSchema, data)
}

export async function grpcWebServerStream<Input extends DescMessage, Output extends DescMessage>({
  input,
  inputSchema,
  onMessage,
  outputSchema,
  path,
}: {
  input: MessageInitShape<Input>
  inputSchema: Input
  onMessage: (message: MessageShape<Output>) => void
  outputSchema: Output
  path: string
}): Promise<void> {
  const response = await fetch(path, {
    body: bodyFromBytes(frameMessage(toBinary(inputSchema, create(inputSchema, input)))),
    headers: grpcWebHeaders(),
    method: 'POST',
  })
  if (!response.body) {
    const bytes = new Uint8Array(await response.arrayBuffer())
    parseStreamingBytes(bytes, outputSchema, onMessage, response)
    return
  }

  const reader = response.body.getReader()
  let pending = new Uint8Array()
  let trailers = new Headers()

  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    const combined = new Uint8Array(pending.length + value.length)
    combined.set(pending)
    combined.set(value, pending.length)
    const parsed = parseFrames(combined, true)
    for (const data of parsed.data) onMessage(fromBinary(outputSchema, data))
    trailers = parsed.trailers
    pending = parsed.remainder
  }

  if (pending.length > 0) {
    const parsed = parseFrames(pending)
    for (const data of parsed.data) onMessage(fromBinary(outputSchema, data))
    trailers = parsed.trailers
  }

  assertGrpcSuccess(response, trailers, 'Coral stream failed')
}

function grpcWebHeaders(): HeadersInit {
  return {
    accept: 'application/grpc-web+proto',
    'content-type': 'application/grpc-web+proto',
    'x-grpc-web': '1',
  }
}

function frameMessage(message: Uint8Array): Uint8Array {
  const framed = new Uint8Array(5 + message.length)
  framed[0] = DATA_FRAME
  new DataView(framed.buffer).setUint32(1, message.length, false)
  framed.set(message, 5)
  return framed
}

function bodyFromBytes(bytes: Uint8Array): BodyInit {
  const buffer = new ArrayBuffer(bytes.byteLength)
  new Uint8Array(buffer).set(bytes)
  return buffer
}

function parseStreamingBytes<Output extends DescMessage>(
  bytes: Uint8Array,
  outputSchema: Output,
  onMessage: (message: MessageShape<Output>) => void,
  response: Response,
): void {
  const frames = parseFrames(bytes)
  for (const data of frames.data) onMessage(fromBinary(outputSchema, data))
  assertGrpcSuccess(response, frames.trailers, 'Coral stream failed')
}

function parseFrames(bytes: Uint8Array, allowPartial = false) {
  const data: Uint8Array[] = []
  let offset = 0
  let trailers = new Headers()
  while (offset + 5 <= bytes.length) {
    const frameType = bytes[offset]
    const length = new DataView(bytes.buffer, bytes.byteOffset + offset + 1, 4).getUint32(0, false)
    const frameStart = offset + 5
    const frameEnd = frameStart + length
    if (frameEnd > bytes.length) break
    const frame = bytes.slice(frameStart, frameEnd)
    if ((frameType & TRAILER_FRAME) === TRAILER_FRAME) {
      trailers = parseTrailers(frame)
    } else {
      data.push(frame)
    }
    offset = frameEnd
  }
  if (!allowPartial && offset !== bytes.length) {
    throw new GrpcWebError('Coral response ended with an incomplete gRPC-Web frame')
  }
  return { data, remainder: bytes.slice(offset), trailers }
}

function parseTrailers(bytes: Uint8Array): Headers {
  const text = new TextDecoder().decode(bytes)
  const headers = new Headers()
  for (const line of text.split(/\r?\n/)) {
    if (!line) continue
    const separator = line.indexOf(':')
    if (separator < 0) continue
    headers.set(line.slice(0, separator).trim(), line.slice(separator + 1).trim())
  }
  return headers
}

function assertGrpcSuccess(response: Response, trailers: Headers, fallbackMessage: string): void {
  const { code, message } = grpcStatus(response, trailers)

  // A non-2xx HTTP response is a transport failure; surface the most useful message
  // we have, falling back to the HTTP status text.
  if (!response.ok) {
    throw new GrpcWebError(message || response.statusText || fallbackMessage, code ?? undefined)
  }

  // A 2xx response with no grpc-status is a protocol violation, not implicit success:
  // a well-formed gRPC-Web response always reports a status in headers or trailers.
  if (code === null) {
    throw new GrpcWebError('Coral response did not include a gRPC status')
  }

  if (code !== '0') {
    throw new GrpcWebError(message || response.statusText || fallbackMessage, code)
  }
}

function grpcStatus(
  response: Response,
  trailers: Headers,
): { code: string | null; message: string } {
  const code = trailers.get('grpc-status') ?? response.headers.get('grpc-status')
  const rawMessage = trailers.get('grpc-message') ?? response.headers.get('grpc-message') ?? ''
  return { code: code ?? null, message: safeDecodeMessage(rawMessage) }
}

// grpc-message is percent-encoded by the server, but a malformed value must not crash
// error handling with a URIError. Fall back to the raw value when decoding fails.
function safeDecodeMessage(value: string): string {
  try {
    return decodeURIComponent(value)
  } catch {
    return value
  }
}
