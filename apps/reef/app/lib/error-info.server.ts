import { BinaryReader, WireType } from '@bufbuild/protobuf/wire'
import { ConnectError } from '@connectrpc/connect'

/**
 * The safe values Coral attaches to a refusal, by name: what a client shows or
 * acts on, such as the `message` that says what to correct. Empty when the
 * error carries none.
 *
 * Coral sends them as the metadata of a `google.rpc.ErrorInfo` detail. Reef
 * generates no schema for that message, so this reads its two fields from the
 * wire.
 */
export function refusalMetadata(error: unknown): Record<string, string> {
  if (!(error instanceof ConnectError)) return {}
  for (const detail of error.details) {
    if ('type' in detail && detail.type === 'google.rpc.ErrorInfo') return metadata(detail.value)
  }
  return {}
}

// ErrorInfo: `reason` = 1, `domain` = 2, `metadata` = 3, a map of strings.
function metadata(bytes: Uint8Array) {
  const values: Record<string, string> = {}
  const reader = new BinaryReader(bytes)
  while (reader.pos < reader.len) {
    const [number, wireType] = reader.tag()
    if (number !== 3 || wireType !== WireType.LengthDelimited) {
      reader.skip(wireType)
      continue
    }
    // One map entry: `key` = 1, `value` = 2.
    const entry = new BinaryReader(reader.bytes())
    let key = ''
    let value = ''
    while (entry.pos < entry.len) {
      const [part, partType] = entry.tag()
      if (part === 1 && partType === WireType.LengthDelimited) key = entry.string()
      else if (part === 2 && partType === WireType.LengthDelimited) value = entry.string()
      else entry.skip(partType)
    }
    values[key] = value
  }
  return values
}
