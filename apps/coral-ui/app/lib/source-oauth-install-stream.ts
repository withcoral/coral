export type OAuthInstallStreamEvent =
  | {
      type: 'oauthAuthorization'
      authorizationUrl: string
      expiresInSeconds: string
      inputKey: string
      userCode: string
      verificationUri: string
      verificationUriComplete: string
    }
  | { type: 'oauthCallbackReceived'; inputKey: string }
  | { type: 'oauthCompleted'; inputKey: string; metadata: { key: string; value: string }[] }
  | { type: 'source'; name: string; version: string }
  | { type: 'error'; message: string }

export interface OAuthInstallStreamHandlers {
  onAuthorization?: (
    event: Extract<OAuthInstallStreamEvent, { type: 'oauthAuthorization' }>,
  ) => void
  onCallbackReceived?: (
    event: Extract<OAuthInstallStreamEvent, { type: 'oauthCallbackReceived' }>,
  ) => void
  onCompleted?: (event: Extract<OAuthInstallStreamEvent, { type: 'oauthCompleted' }>) => void
  onSource?: (event: Extract<OAuthInstallStreamEvent, { type: 'source' }>) => void
}

export async function readOAuthInstallStream(
  response: Response,
  handlers: OAuthInstallStreamHandlers = {},
): Promise<Extract<OAuthInstallStreamEvent, { type: 'source' }>> {
  if (!response.body) throw new Error('OAuth install response did not include a stream')

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''

  try {
    while (true) {
      const { done, value } = await reader.read()
      buffer += decoder.decode(value, { stream: !done })
      const lines = buffer.split('\n')
      buffer = lines.pop() ?? ''
      for (const line of lines) {
        const source = handleOAuthInstallEvent(parseOAuthInstallEvent(line), handlers)
        if (source) return source
      }
      if (done) break
    }

    const tail = buffer.trim()
    if (tail) {
      const source = handleOAuthInstallEvent(parseOAuthInstallEvent(tail), handlers)
      if (source) return source
    }
  } finally {
    reader.releaseLock()
  }

  if (!response.ok) throw new Error(`OAuth install failed with HTTP ${response.status}`)
  throw new Error('OAuth install stream ended without a source event')
}

export function oauthInstallEventToNdjson(event: OAuthInstallStreamEvent): string {
  return `${JSON.stringify(event)}\n`
}

function handleOAuthInstallEvent(
  event: OAuthInstallStreamEvent,
  handlers: OAuthInstallStreamHandlers,
): Extract<OAuthInstallStreamEvent, { type: 'source' }> | null {
  if (event.type === 'error') throw new Error(event.message)
  if (event.type === 'oauthAuthorization') handlers.onAuthorization?.(event)
  if (event.type === 'oauthCallbackReceived') handlers.onCallbackReceived?.(event)
  if (event.type === 'oauthCompleted') handlers.onCompleted?.(event)
  if (event.type === 'source') {
    handlers.onSource?.(event)
    return event
  }
  return null
}

function parseOAuthInstallEvent(line: string): OAuthInstallStreamEvent {
  const trimmed = line.trim()
  if (!trimmed) throw new Error('OAuth install stream included an empty event')
  const parsed: unknown = JSON.parse(trimmed)
  if (!isOAuthInstallEvent(parsed))
    throw new Error('OAuth install stream included an invalid event')
  return parsed
}

function isOAuthInstallEvent(value: unknown): value is OAuthInstallStreamEvent {
  if (!isRecord(value)) return false
  if (value.type === 'oauthAuthorization') {
    return hasStringFields(value, [
      'authorizationUrl',
      'expiresInSeconds',
      'inputKey',
      'userCode',
      'verificationUri',
      'verificationUriComplete',
    ])
  }
  if (value.type === 'oauthCallbackReceived') return hasStringFields(value, ['inputKey'])
  if (value.type === 'oauthCompleted') {
    return (
      typeof value.inputKey === 'string' &&
      Array.isArray(value.metadata) &&
      value.metadata.every((item) => isRecord(item) && hasStringFields(item, ['key', 'value']))
    )
  }
  if (value.type === 'source') return hasStringFields(value, ['name', 'version'])
  if (value.type === 'error') return hasStringFields(value, ['message'])
  return false
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

function hasStringFields(value: Record<string, unknown>, fields: string[]): boolean {
  return fields.every((field) => typeof value[field] === 'string')
}
