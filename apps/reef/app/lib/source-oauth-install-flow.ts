import { useEffect, useRef, useState } from 'react'

import { readOAuthInstallStream } from './source-oauth-install-stream'

export type OAuthInstallProgress =
  | { kind: 'idle' }
  | { kind: 'busy' }
  | {
      kind: 'awaiting-oauth'
      authorizationUrl: string
      inputKey: string
      userCode: string
      verificationUri: string
      verificationUriComplete: string
    }
  | { kind: 'oauth-callback-received'; inputKey: string }
  | { kind: 'oauth-completed'; inputKey: string }
  | { kind: 'success'; name: string }

export function useOAuthInstallFlow({
  fetchOAuthInstall,
  onComplete,
  openAuthorization,
}: {
  fetchOAuthInstall: typeof fetch
  onComplete: (name: string) => Promise<void> | void
  openAuthorization: (url: string) => unknown
}) {
  const abortRef = useRef<AbortController | null>(null)
  const [progress, setProgress] = useState<OAuthInstallProgress>({ kind: 'idle' })
  const [error, setError] = useState<string | null>(null)
  const busy = progress.kind !== 'idle'

  useEffect(() => () => abortRef.current?.abort(), [])

  function cancel() {
    abortRef.current?.abort()
    abortRef.current = null
    setProgress({ kind: 'idle' })
  }

  async function start(endpoint: string, formData: FormData) {
    if (abortRef.current) return
    setError(null)
    setProgress({ kind: 'busy' })

    const abortController = new AbortController()
    abortRef.current = abortController
    try {
      const response = await fetchOAuthInstall(endpoint, {
        body: formData,
        method: 'POST',
        signal: abortController.signal,
      })
      const source = await readOAuthInstallStream(response, {
        onAuthorization: (event) => {
          setProgress({
            kind: 'awaiting-oauth',
            authorizationUrl: event.authorizationUrl,
            inputKey: event.inputKey,
            userCode: event.userCode,
            verificationUri: event.verificationUri,
            verificationUriComplete: event.verificationUriComplete,
          })
          openAuthorization(event.authorizationUrl)
        },
        onCallbackReceived: (event) => {
          setProgress({ kind: 'oauth-callback-received', inputKey: event.inputKey })
        },
        onCompleted: (event) => {
          setProgress({ kind: 'oauth-completed', inputKey: event.inputKey })
        },
        onSource: (event) => {
          setProgress({ kind: 'success', name: event.name })
        },
      })

      if (!abortController.signal.aborted) {
        setProgress({ kind: 'success', name: source.name })
        await onComplete(source.name)
      }
    } catch (cause) {
      if (abortController.signal.aborted) return
      setError(cause instanceof Error ? cause.message : String(cause))
      setProgress({ kind: 'idle' })
    } finally {
      if (abortRef.current === abortController) abortRef.current = null
    }
  }

  return { busy, cancel, error, progress, start }
}

export function oauthActionLabel(
  progress: OAuthInstallProgress,
  labels: { busy: string; idle: string },
): string {
  if (progress.kind === 'busy') return labels.busy
  if (progress.kind === 'awaiting-oauth') return 'Awaiting OAuth…'
  if (progress.kind === 'oauth-callback-received') return 'Exchanging token…'
  if (progress.kind === 'oauth-completed') return 'Finishing…'
  if (progress.kind === 'success') return 'Configured'
  return labels.idle
}
