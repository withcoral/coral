import { useEffect, useRef, useState } from 'react'

import {
  AUTH_STREAM_REQUEST_HEADER,
  AUTH_STREAM_RETURN_TO_HEADER,
  EXPIRED_SESSION_LOGIN_HEADER,
} from '@/auth/response'

import { readOAuthInstallStream } from './source-oauth-install-stream'

/**
 * The login location an expired-session response carries, or `null` for any
 * other response.
 *
 * Split out of the hook so it can be tested without rendering: Reef's Vitest
 * coverage is Node-only, and the branch it guards — a stream fetch answered with
 * an expired session — is otherwise reachable only through a React render.
 */
export function expiredSessionLoginLocation(response: Response): string | null {
  return response.headers.get(EXPIRED_SESSION_LOGIN_HEADER)
}

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
  navigateToLogin = (location) => window.location.assign(location),
  onComplete,
  openAuthorization,
  returnTo,
}: {
  fetchOAuthInstall: typeof fetch
  navigateToLogin?: (location: string) => void
  onComplete: (name: string, signal: AbortSignal) => Promise<void> | void
  openAuthorization: (url: string) => unknown
  returnTo?: string
}) {
  const abortRef = useRef<AbortController | null>(null)
  const [progress, setProgress] = useState<OAuthInstallProgress>({ kind: 'idle' })
  const [error, setError] = useState<string | null>(null)
  const busy = progress.kind !== 'idle'

  useEffect(() => () => abortRef.current?.abort(), [])

  function cancel() {
    abortRef.current?.abort()
    abortRef.current = null
    setError(null)
    setProgress({ kind: 'idle' })
  }

  async function start(endpoint: string, formData: FormData) {
    if (abortRef.current) return
    setError(null)
    setProgress({ kind: 'busy' })

    const abortController = new AbortController()
    abortRef.current = abortController
    try {
      const visibleLocation =
        returnTo ?? `${window.location.pathname}${window.location.search}${window.location.hash}`
      const response = await fetchOAuthInstall(endpoint, {
        body: formData,
        headers: {
          [AUTH_STREAM_REQUEST_HEADER]: '1',
          [AUTH_STREAM_RETURN_TO_HEADER]: visibleLocation,
        },
        method: 'POST',
        signal: abortController.signal,
      })
      const loginLocation = expiredSessionLoginLocation(response)
      if (loginLocation) {
        setProgress({ kind: 'idle' })
        navigateToLogin(loginLocation)
        return
      }
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
      })

      if (!abortController.signal.aborted) {
        setProgress({ kind: 'success', name: source.name })
        try {
          await onComplete(source.name, abortController.signal)
        } catch (cause) {
          if (!abortController.signal.aborted) {
            console.error('Failed to finish OAuth source setup:', cause)
          }
        }
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
