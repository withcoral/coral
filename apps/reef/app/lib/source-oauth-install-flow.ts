import { useEffect, useRef, useState } from 'react'

import {
  AUTH_STREAM_REQUEST_HEADER,
  AUTH_STREAM_RETURN_TO_HEADER,
  EXPIRED_SESSION_LOGIN_HEADER,
} from '@/auth/response'

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

export async function runOAuthInstallFlow({
  endpoint,
  fetchOAuthInstall,
  formData,
  navigateToLogin,
  onComplete,
  openAuthorization,
  setError,
  setProgress,
  signal,
  visibleLocation,
}: {
  endpoint: string
  fetchOAuthInstall: typeof fetch
  formData: FormData
  navigateToLogin: (location: string) => void
  onComplete: (name: string, signal: AbortSignal) => Promise<void> | void
  openAuthorization: (url: string) => unknown
  setError: (error: string | null) => void
  setProgress: (progress: OAuthInstallProgress) => void
  signal: AbortSignal
  visibleLocation: string
}): Promise<void> {
  setError(null)
  setProgress({ kind: 'busy' })

  try {
    const response = await fetchOAuthInstall(endpoint, {
      body: formData,
      headers: {
        [AUTH_STREAM_REQUEST_HEADER]: '1',
        [AUTH_STREAM_RETURN_TO_HEADER]: visibleLocation,
      },
      method: 'POST',
      signal,
    })
    const loginLocation = response.headers.get(EXPIRED_SESSION_LOGIN_HEADER)
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

    if (!signal.aborted) {
      setProgress({ kind: 'success', name: source.name })
      try {
        await onComplete(source.name, signal)
      } catch (cause) {
        if (!signal.aborted) {
          console.error('Failed to finish OAuth source setup:', cause)
        }
      }
    }
  } catch (cause) {
    if (signal.aborted) return
    setError(cause instanceof Error ? cause.message : String(cause))
    setProgress({ kind: 'idle' })
  }
}

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

    const abortController = new AbortController()
    abortRef.current = abortController
    try {
      const visibleLocation =
        returnTo ?? `${window.location.pathname}${window.location.search}${window.location.hash}`
      await runOAuthInstallFlow({
        endpoint,
        fetchOAuthInstall,
        formData,
        navigateToLogin,
        onComplete,
        openAuthorization,
        setError,
        setProgress,
        signal: abortController.signal,
        visibleLocation,
      })
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
