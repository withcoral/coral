import { RouterContextProvider } from 'react-router'

import { DEFAULT_DEV_CORAL_ENDPOINT } from '@/lib/constants'
import { runtimeConfigContext, type CoralWebRuntimeConfig } from '@/lib/runtime-config.server'

import { requestAuthContext } from './server-context'

/**
 * A local runtime with disabled auth, as the root middleware sets it for a
 * development server. Pass another to these helpers for a hosted one.
 */
export const localRuntimeConfig: CoralWebRuntimeConfig = {
  auth: { mode: 'disabled' },
  endpoint: { authenticatedCleartextOrigin: null, baseUrl: DEFAULT_DEV_CORAL_ENDPOINT },
  upstreamOAuthCallbackUrl: '',
  agentsWithAvailablePlugins: [],
}

/** The context the root middleware gives every route: only the bootstrap configuration. */
export function runtimeTestContext(runtime: CoralWebRuntimeConfig = localRuntimeConfig) {
  const context = new RouterContextProvider()
  context.set(runtimeConfigContext, runtime)
  return context
}

/** Arguments for a public route, which sees the runtime configuration and no request auth. */
export function publicRouteTestArgs(
  request: Request,
  runtime: CoralWebRuntimeConfig = localRuntimeConfig,
) {
  const url = new URL(request.url)
  return {
    context: runtimeTestContext(runtime),
    params: {},
    pattern: url.pathname,
    request,
    url,
  }
}

export function authTestContext(
  accessToken: string | null = 'test-coral-token',
  runtime: CoralWebRuntimeConfig = localRuntimeConfig,
) {
  const context = runtimeTestContext(runtime)
  // `null` is the only value that means disabled auth. Treating every falsy one
  // that way would quietly turn a test that passes `''` — an empty token is a
  // threading bug worth catching — into one that proves nothing.
  if (accessToken === null) {
    context.set(requestAuthContext, { accessToken: null, mode: 'disabled' })
    return context
  }

  context.set(requestAuthContext, {
    accessToken,
    csrfToken: 'test-csrf-token',
    mode: 'required',
    session: {
      accessToken,
      expiresAt: 4_102_444_800,
      tokenType: 'Bearer',
    },
  })
  return context
}

export function authRouteTestArgs<Params>(
  request: Request,
  params: Params,
  accessToken: string | null = 'test-coral-token',
  runtime: CoralWebRuntimeConfig = localRuntimeConfig,
) {
  const url = new URL(request.url)
  return {
    context: authTestContext(accessToken, runtime),
    params,
    pattern: url.pathname,
    request,
    url,
  }
}
