import { createContext, type MiddlewareFunction } from 'react-router'

import { coralWebAuthConfig } from '@/auth/config.server'
import type { AuthConfig } from '@/auth/types'

import { resolveCoralEndpoint, type CoralEndpointPolicy } from './coral-endpoint.server'

const STARTUP_REQUEST = new Request('http://127.0.0.1')

export interface CoralWebRuntimeConfig {
  auth: AuthConfig
  endpoint: CoralEndpointPolicy
  /** Where a local build asks upstream OAuth to return; empty uses the request origin. */
  upstreamOAuthCallbackUrl: string
}

/**
 * The bootstrap configuration for every request. The root route's middleware
 * sets it; request code reads it here, never from the environment.
 */
export const runtimeConfigContext = createContext<CoralWebRuntimeConfig>()

/** Gives every route the bootstrap configuration. `root.tsx` installs it. */
export const runtimeConfigMiddleware: MiddlewareFunction<Response> = ({ context }, next) => {
  context.set(runtimeConfigContext, bootstrapRuntimeConfig())
  return next()
}

/** Read at server bootstrap, then pass these explicit values to request paths. */
export function readCoralWebRuntimeConfig(): CoralWebRuntimeConfig {
  const auth = coralWebAuthConfig()
  const endpoint = resolveCoralEndpoint({
    authenticated: auth.mode === 'required',
    request: STARTUP_REQUEST,
  })
  return {
    auth,
    endpoint,
    upstreamOAuthCallbackUrl: process.env.CORAL_UPSTREAM_OAUTH_CALLBACK_URL ?? '',
  }
}

let bootstrap: CoralWebRuntimeConfig | undefined

/** The process's configuration, read from the environment once. */
export function bootstrapRuntimeConfig(): CoralWebRuntimeConfig {
  bootstrap ??= readCoralWebRuntimeConfig()
  return bootstrap
}

// The server build imports this module at startup, so an invalid configuration
// fails the import and names its setting before the server listens.
bootstrapRuntimeConfig()
