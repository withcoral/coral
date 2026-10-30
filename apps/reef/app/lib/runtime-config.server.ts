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
  /** The MCP clients that may have a Coral plugin; Reef does not check it. Empty offers none. */
  clientsWithAvailablePlugins: readonly string[]
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
    clientsWithAvailablePlugins: clientNames(process.env.AGENTS_WITH_AVAILABLE_PLUGINS ?? ''),
  }
}

const MAX_CLIENT_NAMES = 8
const MAX_CLIENT_NAME_LENGTH = 32
/** Letters, digits and single spaces, dots, hyphens or underscores between them. */
const CLIENT_NAME = /^[a-z0-9]+(?:[ ._-][a-z0-9]+)*$/

/**
 * A comma-separated list of MCP client names, in lowercase with single spaces, each
 * once. The onboarding prompt quotes them to a client, so each is a short plain name.
 */
function clientNames(value: string): string[] {
  const names = new Set(
    value
      .split(',')
      .map((name) => name.trim().replace(/\s+/g, ' ').toLowerCase())
      .filter(Boolean),
  )
  if (names.size > MAX_CLIENT_NAMES)
    throw new Error(
      `AGENTS_WITH_AVAILABLE_PLUGINS lists more than ${MAX_CLIENT_NAMES} client names`,
    )
  for (const name of names)
    if (name.length > MAX_CLIENT_NAME_LENGTH || !CLIENT_NAME.test(name))
      throw new Error(
        `AGENTS_WITH_AVAILABLE_PLUGINS has an invalid client name ${JSON.stringify(name)}: use up to ${MAX_CLIENT_NAME_LENGTH} letters, digits, spaces, dots, hyphens or underscores`,
      )
  return [...names]
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
