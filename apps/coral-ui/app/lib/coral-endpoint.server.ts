import { Code, ConnectError } from '@connectrpc/connect'

import { DEFAULT_DEV_CORAL_ENDPOINT, DEFAULT_DEV_CORAL_PORT } from './constants'
import { isExplicitLoopbackUrl } from './loopback.server'
import { trimTrailingSlash } from './utils'

export interface CoralEndpointPolicy {
  authenticatedCleartextOrigin: string | null
  baseUrl: string
}

interface CoralEndpointInput {
  authenticated: boolean
  env?: NodeJS.ProcessEnv
  request: Request
}

export function resolveCoralEndpoint({
  authenticated,
  env = process.env,
  request,
}: CoralEndpointInput): CoralEndpointPolicy {
  const configured = env.CORAL_ENDPOINT?.trim()
  if (!configured) {
    if (authenticated)
      throw new Error('CORAL_ENDPOINT must be set when Coral authentication is enabled')
    if (env.NODE_ENV === 'production') {
      // Unavailable, not a plain Error: a Coral UI deployed without a Coral
      // endpoint has no Coral to reach, so it renders the Coral-unavailable
      // boundary alongside every other failure to reach Coral.
      throw new ConnectError('CORAL_ENDPOINT must be set in production', Code.Unavailable)
    }

    const requestUrl = new URL(request.url)
    return policy(
      isExplicitLoopbackUrl(requestUrl) && requestUrl.port !== DEFAULT_DEV_CORAL_PORT
        ? DEFAULT_DEV_CORAL_ENDPOINT
        : requestUrl.origin,
    )
  }

  return policy(configured, authenticated, env.CORAL_UI_ALLOW_INSECURE_CORAL_ENDPOINT)
}

function policy(
  configured: string,
  authenticated = false,
  allowInsecure: string | undefined = undefined,
): CoralEndpointPolicy {
  let endpoint: URL
  try {
    endpoint = new URL(configured)
  } catch {
    throw new Error('CORAL_ENDPOINT must be an absolute HTTP(S) URL')
  }
  if (endpoint.protocol !== 'http:' && endpoint.protocol !== 'https:') {
    throw new Error('CORAL_ENDPOINT must be an absolute HTTP(S) URL')
  }
  if (
    endpoint.username ||
    endpoint.password ||
    configured.includes('?') ||
    configured.includes('#')
  ) {
    throw new Error('CORAL_ENDPOINT must not include credentials, a query string, or a fragment')
  }

  const baseUrl = trimTrailingSlash(configured)
  if (!authenticated || endpoint.protocol === 'https:' || isExplicitLoopbackUrl(endpoint)) {
    return { authenticatedCleartextOrigin: null, baseUrl }
  }

  const optIn = allowInsecure?.trim().toLowerCase()
  if (optIn === '1' || optIn === 'true') {
    return { authenticatedCleartextOrigin: endpoint.origin, baseUrl }
  }
  if (optIn && optIn !== '0' && optIn !== 'false') {
    throw new Error('CORAL_UI_ALLOW_INSECURE_CORAL_ENDPOINT must be set to 1 or true')
  }
  throw new Error(
    'CORAL_ENDPOINT must use HTTPS or explicit-loopback HTTP when Coral authentication is enabled',
  )
}
