import type { Route } from './+types/oauth-client-metadata'

import { authClientId, authRedirectUri, reefAuthConfig } from '@/auth/config.server'

const MAX_CLIENT_METADATA_BYTES = 5 * 1024

export async function loader({ request: _request }: Route.LoaderArgs): Promise<Response> {
  const config = reefAuthConfig()
  if (config.mode === 'disabled') throw new Response('Not Found', { status: 404 })

  const body = JSON.stringify({
    client_id: authClientId(config),
    client_name: 'Coral Reef',
    grant_types: ['authorization_code'],
    redirect_uris: [authRedirectUri(config)],
    response_types: ['code'],
    token_endpoint_auth_method: 'none',
  })
  if (Buffer.byteLength(body) > MAX_CLIENT_METADATA_BYTES) {
    throw new Error('Reef OAuth client metadata exceeds the 5 KiB CIMD limit')
  }

  return new Response(body, {
    headers: {
      'Cache-Control': 'public, max-age=300',
      'Content-Type': 'application/json',
    },
  })
}
