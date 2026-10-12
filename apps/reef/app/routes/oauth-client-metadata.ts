import type { Route } from './+types/oauth-client-metadata'

import { authClientId, authRedirectUri, providerSetupRedirectUri } from '@/auth/config.server'
import { runtimeConfigContext } from '@/lib/runtime-config.server'

const MAX_CLIENT_METADATA_BYTES = 5 * 1024

export async function loader({ context }: Route.LoaderArgs): Promise<Response> {
  const config = context.get(runtimeConfigContext).auth
  if (config.mode === 'disabled') throw new Response('Not Found', { status: 404 })

  const body = JSON.stringify({
    client_id: authClientId(config),
    client_name: 'Coral',
    grant_types: ['authorization_code'],
    // Both callbacks Coral's OAuth clients use: login, and Provider setup consent.
    // An upstream authorization server that reads this document accepts only
    // the redirect URIs listed here.
    redirect_uris: [authRedirectUri(config), providerSetupRedirectUri(config)],
    response_types: ['code'],
    token_endpoint_auth_method: 'none',
  })
  if (Buffer.byteLength(body) > MAX_CLIENT_METADATA_BYTES) {
    throw new Error('Coral web OAuth client metadata exceeds the 5 KiB CIMD limit')
  }

  return new Response(body, {
    headers: {
      'Cache-Control': 'public, max-age=300',
      'Content-Type': 'application/json',
    },
  })
}
