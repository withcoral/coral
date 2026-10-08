import { coralUIAuthConfig } from '@/auth/config.server'

import { resolveCoralEndpoint } from './coral-endpoint.server'

const STARTUP_REQUEST = new Request('http://127.0.0.1')

/** Fail startup before Coral UI accepts traffic when production configuration is invalid. */
export function assertCoralUIRuntimeConfig(): void {
  const auth = coralUIAuthConfig()
  resolveCoralEndpoint({
    authenticated: auth.mode === 'required',
    request: STARTUP_REQUEST,
  })
}
