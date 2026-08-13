import { reefAuthConfig } from '@/auth/config.server'

import { resolveCoralEndpoint } from './coral-endpoint.server'

const STARTUP_REQUEST = new Request('http://127.0.0.1')

/** Fail startup before Reef accepts traffic when production configuration is invalid. */
export function assertReefRuntimeConfig(): void {
  const auth = reefAuthConfig()
  resolveCoralEndpoint({
    authenticated: auth.mode === 'required',
    request: STARTUP_REQUEST,
  })
}
