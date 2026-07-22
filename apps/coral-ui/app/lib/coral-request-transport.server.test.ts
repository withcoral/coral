import { Code, ConnectError } from '@connectrpc/connect'
import { afterEach, expect, it, vi } from 'vitest'

import { guiOnboardingClientForRequest } from './coral-request.server'
import { isCoralUnavailableError } from './coral-unavailable'

// Kept out of `coral-request.server.test.ts`: that file mocks the Connect
// transport away, and this case needs the real one so the connection it opens
// actually fails.
const request = new Request('http://coral-ui.test/onboarding')

afterEach(() => {
  vi.unstubAllEnvs()
})

it('classifies connection failures as unavailable', async () => {
  vi.stubEnv('CORAL_UI_AUTH_MODE', 'disabled')
  // Port 1 is privileged and unbound, so the connection is refused rather than
  // left to time out.
  vi.stubEnv('CORAL_ENDPOINT', 'http://127.0.0.1:1')

  const error = await guiOnboardingClientForRequest(request, null)
    .getGuiOnboardingState({})
    .catch((caught) => caught)

  expect(error).toBeInstanceOf(ConnectError)
  expect(error).toMatchObject({ code: Code.Unavailable })
  expect(isCoralUnavailableError(error)).toBe(true)
})
