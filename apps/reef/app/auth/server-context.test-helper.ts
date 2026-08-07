import { RouterContextProvider } from 'react-router'

import { requestAuthContext } from './server-context'

export function authTestContext(accessToken: string | null = 'test-coral-token') {
  const context = new RouterContextProvider()
  // `null` is the only value that means disabled auth. Treating every falsy one
  // that way would quietly turn a test that passes `''` — an empty token is a
  // threading bug worth catching — into one that proves nothing.
  if (accessToken === null) {
    context.set(requestAuthContext, { accessToken: null, mode: 'disabled' })
    return context
  }

  context.set(requestAuthContext, {
    accessToken,
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
) {
  const url = new URL(request.url)
  return {
    context: authTestContext(accessToken),
    params,
    pattern: url.pathname,
    request,
    url,
  }
}
