import { RouterContextProvider } from 'react-router'

import { requestAuthContext } from './server-context'

export function authTestContext(accessToken: string | null = 'test-coral-token') {
  const context = new RouterContextProvider()
  if (!accessToken) {
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
