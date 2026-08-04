import { AUTH_STREAM_REQUEST_HEADER, AUTH_STREAM_RETURN_TO_HEADER } from './response'

export const EXPIRED_SESSION_RESPONSE_HEADER = 'X-Reef-Expired-Session'

/**
 * The same headers [`markAuthResponsePrivate`] applies, for a route that
 * declares them rather than being handed a response to mark.
 *
 * Derived from that function rather than restating it. Spelling the policy out
 * twice is how the two drift: a header added to one would silently not apply to
 * routes using the other, and nothing would fail. Running the real thing over a
 * throwaway response costs an allocation on a path that already builds one.
 */
export function authPrivateHeaders(): Headers {
  return markAuthResponsePrivate(new Response(null)).headers
}

export function expiredSessionRedirect(request: Request): Response {
  return new Response(null, {
    headers: {
      [EXPIRED_SESSION_RESPONSE_HEADER]: '1',
      Location: loginLocationForRequest(request),
    },
    status: 302,
  })
}

export function loginLocationForRequest(request: Request): string {
  return `/login?returnTo=${encodeURIComponent(returnToForRequest(request))}`
}

function returnToForRequest(request: Request): string {
  if (request.headers.get(AUTH_STREAM_REQUEST_HEADER) === '1') {
    return safeRelativeLocation(request.headers.get(AUTH_STREAM_RETURN_TO_HEADER))
  }

  const url = new URL(request.url)
  return `${url.pathname}${url.search}`
}

function safeRelativeLocation(value: string | null): string {
  if (!value || value.length > 2048 || !value.startsWith('/') || value.startsWith('//')) return '/'
  try {
    const parsed = new URL(value, 'https://reef.invalid')
    if (parsed.origin !== 'https://reef.invalid') return '/'
    return `${parsed.pathname}${parsed.search}${parsed.hash}`
  } catch {
    return '/'
  }
}

export function markAuthResponsePrivate(response: Response): Response {
  response.headers.set('Cache-Control', 'private, no-store')
  const varies = response.headers
    .get('Vary')
    ?.split(',')
    .map((value) => value.trim().toLowerCase())
  // `*` is not a field name the list can be extended with — it means the response
  // is unique to its request, which already covers everything `Cookie` would say.
  // Appending would produce `*, Cookie`, which is not a valid `Vary` value, and
  // would weaken the header rather than strengthen it.
  if (!varies?.includes('*') && !varies?.includes('cookie')) {
    response.headers.append('Vary', 'Cookie')
  }

  return response
}
