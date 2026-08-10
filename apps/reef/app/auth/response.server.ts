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
