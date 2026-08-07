export function authPrivateHeaders(): Headers {
  return new Headers({
    'Cache-Control': 'private, no-store',
    Vary: 'Cookie',
  })
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
