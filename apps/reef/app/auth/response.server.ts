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
  if (!varies?.includes('cookie')) response.headers.append('Vary', 'Cookie')
  return response
}
