export function isWindowsRequest(request: Request): boolean {
  const clientHint = request.headers.get('sec-ch-ua-platform')?.replaceAll('"', '')
  if (clientHint) return clientHint === 'Windows'

  return /Windows NT/i.test(request.headers.get('user-agent') ?? '')
}
