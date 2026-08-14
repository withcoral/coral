import { isIP } from 'node:net'

export function isExplicitLoopbackUrl(url: URL): boolean {
  const hostname = unbracketedHostname(url.hostname).toLowerCase().replace(/\.$/, '')
  if (hostname === 'localhost') return true

  const family = isIP(hostname)
  if (family === 4) return hostname.split('.')[0] === '127'
  if (family !== 6) return false
  if (hostname === '::1') return true

  // WHATWG URL serialization renders IPv4-mapped loopback addresses in this
  // canonical hexadecimal form, e.g. ::ffff:127.0.0.1 -> ::ffff:7f00:1.
  return /^::ffff:7f[0-9a-f]{2}:[0-9a-f]{1,4}$/i.test(hostname)
}

export function isLocalhostSubdomain(hostname: string): boolean {
  return unbracketedHostname(hostname).toLowerCase().replace(/\.$/, '').endsWith('.localhost')
}

function unbracketedHostname(hostname: string): string {
  return hostname.startsWith('[') && hostname.endsWith(']') ? hostname.slice(1, -1) : hostname
}
