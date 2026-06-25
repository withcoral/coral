import { useCallback, useSyncExternalStore } from 'react'

export type Route = { kind: 'traces' } | { kind: 'sources' } | { kind: 'settings' }

export interface ParsedLocation {
  route: Route
  installSource?: string
}

function parseHash(): ParsedLocation {
  const raw = window.location.hash.replace(/^#\/?/, '')
  const [pathPart, queryPart = ''] = raw.split('?')
  const segments = pathPart.split('/').filter(Boolean)

  if (segments[0] === 'sources') {
    const installSource = new URLSearchParams(queryPart).get('install')?.trim()
    return {
      route: { kind: 'sources' },
      ...(installSource ? { installSource } : {}),
    }
  }

  if (segments[0] === 'settings') {
    return { route: { kind: 'settings' } }
  }

  if (segments[0] === 'traces' || segments.length === 0) {
    return { route: { kind: 'traces' } }
  }

  return { route: { kind: 'traces' } }
}

function serialise(parsed: ParsedLocation): string {
  if (parsed.route.kind === 'traces') return '#/traces'
  if (parsed.route.kind === 'settings') return '#/settings'
  const params = new URLSearchParams()
  if (parsed.installSource) params.set('install', parsed.installSource)
  const query = params.toString()
  return `#/sources${query ? `?${query}` : ''}`
}

let cachedLocation: ParsedLocation = parseHash()
const listeners = new Set<() => void>()

function onHashChange() {
  cachedLocation = parseHash()
  listeners.forEach((l) => l())
}

function subscribe(listener: () => void) {
  if (listeners.size === 0) window.addEventListener('hashchange', onHashChange)
  listeners.add(listener)
  return () => {
    listeners.delete(listener)
    if (listeners.size === 0) window.removeEventListener('hashchange', onHashChange)
  }
}

function getSnapshot(): ParsedLocation {
  return cachedLocation
}

export function useRouter() {
  const location = useSyncExternalStore(subscribe, getSnapshot, getSnapshot)
  const navigate = useCallback((next: ParsedLocation) => {
    const hash = serialise(next)
    if (window.location.hash !== hash) {
      window.location.hash = hash
    }
  }, [])
  return { location, navigate }
}
