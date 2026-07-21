import { routePath } from '@/routing/routemap'

export const ROOT_SPAN_ID_PARAM = 'rootSpanId'

function searchWithRootSpanId(search: string, rootSpanId: string): string {
  const base = listSearch(search)
  if (!rootSpanId) return base
  return `${base || '?'}${base ? '&' : ''}${ROOT_SPAN_ID_PARAM}=${encodeURIComponent(rootSpanId)}`
}

export function listSearch(search: string): string {
  // Parse manually so removing rootSpanId preserves every other parameter's raw spelling and
  // valueless flags instead of normalizing them through URLSearchParams.
  const parts = search
    .replace(/^\?/, '')
    .split('&')
    .filter(Boolean)
    .filter((part) => {
      const rawName = part.split('=', 1)[0]
      try {
        return decodeURIComponent(rawName.replace(/\+/g, ' ')) !== ROOT_SPAN_ID_PARAM
      } catch {
        return rawName !== ROOT_SPAN_ID_PARAM
      }
    })
  return parts.length > 0 ? `?${parts.join('&')}` : ''
}

export function rootSpanIdFromSearch(search: string): string {
  return new URLSearchParams(search).get(ROOT_SPAN_ID_PARAM) ?? ''
}

export function traceLocation(
  workspaceId: string,
  traceId: string,
  search: string,
  rootSpanId?: string,
) {
  return {
    pathname: routePath('workspaceTrace', { traceId, workspaceId }),
    search: rootSpanId === undefined ? search : searchWithRootSpanId(search, rootSpanId),
  }
}
