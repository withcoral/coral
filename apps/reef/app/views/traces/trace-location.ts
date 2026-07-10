export function traceLocation(traceId: string, search: string) {
  return { pathname: `/traces/${encodeURIComponent(traceId)}`, search }
}
