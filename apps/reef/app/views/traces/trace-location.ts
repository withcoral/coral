import { routePath } from '@/routing/routemap'

export function traceLocation(workspaceId: string, traceId: string, search: string) {
  return { pathname: routePath('workspaceTrace', { traceId, workspaceId }), search }
}
