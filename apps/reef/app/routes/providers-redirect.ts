import { redirect } from 'react-router'
import type { Route } from './+types/providers-redirect'
import { privateHeaders, resourcePath, workspaceName } from '@/lib/management.server'

/** An address of the former Providers page opens the Tools page, with its query. */
export function loader({ params, request }: Route.LoaderArgs) {
  const { search } = new URL(request.url)
  return redirect(`${resourcePath(workspaceName(params.workspace))}/tools${search}`, {
    headers: privateHeaders,
  })
}
