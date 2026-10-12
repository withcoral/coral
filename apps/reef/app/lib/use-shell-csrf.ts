import { useRouteLoaderData } from 'react-router'
import type { loader } from '@/routes/app-shell'

/** The shell's CSRF token, read where a mutation form renders instead of passed down. */
export function useShellCsrf() {
  return useRouteLoaderData<typeof loader>('routes/app-shell')?.form.csrf ?? ''
}
