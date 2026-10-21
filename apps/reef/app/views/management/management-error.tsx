import { isRouteErrorResponse, useRouteError } from 'react-router'

import { Banner, Button } from '@/wax/components'

import { ManagementLayout, trail } from './layout'
import * as styles from './management.css'

export function ManagementError({ status, reason }: { status: number; reason?: string }) {
  const message =
    status === 404
      ? 'This page does not exist.'
      : status === 403
        ? 'You do not have permission to open this page.'
        : status === 401
          ? 'Sign in again to continue.'
          : (status === 400 || status === 409) && reason
            ? reason
            : status < 500
              ? 'Coral cannot complete this request. Go back to the previous page.'
              : 'Coral is unavailable. Reload the page to try again.'
  return (
    <ManagementLayout breadcrumbs={trail([], status === 404 ? 'Page not found' : 'Error')}>
      <Banner variant="error">{message}</Banner>
      <div className={styles.row}>
        {status === 401 && <Button.InternalLink to="/login">Sign in</Button.InternalLink>}
      </div>
    </ManagementLayout>
  )
}
export function ManagementErrorBoundary() {
  const error = useRouteError()
  if (!isRouteErrorResponse(error)) return <ManagementError status={503} />
  // Reef writes every refusal's body itself and never passes on Coral's own
  // message, so the body is safe to show as the reason.
  const reason = typeof error.data === 'string' ? error.data.trim() : ''
  return <ManagementError status={error.status} reason={reason || undefined} />
}
