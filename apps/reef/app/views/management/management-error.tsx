import { isRouteErrorResponse, useRouteError } from 'react-router'

import { Banner, Button } from '@/wax/components'

import { ManagementLayout, trail } from './layout'
import * as styles from './management.css'

export function ManagementError({ status }: { status: number }) {
  const message =
    status === 404
      ? 'This page does not exist.'
      : status === 403
        ? 'You do not have permission to open this page.'
        : status === 401
          ? 'Sign in again to continue.'
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
  return <ManagementError status={isRouteErrorResponse(error) ? error.status : 503} />
}
