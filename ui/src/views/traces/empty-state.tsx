import { Icon } from '@/wax/components/icon'
import { Typography } from '@/wax/components/typography'

import * as s from '../traces-page.css'

export function EmptyState({
  details,
  error,
  title,
}: {
  details?: React.ReactNode
  error?: string | null
  title?: React.ReactNode
}) {
  return (
    <div className={s.emptyState}>
      <Icon name={error ? 'CircleAlert' : 'Activity'} size="30" color={error ? 'error' : 'tertiary'} />
      <div className={s.emptyStateText}>
        <Typography.BodyLargeStrong>{error ? 'Tracing unavailable' : title ?? 'No queries yet'}</Typography.BodyLargeStrong>
        <Typography.Body variant="tertiary">
          {error ? error : details ?? 'Make sure tracing is enabled, then run a SQL query to see it here in real-time.'}
        </Typography.Body>
      </div>
    </div>
  )
}
