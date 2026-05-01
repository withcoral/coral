import { Typography } from '@/wax/components/typography'
import { IconButton } from '@/wax/components/button'
import { formatApiError } from '@/lib/phoebe-client'
import * as s from './api-error-box.css'

interface ApiErrorBoxProps {
  error: string
  serverUrl?: string
  onRetry?: () => void
}

export function ApiErrorBox({ error, serverUrl, onRetry }: ApiErrorBoxProps) {
  return (
    <div className={s.errorBox}>
      <Typography.Body>{formatApiError(error, serverUrl)}</Typography.Body>
      {onRetry && (
        <IconButton
          name="RefreshCw"
          size="22"
          variant="bare"
          ariaLabel="Retry"
          onClick={onRetry}
        />
      )}
    </div>
  )
}
