import { Icon } from '@/wax/components/icon'
import { Typography } from '@/wax/components/typography'
import { formatApiError } from '@/lib/phoebe-client'
import * as s from './server-error.css'

interface ServerErrorProps {
  title?: string
  error: string
  serverUrl?: string
}

export function ServerError({
  title = 'Could not reach the server',
  error,
  serverUrl,
}: ServerErrorProps) {
  return (
    <div className={s.root}>
      <Icon className={s.icon} name="CircleAlert" size="24" />
      <Typography.Body variant="secondary">{title}</Typography.Body>
      <Typography.BodySmall className={s.details}>
        {formatApiError(error, serverUrl)}
      </Typography.BodySmall>
    </div>
  )
}
