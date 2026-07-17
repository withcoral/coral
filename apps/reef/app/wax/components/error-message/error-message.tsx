import classNames from 'classnames'

import { Icon } from '../icon'
import { Typography } from '../typography'
import * as styles from './error-message.css'

interface ErrorMessageProps {
  className?: string
  message: string
}

export function ErrorMessage({ className, message }: ErrorMessageProps) {
  return (
    <div className={classNames(styles.container, className)}>
      <Icon className={styles.icon} color="error" name="CircleAlert" size="18" />
      <Typography.Body variant="error">{message}</Typography.Body>
    </div>
  )
}
