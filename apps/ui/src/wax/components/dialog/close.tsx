import { Dialog as BaseDialog } from '@base-ui-components/react/dialog'
import classNames from 'classnames'

import { IconButton } from '@/wax/components/button/icon-button'

import * as styles from './dialog.css'

export interface CloseProps {
  className?: string
  ref?: React.Ref<HTMLButtonElement>
}

export function Close({ className, ref }: CloseProps) {
  return (
    <BaseDialog.Close
      className={classNames(styles.close, className)}
      ref={ref}
      render={<IconButton name="X" size="22" ariaLabel="Close" variant="bare" />}
    />
  )
}
