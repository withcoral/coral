import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import classNames from 'classnames'

import { Button } from '@/wax/components'

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
      render={<Button.IconButton name="X" size="22" tooltipText="Close" variant="bare" />}
    />
  )
}
