import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import classNames from 'classnames'

import { Button } from '@/wax/components'

import * as styles from './dialog.css'

export interface CloseProps {
  ariaLabel?: string
  className?: string
  ref?: React.Ref<HTMLButtonElement>
}

export function Close({ ariaLabel = 'Close', className, ref }: CloseProps) {
  return (
    <BaseDialog.Close
      className={classNames(styles.close, className)}
      ref={ref}
      // A focused close button must not open a tooltip that consumes the first Escape key.
      render={<Button.IconButton name="X" size="22" ariaLabel={ariaLabel} variant="bare" />}
    />
  )
}
