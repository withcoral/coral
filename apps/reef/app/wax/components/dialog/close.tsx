import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import classNames from 'classnames'

import { Button } from '@/wax/components'

import * as styles from './dialog.css'

export interface CloseProps {
  ariaLabel?: string
  className?: string
  ref?: React.Ref<HTMLButtonElement>
  /** Sit in the flow of a header the content owns instead of the popup corner. */
  inline?: boolean
}

export function Close({ ariaLabel = 'Close', className, ref, inline }: CloseProps) {
  return (
    <BaseDialog.Close
      className={classNames(inline ? styles.closeInline : styles.close, className)}
      ref={ref}
      // A focused close button must not open a tooltip that consumes the first Escape key.
      render={<Button.IconButton name="X" size="22" ariaLabel={ariaLabel} variant="bare" />}
    />
  )
}
