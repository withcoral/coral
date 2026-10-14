import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import classNames from 'classnames'
import { useContext } from 'react'

import * as styles from './dialog.css'
import { OwnBackdropContext, RailContext } from './rail'

export interface BackdropProps extends React.HTMLAttributes<HTMLDivElement> {
  ref?: React.Ref<HTMLDivElement>
}

export function Backdrop({ className, ref, ...props }: BackdropProps) {
  const rail = useContext(RailContext)
  const own = useContext(OwnBackdropContext)
  // A rail beside the page leaves the page in view and usable.
  if (rail && !rail.modal) return null
  return (
    <BaseDialog.Backdrop
      className={classNames(styles.backdrop, className)}
      forceRender={own}
      ref={ref}
      {...props}
    />
  )
}
