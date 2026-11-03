import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import classNames from 'classnames'
import { useContext } from 'react'

import * as styles from './dialog.css'
import { ForceBackdropContext, RailContext } from './rail'

export interface BackdropProps extends React.HTMLAttributes<HTMLDivElement> {
  ref?: React.Ref<HTMLDivElement>
}

export function Backdrop({ className, ref, ...props }: BackdropProps) {
  const rail = useContext(RailContext)
  const force = useContext(ForceBackdropContext)
  // A rail beside the page leaves the page in view and usable.
  if (rail && rail.mode !== 'modal') return null
  return (
    <BaseDialog.Backdrop
      className={classNames(styles.backdrop, className)}
      forceRender={force}
      ref={ref}
      {...props}
    />
  )
}
