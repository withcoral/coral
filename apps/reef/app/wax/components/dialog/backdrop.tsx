import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import classNames from 'classnames'
import { useContext } from 'react'

import { BesidePageContext, OwnBackdropContext } from './beside-page-root'

import * as styles from './dialog.css'

export interface BackdropProps extends React.HTMLAttributes<HTMLDivElement> {
  ref?: React.Ref<HTMLDivElement>
}

export function Backdrop({ className, ref, ...props }: BackdropProps) {
  // A drawer beside the page leaves the page in view and usable.
  const beside = useContext(BesidePageContext)?.beside
  const own = useContext(OwnBackdropContext)
  if (beside) return null
  return (
    <BaseDialog.Backdrop
      className={classNames(styles.backdrop, className)}
      forceRender={own}
      ref={ref}
      {...props}
    />
  )
}
