import { Dialog as BaseDialog } from '@base-ui-components/react/dialog'
import classNames from 'classnames'

import * as styles from './dialog.css'

export interface BackdropProps extends React.HTMLAttributes<HTMLDivElement> {
  ref?: React.Ref<HTMLDivElement>
}

export function Backdrop({ className, ref, ...props }: BackdropProps) {
  return (
    <BaseDialog.Backdrop className={classNames(styles.backdrop, className)} ref={ref} {...props} />
  )
}
