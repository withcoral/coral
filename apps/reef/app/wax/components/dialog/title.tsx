import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import classNames from 'classnames'

import * as styles from './dialog.css'

export interface TitleProps extends React.HTMLAttributes<HTMLHeadingElement> {
  children: React.ReactNode
  ref?: React.Ref<HTMLHeadingElement>
  /** Name the dialog without showing the title, when the content shows its own heading. */
  hidden?: boolean
}

export function Title({ children, className, ref, hidden, ...props }: TitleProps) {
  return (
    <BaseDialog.Title
      className={classNames(hidden ? styles.titleHidden : styles.title, className)}
      ref={ref}
      {...props}
    >
      {children}
    </BaseDialog.Title>
  )
}
