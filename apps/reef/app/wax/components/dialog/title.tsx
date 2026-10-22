import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import classNames from 'classnames'

import { visuallyHidden as visuallyHiddenStyle } from '@/wax/components/visually-hidden.css'

import * as styles from './dialog.css'

export interface TitleProps extends React.HTMLAttributes<HTMLHeadingElement> {
  children: React.ReactNode
  ref?: React.Ref<HTMLHeadingElement>
  /** Name the dialog without showing the title, when the content shows its own heading. */
  visuallyHidden?: boolean
}

export function Title({ children, className, ref, visuallyHidden, ...props }: TitleProps) {
  return (
    <BaseDialog.Title
      className={classNames(visuallyHidden ? visuallyHiddenStyle : styles.title, className)}
      ref={ref}
      {...props}
    >
      {children}
    </BaseDialog.Title>
  )
}
