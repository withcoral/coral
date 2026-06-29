import { Dialog as BaseDialog } from '@base-ui-components/react/dialog'
import classNames from 'classnames'

import * as styles from './dialog.css'

export interface TitleProps extends React.HTMLAttributes<HTMLHeadingElement> {
  children: React.ReactNode
  ref?: React.Ref<HTMLHeadingElement>
}

export function Title({ children, className, ref, ...props }: TitleProps) {
  return (
    <BaseDialog.Title className={classNames(styles.title, className)} ref={ref} {...props}>
      {children}
    </BaseDialog.Title>
  )
}
