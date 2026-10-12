import classNames from 'classnames'

import * as styles from './dialog.css'

export interface ActionsProps extends React.HTMLAttributes<HTMLDivElement> {
  children: React.ReactNode
  ref?: React.Ref<HTMLDivElement>
}

export function Actions({ children, className, ref, ...props }: ActionsProps) {
  return (
    <div className={classNames(styles.actions, className)} ref={ref} {...props}>
      {children}
    </div>
  )
}
