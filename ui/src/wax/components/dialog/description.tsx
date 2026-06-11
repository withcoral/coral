import { Dialog as BaseDialog } from '@base-ui-components/react/dialog'
import classNames from 'classnames'

import * as styles from './dialog.css'

export interface DescriptionProps
  extends
    React.HTMLAttributes<HTMLParagraphElement>,
    Pick<React.ComponentProps<typeof BaseDialog.Description>, 'render'> {
  children: React.ReactNode
  ref?: React.Ref<HTMLParagraphElement>
}

export function Description({ children, className, ref, ...props }: DescriptionProps) {
  return (
    <BaseDialog.Description
      className={classNames(styles.description, className)}
      ref={ref}
      {...props}
    >
      {children}
    </BaseDialog.Description>
  )
}
