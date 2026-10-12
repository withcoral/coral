import classNames from 'classnames'
import { Popover as AriaPopover, PopoverProps as AriaPopoverProps } from 'react-aria-components'

import * as styles from './popover.css'

export interface PopoverProps extends Omit<AriaPopoverProps, 'children'> {
  children: React.ReactNode
}

export function Popover({ children, className, ...props }: PopoverProps) {
  return (
    <AriaPopover {...props} className={classNames(styles.popover, className)}>
      {children}
    </AriaPopover>
  )
}
