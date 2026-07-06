import { Menu as BaseMenu } from '@base-ui/react/menu'
import classNames from 'classnames'
import { RefObject } from 'react'

import * as styles from './menu.css'

export interface ContentProps {
  align?: 'center' | 'end' | 'start'
  alignOffset?: number
  children: React.ReactNode
  className?: string
  container?: HTMLElement | null | RefObject<HTMLElement | null>
  side?: 'bottom' | 'left' | 'right' | 'top'
  sideOffset?: number
}

export function Content({
  align = 'start',
  alignOffset = 0,
  children,
  className,
  container,
  side = 'bottom',
  sideOffset = 4,
}: ContentProps) {
  return (
    <BaseMenu.Portal container={container}>
      <BaseMenu.Positioner
        align={align}
        alignOffset={alignOffset}
        className={styles.positioner}
        side={side}
        sideOffset={sideOffset}
      >
        <BaseMenu.Popup className={classNames(styles.popup, className)}>{children}</BaseMenu.Popup>
      </BaseMenu.Positioner>
    </BaseMenu.Portal>
  )
}
